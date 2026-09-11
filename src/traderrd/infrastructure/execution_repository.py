from __future__ import annotations

from contextlib import contextmanager
from dataclasses import replace
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
import sqlite3
from typing import Iterator

from traderrd.domain.execution import (
    ExecutionIntent,
    ExecutionIntentKind,
    ExecutionIntentState,
)
from traderrd.domain.models import Direction, canonical_decimal


class SQLiteDemoExecutionRepository:
    def __init__(self, database_path: str | Path) -> None:
        self._database_path = Path(database_path)

    @property
    def database_path(self) -> Path:
        return self._database_path

    def initialize(self) -> None:
        self._database_path.parent.mkdir(parents=True, exist_ok=True)
        with self._connection() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS demo_execution_intents (
                    intent_id TEXT PRIMARY KEY,
                    order_link_id TEXT NOT NULL,
                    risk_command_id TEXT NOT NULL,
                    risk_reservation_id TEXT NOT NULL,
                    kind TEXT NOT NULL,
                    state TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    direction TEXT NOT NULL,
                    quantity TEXT NOT NULL,
                    price TEXT,
                    take_profit TEXT,
                    stop_loss TEXT,
                    expires_at TEXT,
                    exchange_order_id TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS demo_execution_events (
                    id INTEGER PRIMARY KEY,
                    intent_id TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    detail TEXT NOT NULL,
                    occurred_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS demo_execution_submission_attempts (
                    intent_id TEXT PRIMARY KEY,
                    attempted_at TEXT NOT NULL
                );

                -- This ledger starts at the performance feature rollout.  It is
                -- intentionally separate from execution events so that only a
                -- uniquely attributed, exchange-reported close can affect P&L.
                CREATE TABLE IF NOT EXISTS demo_performance_outcomes (
                    risk_reservation_id TEXT PRIMARY KEY,
                    entry_intent_id TEXT NOT NULL UNIQUE,
                    status TEXT NOT NULL CHECK(status IN ('attributed', 'unresolved')),
                    unresolved_reason TEXT,
                    symbol TEXT NOT NULL,
                    direction TEXT NOT NULL,
                    quantity TEXT NOT NULL,
                    exchange_order_id TEXT,
                    closed_at TEXT,
                    closed_pnl TEXT,
                    open_fee TEXT,
                    close_fee TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_demo_intent_state
                    ON demo_execution_intents(state);
                CREATE INDEX IF NOT EXISTS idx_demo_intent_reservation
                    ON demo_execution_intents(risk_reservation_id);
                CREATE INDEX IF NOT EXISTS idx_demo_performance_status
                    ON demo_performance_outcomes(status);
                """
            )

            columns = {row[1] for row in connection.execute("PRAGMA table_info(demo_execution_intents)")}
            if "filled_quantity" not in columns:
                connection.execute("ALTER TABLE demo_execution_intents ADD COLUMN filled_quantity TEXT")

    def record_fill_quantity(self, intent_id: str, quantity: Decimal) -> ExecutionIntent:
        with self._connection() as connection:
            connection.execute("BEGIN IMMEDIATE")
            current = self._get(connection, intent_id)
            if current is None or not quantity.is_finite() or not 0 < quantity <= current.quantity:
                raise ValueError("Invalid owned execution quantity")
            if current.filled_quantity is not None and quantity < current.filled_quantity:
                raise ValueError("Cumulative execution quantity cannot decrease")
            connection.execute(
                "UPDATE demo_execution_intents SET filled_quantity = ? WHERE intent_id = ?",
                (canonical_decimal(quantity), intent_id),
            )
            updated = self._get(connection, intent_id)
            if updated is None:
                raise RuntimeError("Execution intent disappeared during fill persistence")
            return updated

    def record_attributed_outcome(
        self,
        entry: ExecutionIntent,
        *,
        exchange_order_id: str,
        closed_at: datetime,
        closed_pnl: Decimal,
        open_fee: Decimal,
        close_fee: Decimal,
    ) -> None:
        """Persist one immutable, exchange-attributed outcome per reservation."""
        now = datetime.now(timezone.utc).isoformat()
        values = (
            entry.risk_reservation_id,
            entry.intent_id,
            entry.symbol,
            entry.direction.value,
            canonical_decimal(entry.filled_quantity or entry.quantity),
            exchange_order_id,
            closed_at.isoformat(),
            canonical_decimal(closed_pnl),
            canonical_decimal(open_fee),
            canonical_decimal(close_fee),
        )
        with self._connection() as connection:
            existing = connection.execute(
                "SELECT * FROM demo_performance_outcomes WHERE risk_reservation_id = ?",
                (entry.risk_reservation_id,),
            ).fetchone()
            if existing is not None and existing["status"] == "attributed":
                expected = (
                    existing["entry_intent_id"], existing["symbol"],
                    existing["direction"], existing["quantity"],
                    existing["exchange_order_id"], existing["closed_at"],
                    existing["closed_pnl"], existing["open_fee"], existing["close_fee"],
                )
                actual = values[1:5] + values[5:]
                if expected != actual:
                    raise ValueError("Performance outcome conflicts with stored evidence")
                return
            connection.execute(
                """
                INSERT INTO demo_performance_outcomes (
                    risk_reservation_id, entry_intent_id, status, unresolved_reason,
                    symbol, direction, quantity, exchange_order_id, closed_at,
                    closed_pnl, open_fee, close_fee, created_at, updated_at
                ) VALUES (?, ?, 'attributed', NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(risk_reservation_id) DO UPDATE SET
                    entry_intent_id = excluded.entry_intent_id,
                    status = 'attributed', unresolved_reason = NULL,
                    symbol = excluded.symbol, direction = excluded.direction,
                    quantity = excluded.quantity,
                    exchange_order_id = excluded.exchange_order_id,
                    closed_at = excluded.closed_at, closed_pnl = excluded.closed_pnl,
                    open_fee = excluded.open_fee, close_fee = excluded.close_fee,
                    updated_at = excluded.updated_at
                WHERE demo_performance_outcomes.status = 'unresolved'
                """,
                values + (now, now),
            )

    def record_unresolved_outcome(self, entry: ExecutionIntent, reason: str) -> None:
        """Durably expose a close which must not yet be counted as performance."""
        now = datetime.now(timezone.utc).isoformat()
        with self._connection() as connection:
            connection.execute(
                """
                INSERT INTO demo_performance_outcomes (
                    risk_reservation_id, entry_intent_id, status, unresolved_reason,
                    symbol, direction, quantity, created_at, updated_at
                ) VALUES (?, ?, 'unresolved', ?, ?, ?, ?, ?, ?)
                ON CONFLICT(risk_reservation_id) DO UPDATE SET
                    unresolved_reason = excluded.unresolved_reason,
                    updated_at = excluded.updated_at
                WHERE demo_performance_outcomes.status = 'unresolved'
                """,
                (
                    entry.risk_reservation_id, entry.intent_id, reason,
                    entry.symbol, entry.direction.value,
                    canonical_decimal(entry.filled_quantity or entry.quantity), now, now,
                ),
            )

    def outcome_is_attributed(self, reservation_id: str) -> bool:
        with self._connection(readonly=True) as connection:
            row = connection.execute(
                "SELECT status FROM demo_performance_outcomes WHERE risk_reservation_id = ?",
                (reservation_id,),
            ).fetchone()
        return row is not None and row["status"] == "attributed"

    def save_planned(self, intent: ExecutionIntent) -> ExecutionIntent:
        now = datetime.now(timezone.utc).isoformat()
        with self._connection() as connection:
            connection.execute("BEGIN IMMEDIATE")
            existing = self._get(connection, intent.intent_id)
            if existing is not None:
                comparable = replace(
                    existing,
                    state=ExecutionIntentState.PLANNED,
                    exchange_order_id=None,
                    filled_quantity=None,
                )
                if comparable != intent:
                    raise ValueError("Execution intent ID conflicts with stored input")
                return existing
            connection.execute(
                """
                INSERT INTO demo_execution_intents (
                    intent_id, order_link_id, risk_command_id,
                    risk_reservation_id, kind, state, symbol, direction,
                    quantity, price, take_profit, stop_loss, expires_at,
                    exchange_order_id, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    intent.intent_id,
                    intent.order_link_id,
                    intent.risk_command_id,
                    intent.risk_reservation_id,
                    intent.kind.value,
                    intent.state.value,
                    intent.symbol,
                    intent.direction.value,
                    canonical_decimal(intent.quantity),
                    _decimal(intent.price),
                    _decimal(intent.take_profit),
                    _decimal(intent.stop_loss),
                    _datetime(intent.expires_at),
                    intent.exchange_order_id,
                    now,
                    now,
                ),
            )
            self._event(connection, intent.intent_id, "planned", "risk_action", now)
        return intent

    def transition(
        self,
        intent_id: str,
        state: ExecutionIntentState,
        event_type: str,
        detail: str,
        exchange_order_id: str | None = None,
    ) -> ExecutionIntent:
        now = datetime.now(timezone.utc).isoformat()
        with self._connection() as connection:
            current = self._get(connection, intent_id)
            if current is None:
                raise ValueError("Execution intent does not exist")
            if current.state is state and (
                exchange_order_id is None
                or exchange_order_id == current.exchange_order_id
            ):
                return current
            connection.execute(
                """
                UPDATE demo_execution_intents
                SET state = ?, exchange_order_id = COALESCE(?, exchange_order_id),
                    updated_at = ? WHERE intent_id = ?
                """,
                (state.value, exchange_order_id, now, intent_id),
            )
            self._event(connection, intent_id, event_type, detail, now)
            updated = self._get(connection, intent_id)
            if updated is None:
                raise RuntimeError("Execution intent disappeared during transition")
            return updated

    def get(self, intent_id: str) -> ExecutionIntent | None:
        with self._connection(readonly=True) as connection:
            return self._get(connection, intent_id)

    def find_entry(self, reservation_id: str) -> ExecutionIntent | None:
        with self._connection(readonly=True) as connection:
            row = connection.execute(
                """
                SELECT * FROM demo_execution_intents
                WHERE risk_reservation_id = ? AND kind = 'entry'
                ORDER BY created_at DESC LIMIT 1
                """,
                (reservation_id,),
            ).fetchone()
            return self._from_row(row) if row else None

    def expirable_entries(self, now: datetime) -> list[ExecutionIntent]:
        with self._connection(readonly=True) as connection:
            rows = connection.execute(
                """
                SELECT * FROM demo_execution_intents
                WHERE kind = 'entry'
                  AND state IN ('acknowledged', 'working')
                  AND expires_at IS NOT NULL AND expires_at <= ?
                ORDER BY expires_at
                """,
                (now.isoformat(),),
            ).fetchall()
            return [self._from_row(row) for row in rows]

    def monitorable_intents(self) -> list[ExecutionIntent]:
        """Return owned intents whose exchange state still needs monitoring."""
        with self._connection(readonly=True) as connection:
            rows = connection.execute(
                """
                SELECT * FROM demo_execution_intents
                WHERE state IN (
                    'acknowledged', 'working', 'filled',
                    'protection_verified', 'position_closed_pending',
                    'reconciliation_required'
                ) OR ((state IN ('cancelled', 'rejected') AND kind IN ('entry', 'cancel_entry')
                       OR state = 'close_confirmed')
                    AND NOT EXISTS (SELECT 1 FROM demo_execution_events AS event
                        WHERE event.intent_id = demo_execution_intents.intent_id
                          AND event.event_type = 'risk_sync_completed'))
                ORDER BY created_at
                """
            ).fetchall()
            return [self._from_row(row) for row in rows]

    def mark_risk_sync_completed(self, intent_id: str) -> None:
        """Complete the durable terminal handoff only after all risk effects."""
        with self._connection() as connection:
            self._event(connection, intent_id, "risk_sync_completed", "risk_effects_persisted",
                        datetime.now(timezone.utc).isoformat())

    def planned_intents(self) -> list[ExecutionIntent]:
        """Return durable plans awaiting their first exchange submission."""
        with self._connection(readonly=True) as connection:
            rows = connection.execute(
                """
                SELECT * FROM demo_execution_intents
                WHERE state = 'planned'
                ORDER BY created_at
                """
            ).fetchall()
        return [self._from_row(row) for row in rows]

    def mark_submission_attempt(self, intent_id: str) -> None:
        with self._connection() as connection:
            connection.execute(
                """
                INSERT OR IGNORE INTO demo_execution_submission_attempts (
                    intent_id, attempted_at
                ) VALUES (?, ?)
                """,
                (intent_id, datetime.now(timezone.utc).isoformat()),
            )

    def has_submission_attempt(self, intent_id: str) -> bool:
        try:
            with self._connection(readonly=True) as connection:
                row = connection.execute(
                    """
                    SELECT 1 FROM demo_execution_submission_attempts
                    WHERE intent_id = ?
                    """,
                    (intent_id,),
                ).fetchone()
        except sqlite3.OperationalError as exc:
            if "no such table" in str(exc):
                return False
            raise
        return row is not None

    def intents_for_risk_command(self, risk_command_id: str) -> list[ExecutionIntent]:
        with self._connection(readonly=True) as connection:
            rows = connection.execute(
                """
                SELECT * FROM demo_execution_intents
                WHERE risk_command_id = ?
                ORDER BY created_at
                """,
                (risk_command_id,),
            ).fetchall()
        return [self._from_row(row) for row in rows]

    def owned_entry_keys(self) -> set[tuple[str, Direction]]:
        """Return symbols/directions represented by non-terminal owned entries."""
        with self._connection(readonly=True) as connection:
            rows = connection.execute(
                """
                SELECT symbol, direction FROM demo_execution_intents
                WHERE kind = 'entry'
                  AND state NOT IN (
                      'cancelled', 'rejected', 'position_closed'
                  )
                """
            ).fetchall()
        return {(row["symbol"], Direction(row["direction"])) for row in rows}

    def counts(self) -> tuple[int, int]:
        with self._connection(readonly=True) as connection:
            intents = connection.execute(
                "SELECT COUNT(*) FROM demo_execution_intents"
            ).fetchone()[0]
            events = connection.execute(
                "SELECT COUNT(*) FROM demo_execution_events"
            ).fetchone()[0]
        return int(intents), int(events)

    def known_order_links(self) -> set[str]:
        if not self._database_path.is_file():
            return set()
        try:
            with self._connection(readonly=True) as connection:
                rows = connection.execute(
                    "SELECT order_link_id FROM demo_execution_intents"
                ).fetchall()
        except sqlite3.OperationalError as exc:
            if "no such table" in str(exc):
                return set()
            raise
        return {str(row["order_link_id"]) for row in rows}

    def managed_position_intents(self) -> list[ExecutionIntent]:
        if not self._database_path.is_file():
            return []
        try:
            with self._connection(readonly=True) as connection:
                rows = connection.execute(
                    """
                    SELECT * FROM demo_execution_intents
                    WHERE kind = 'entry'
                      AND state IN (
                          'filled', 'protection_verified',
                          'position_closed_pending',
                          'reconciliation_required'
                      )
                      AND NOT EXISTS (
                          SELECT 1 FROM demo_execution_intents AS closed
                          WHERE closed.risk_reservation_id =
                                demo_execution_intents.risk_reservation_id
                            AND closed.kind = 'close_position'
                            AND closed.state = 'close_confirmed'
                      )
                    ORDER BY created_at
                    """
                ).fetchall()
        except sqlite3.OperationalError as exc:
            if "no such table" in str(exc):
                return []
            raise
        return [self._from_row(row) for row in rows]

    @staticmethod
    def _event(
        connection: sqlite3.Connection,
        intent_id: str,
        event_type: str,
        detail: str,
        occurred_at: str,
    ) -> None:
        connection.execute(
            """
            INSERT INTO demo_execution_events (
                intent_id, event_type, detail, occurred_at
            ) VALUES (?, ?, ?, ?)
            """,
            (intent_id, event_type, detail, occurred_at),
        )

    @staticmethod
    def _get(
        connection: sqlite3.Connection, intent_id: str
    ) -> ExecutionIntent | None:
        row = connection.execute(
            "SELECT * FROM demo_execution_intents WHERE intent_id = ?",
            (intent_id,),
        ).fetchone()
        return SQLiteDemoExecutionRepository._from_row(row) if row else None

    @staticmethod
    def _from_row(row: sqlite3.Row) -> ExecutionIntent:
        return ExecutionIntent(
            intent_id=row["intent_id"],
            order_link_id=row["order_link_id"],
            risk_command_id=row["risk_command_id"],
            risk_reservation_id=row["risk_reservation_id"],
            kind=ExecutionIntentKind(row["kind"]),
            state=ExecutionIntentState(row["state"]),
            symbol=row["symbol"],
            direction=Direction(row["direction"]),
            quantity=Decimal(row["quantity"]),
            price=Decimal(row["price"]) if row["price"] else None,
            take_profit=(
                Decimal(row["take_profit"]) if row["take_profit"] else None
            ),
            stop_loss=Decimal(row["stop_loss"]) if row["stop_loss"] else None,
            expires_at=(
                datetime.fromisoformat(row["expires_at"])
                if row["expires_at"]
                else None
            ),
            exchange_order_id=row["exchange_order_id"],
            filled_quantity=(Decimal(row["filled_quantity"]) if "filled_quantity" in row.keys() and row["filled_quantity"] else None),
        )

    @contextmanager
    def _connection(
        self, readonly: bool = False
    ) -> Iterator[sqlite3.Connection]:
        if readonly:
            database = f"{self._database_path.resolve().as_uri()}?mode=ro"
            connection = sqlite3.connect(database, uri=True, timeout=5)
        else:
            connection = sqlite3.connect(self._database_path, timeout=5)
        try:
            connection.row_factory = sqlite3.Row
            connection.execute("PRAGMA busy_timeout = 5000")
            if not readonly:
                connection.execute("PRAGMA journal_mode = WAL")
            with connection:
                yield connection
        finally:
            connection.close()


def _decimal(value: Decimal | None) -> str | None:
    return canonical_decimal(value) if value is not None else None


def _datetime(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None
