from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import sqlite3
from typing import Iterator

from traderrd.application.risk_engine import (
    RiskExecutionResult,
    command_digest,
    command_from_dict,
    command_to_dict,
    decision_from_dict,
    decision_to_dict,
    state_from_dict,
    state_to_dict,
)
from traderrd.domain.risk import (
    PortfolioRiskEngine, PortfolioState, RiskCommand, StartDemoRiskEpoch,
)


class IdempotencyConflict(ValueError):
    pass


class SQLiteRiskStateRepository:
    def __init__(self, database_path: str | Path) -> None:
        self._database_path = Path(database_path)

    def initialize(self) -> None:
        self._database_path.parent.mkdir(parents=True, exist_ok=True)
        with self._connection() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS risk_engine_state (
                    singleton_id INTEGER PRIMARY KEY CHECK (singleton_id = 1),
                    revision INTEGER NOT NULL CHECK (revision >= 1),
                    state_json TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS risk_engine_commands (
                    id INTEGER PRIMARY KEY,
                    command_id TEXT NOT NULL UNIQUE,
                    command_type TEXT NOT NULL,
                    occurred_at TEXT NOT NULL,
                    input_sha256 TEXT NOT NULL,
                    input_json TEXT NOT NULL,
                    decision_json TEXT NOT NULL,
                    resulting_revision INTEGER NOT NULL,
                    recorded_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS risk_engine_events (
                    id INTEGER PRIMARY KEY,
                    command_id TEXT NOT NULL,
                    event_order INTEGER NOT NULL,
                    occurred_at TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    entity_id TEXT,
                    detail TEXT NOT NULL,
                    UNIQUE (command_id, event_order)
                );

                CREATE INDEX IF NOT EXISTS idx_risk_events_type
                    ON risk_engine_events(event_type);
                """
            )

    def execute(
        self,
        command: RiskCommand,
        engine: PortfolioRiskEngine,
    ) -> RiskExecutionResult:
        if isinstance(command, StartDemoRiskEpoch):
            raise ValueError("Demo epochs require the dedicated operator action")
        return self._execute(command, engine)

    def start_demo_epoch(self, command: StartDemoRiskEpoch) -> RiskExecutionResult:
        """Audit a proven Demo boundary with local guards in the same transaction."""
        if not isinstance(command, StartDemoRiskEpoch):
            raise ValueError("Dedicated Demo epoch input is required")
        return self._execute(command, PortfolioRiskEngine(), demo_epoch=True)

    def check_demo_epoch_ready(self) -> PortfolioState:
        with self._connection(readonly=True) as connection:
            state = self._load_state(connection)
            self._check_epoch_idle(connection, state)
            return state

    @staticmethod
    def _check_epoch_idle(connection: sqlite3.Connection, state: PortfolioState | None) -> None:
        if state is None:
            raise ValueError("Demo epoch requires an initialized risk engine")
        if state.mode.value == "killed":
            raise ValueError("Killed risk engine requires ManualRearm")
        if state.active_reservations or state.reversal_intents:
            raise ValueError("Demo epoch requires no active reservations or reversals")
        # Missing schema is unknown, not evidence of an empty execution ledger.
        rows = connection.execute(
            "SELECT intent_id, risk_reservation_id, kind, state FROM demo_execution_intents"
        ).fetchall()
        unresolved = connection.execute(
            "SELECT 1 FROM demo_performance_outcomes WHERE status != 'attributed' LIMIT 1"
        ).fetchone()
        if unresolved:
            raise ValueError("Demo epoch requires resolved performance outcomes")
        for row in rows:
            reservation = state.reservations.get(row["risk_reservation_id"])
            if reservation is None or row["kind"] not in {
                "entry", "cancel_entry", "close_position", "set_trading_stop",
            }:
                raise ValueError("Demo epoch found unmanaged execution history")
            if row["state"] not in {"cancelled", "rejected", "close_confirmed", "position_closed"}:
                raise ValueError("Demo epoch requires no pending or unresolved execution intents")
            if row["state"] == "close_confirmed" and (
                row["kind"] != "close_position" or reservation.status.value != "closed"
            ):
                raise ValueError("Demo epoch found contradictory owned-close evidence")
            if row["state"] == "position_closed":
                attributed = connection.execute(
                    "SELECT 1 FROM demo_performance_outcomes WHERE entry_intent_id=? "
                    "AND risk_reservation_id=? AND status='attributed'",
                    (row["intent_id"], row["risk_reservation_id"]),
                ).fetchone()
                if row["kind"] != "entry" or reservation.status.value != "closed" or not attributed:
                    raise ValueError("Demo epoch found contradictory closed-position evidence")
            if (
                row["state"] == "close_confirmed"
                or row["kind"] in {"entry", "cancel_entry"}
                and row["state"] in {"cancelled", "rejected"}
            ):
                synced = connection.execute(
                    "SELECT 1 FROM demo_execution_events WHERE intent_id=? AND event_type='risk_sync_completed'",
                    (row["intent_id"],),
                ).fetchone()
                if not synced:
                    raise ValueError("Demo epoch requires completed risk reconciliation")

    def _execute(
        self, command: RiskCommand, engine: PortfolioRiskEngine, *, demo_epoch: bool = False,
    ) -> RiskExecutionResult:
        input_digest = command_digest(command)
        input_payload = command_to_dict(command)
        with self._connection() as connection:
            connection.execute("BEGIN IMMEDIATE")
            existing = connection.execute(
                """
                SELECT input_sha256, decision_json
                FROM risk_engine_commands WHERE command_id = ?
                """,
                (command.command_id,),
            ).fetchone()
            if existing is not None:
                if existing["input_sha256"] != input_digest:
                    raise IdempotencyConflict(
                        "Risk command ID was reused with different input"
                    )
                state = self._load_state(connection)
                if state is None:
                    raise RuntimeError("Risk state is missing after a recorded command")
                return RiskExecutionResult(
                    decision=decision_from_dict(json.loads(existing["decision_json"])),
                    state=state,
                    events=(),
                    duplicate_command=True,
                )

            state = self._load_state(connection)
            if demo_epoch:
                self._check_epoch_idle(connection, state)
                age = datetime.now(timezone.utc) - command.captured_at
                if not timedelta(0) <= age <= timedelta(seconds=10):
                    raise ValueError("Demo epoch evidence expired before the audit transaction")
            transition = engine.process(state, command)
            current = connection.execute(
                "SELECT revision FROM risk_engine_state WHERE singleton_id = 1"
            ).fetchone()
            revision = int(current["revision"]) + 1 if current is not None else 1
            now = datetime.now(timezone.utc).isoformat()
            state_json = json.dumps(
                state_to_dict(transition.state),
                sort_keys=True,
                separators=(",", ":"),
            )
            connection.execute(
                """
                INSERT INTO risk_engine_state (
                    singleton_id, revision, state_json, updated_at
                ) VALUES (1, ?, ?, ?)
                ON CONFLICT(singleton_id) DO UPDATE SET
                    revision = excluded.revision,
                    state_json = excluded.state_json,
                    updated_at = excluded.updated_at
                """,
                (revision, state_json, now),
            )
            decision_json = json.dumps(
                decision_to_dict(transition.decision),
                sort_keys=True,
                separators=(",", ":"),
            )
            connection.execute(
                """
                INSERT INTO risk_engine_commands (
                    command_id, command_type, occurred_at, input_sha256,
                    input_json, decision_json, resulting_revision, recorded_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    command.command_id,
                    input_payload["type"],
                    command.occurred_at.isoformat(),
                    input_digest,
                    json.dumps(input_payload, sort_keys=True, separators=(",", ":")),
                    decision_json,
                    revision,
                    now,
                ),
            )
            for order, event in enumerate(transition.events):
                connection.execute(
                    """
                    INSERT INTO risk_engine_events (
                        command_id, event_order, occurred_at,
                        event_type, entity_id, detail
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        command.command_id,
                        order,
                        command.occurred_at.isoformat(),
                        event.event_type,
                        event.entity_id,
                        event.detail,
                    ),
                )
            return RiskExecutionResult(
                transition.decision,
                transition.state,
                transition.events,
            )

    def load_state(self) -> PortfolioState | None:
        if not self._database_path.is_file():
            return None
        try:
            with self._connection(readonly=True) as connection:
                return self._load_state(connection)
        except sqlite3.OperationalError as exc:
            if "no such table" in str(exc):
                return None
            raise

    def get_command(self, command_id: str) -> RiskCommand | None:
        """Read the immutable input for recovery of interrupted local effects."""
        with self._connection(readonly=True) as connection:
            row = connection.execute(
                "SELECT input_json FROM risk_engine_commands WHERE command_id = ?",
                (command_id,),
            ).fetchone()
        return command_from_dict(json.loads(row["input_json"])) if row else None

    def get_command_result(self, command_id: str) -> RiskExecutionResult | None:
        if not self._database_path.is_file():
            return None
        try:
            with self._connection(readonly=True) as connection:
                row = connection.execute(
                    """
                    SELECT decision_json FROM risk_engine_commands
                    WHERE command_id = ?
                    """,
                    (command_id,),
                ).fetchone()
                if row is None:
                    return None
                state = self._load_state(connection)
        except sqlite3.OperationalError as exc:
            if "no such table" in str(exc):
                return None
            raise
        if state is None:
            raise RuntimeError("Risk state is missing after a recorded command")
        return RiskExecutionResult(
            decision=decision_from_dict(json.loads(row["decision_json"])),
            state=state,
            events=(),
            duplicate_command=True,
        )

    def counts(self) -> tuple[int, int, int]:
        with self._connection(readonly=True) as connection:
            commands = connection.execute(
                "SELECT COUNT(*) FROM risk_engine_commands"
            ).fetchone()[0]
            events = connection.execute(
                "SELECT COUNT(*) FROM risk_engine_events"
            ).fetchone()[0]
            revision = connection.execute(
                "SELECT revision FROM risk_engine_state WHERE singleton_id = 1"
            ).fetchone()
        return int(commands), int(events), int(revision["revision"] if revision else 0)

    @staticmethod
    def _load_state(connection: sqlite3.Connection) -> PortfolioState | None:
        row = connection.execute(
            "SELECT state_json FROM risk_engine_state WHERE singleton_id = 1"
        ).fetchone()
        return state_from_dict(json.loads(row["state_json"])) if row else None

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
            connection.execute("PRAGMA foreign_keys = ON")
            connection.execute("PRAGMA busy_timeout = 5000")
            if not readonly:
                connection.execute("PRAGMA journal_mode = WAL")
            with connection:
                yield connection
        finally:
            connection.close()
