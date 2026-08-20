from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
import sqlite3
from typing import Iterator

from traderrd.domain.bridge import ScopedStoredSignal
from traderrd.domain.models import Direction, canonical_decimal


class SQLiteDemoBridgeRepository:
    def __init__(self, database_path: str | Path) -> None:
        self._database_path = Path(database_path)

    def initialize(self) -> None:
        self._database_path.parent.mkdir(parents=True, exist_ok=True)
        with self._connection() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS demo_signal_bridge_runs (
                    signal_fingerprint TEXT PRIMARY KEY,
                    source_chat_id INTEGER NOT NULL,
                    source_topic_id INTEGER NOT NULL,
                    source_sender_id INTEGER NOT NULL,
                    source_message_id INTEGER NOT NULL,
                    telegram_received_at TEXT NOT NULL,
                    snapshot_at TEXT NOT NULL,
                    strategy_equity TEXT NOT NULL,
                    risk_command_id TEXT NOT NULL UNIQUE,
                    decision_status TEXT NOT NULL,
                    decision_reason TEXT NOT NULL,
                    intent_count INTEGER NOT NULL,
                    recorded_at TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_demo_bridge_message
                    ON demo_signal_bridge_runs(
                        source_chat_id, source_topic_id, source_message_id
                    );

                CREATE TABLE IF NOT EXISTS demo_signal_worker_cursors (
                    source_chat_id INTEGER NOT NULL,
                    source_topic_id INTEGER NOT NULL,
                    source_sender_id INTEGER NOT NULL,
                    last_message_id INTEGER NOT NULL CHECK (last_message_id >= 0),
                    initialized_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (source_chat_id, source_topic_id, source_sender_id)
                );
                """
            )

    def get_worker_cursor(
        self,
        source_chat_id: int,
        source_topic_id: int,
        source_sender_id: int,
    ) -> int | None:
        """Return the durable live-worker cursor, if this worker was initialized."""
        if not self._database_path.is_file():
            return None
        try:
            with self._connection(readonly=True) as connection:
                row = connection.execute(
                    """
                    SELECT last_message_id
                    FROM demo_signal_worker_cursors
                    WHERE source_chat_id = ? AND source_topic_id = ?
                      AND source_sender_id = ?
                    """,
                    (source_chat_id, source_topic_id, source_sender_id),
                ).fetchone()
        except sqlite3.OperationalError as exc:
            if "no such table" in str(exc):
                return None
            raise
        return int(row["last_message_id"]) if row is not None else None

    def initialize_worker_cursor(
        self,
        source_chat_id: int,
        source_topic_id: int,
        source_sender_id: int,
        initial_message_id: int,
    ) -> int:
        """Atomically establish the live boundary without replaying stored history."""
        if min(source_chat_id, source_topic_id, source_sender_id) <= 0:
            raise ValueError("Worker source identifiers must be positive")
        if initial_message_id < 0:
            raise ValueError("Worker initial message ID cannot be negative")
        now = datetime.now(timezone.utc).isoformat()
        with self._connection() as connection:
            connection.execute(
                """
                INSERT OR IGNORE INTO demo_signal_worker_cursors (
                    source_chat_id, source_topic_id, source_sender_id,
                    last_message_id, initialized_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    source_chat_id,
                    source_topic_id,
                    source_sender_id,
                    initial_message_id,
                    now,
                    now,
                ),
            )
            row = connection.execute(
                """
                SELECT last_message_id
                FROM demo_signal_worker_cursors
                WHERE source_chat_id = ? AND source_topic_id = ?
                  AND source_sender_id = ?
                """,
                (source_chat_id, source_topic_id, source_sender_id),
            ).fetchone()
        if row is None:
            raise RuntimeError("Worker cursor was not initialized")
        return int(row["last_message_id"])

    def advance_worker_cursor(
        self,
        source_chat_id: int,
        source_topic_id: int,
        source_sender_id: int,
        source_message_id: int,
    ) -> None:
        if min(source_chat_id, source_topic_id, source_sender_id, source_message_id) <= 0:
            raise ValueError("Worker source identifiers must be positive")
        now = datetime.now(timezone.utc).isoformat()
        with self._connection() as connection:
            updated = connection.execute(
                """
                UPDATE demo_signal_worker_cursors
                SET last_message_id = ?, updated_at = ?
                WHERE source_chat_id = ? AND source_topic_id = ?
                  AND source_sender_id = ? AND last_message_id < ?
                """,
                (
                    source_message_id,
                    now,
                    source_chat_id,
                    source_topic_id,
                    source_sender_id,
                    source_message_id,
                ),
            ).rowcount
        if updated == 0 and self.get_worker_cursor(
            source_chat_id, source_topic_id, source_sender_id
        ) is None:
            raise RuntimeError("Worker cursor was not initialized")

    def list_scoped_signals_after(
        self,
        source_chat_id: int,
        source_topic_id: int,
        source_sender_id: int,
        last_message_id: int,
        limit: int = 100,
    ) -> list[ScopedStoredSignal]:
        """List only source-attested signals after the live cursor."""
        if min(source_chat_id, source_topic_id, source_sender_id) <= 0:
            raise ValueError("Worker source identifiers must be positive")
        if last_message_id < 0 or limit <= 0:
            raise ValueError("Worker cursor and limit must be valid")
        if not self._database_path.is_file():
            return []
        with self._connection(readonly=True) as connection:
            columns = {
                str(row["name"])
                for row in connection.execute("PRAGMA table_info(signals)")
            }
            required = {
                "source_topic_id",
                "source_sender_id",
                "telegram_received_at",
            }
            if not required.issubset(columns):
                return []
            rows = connection.execute(
                """
                SELECT fingerprint, source_chat_id, source_topic_id,
                       source_sender_id, source_message_id,
                       telegram_received_at, direction, symbol,
                       entry, take_profit, stop_loss
                FROM signals
                WHERE source_chat_id = ? AND source_topic_id = ?
                  AND source_sender_id = ? AND source_message_id > ?
                ORDER BY source_message_id ASC
                LIMIT ?
                """,
                (
                    source_chat_id,
                    source_topic_id,
                    source_sender_id,
                    last_message_id,
                    limit,
                ),
            ).fetchall()
        return [
            ScopedStoredSignal(
                fingerprint=str(row["fingerprint"]),
                source_chat_id=int(row["source_chat_id"]),
                source_topic_id=int(row["source_topic_id"]),
                source_sender_id=int(row["source_sender_id"]),
                source_message_id=int(row["source_message_id"]),
                telegram_received_at=datetime.fromisoformat(
                    str(row["telegram_received_at"])
                ),
                direction=Direction(str(row["direction"])),
                symbol=str(row["symbol"]),
                entry=Decimal(str(row["entry"])),
                take_profit=Decimal(str(row["take_profit"])),
                stop_loss=Decimal(str(row["stop_loss"])),
            )
            for row in rows
        ]

    def max_scoped_message_id(
        self,
        source_chat_id: int,
        source_topic_id: int,
        source_sender_id: int,
    ) -> int:
        if not self._database_path.is_file():
            return 0
        try:
            with self._connection(readonly=True) as connection:
                columns = {
                    str(row["name"])
                    for row in connection.execute("PRAGMA table_info(signals)")
                }
                if not {
                    "source_topic_id",
                    "source_sender_id",
                }.issubset(columns):
                    return 0
                row = connection.execute(
                    """
                    SELECT COALESCE(MAX(source_message_id), 0) AS last_message_id
                    FROM signals
                    WHERE source_chat_id = ? AND source_topic_id = ?
                      AND source_sender_id = ?
                    """,
                    (source_chat_id, source_topic_id, source_sender_id),
                ).fetchone()
        except sqlite3.OperationalError as exc:
            if "no such table" in str(exc):
                return 0
            raise
        return int(row["last_message_id"]) if row is not None else 0

    def get_scoped_signal(
        self,
        source_chat_id: int,
        source_topic_id: int,
        source_sender_id: int,
        source_message_id: int,
    ) -> ScopedStoredSignal | None:
        if not self._database_path.is_file():
            return None
        with self._connection(readonly=True) as connection:
            columns = {
                str(row["name"])
                for row in connection.execute("PRAGMA table_info(signals)")
            }
            if not {"source_topic_id", "source_sender_id"}.issubset(columns):
                return None
            row = connection.execute(
                """
                SELECT fingerprint, source_chat_id, source_topic_id,
                       source_sender_id, source_message_id,
                       telegram_received_at, direction, symbol,
                       entry, take_profit, stop_loss
                FROM signals
                WHERE source_chat_id = ?
                  AND source_topic_id = ?
                  AND source_sender_id = ?
                  AND source_message_id = ?
                """,
                (
                    source_chat_id,
                    source_topic_id,
                    source_sender_id,
                    source_message_id,
                ),
            ).fetchone()
        if row is None:
            return None
        return ScopedStoredSignal(
            fingerprint=str(row["fingerprint"]),
            source_chat_id=int(row["source_chat_id"]),
            source_topic_id=int(row["source_topic_id"]),
            source_sender_id=int(row["source_sender_id"]),
            source_message_id=int(row["source_message_id"]),
            telegram_received_at=datetime.fromisoformat(
                str(row["telegram_received_at"])
            ),
            direction=Direction(str(row["direction"])),
            symbol=str(row["symbol"]),
            entry=Decimal(str(row["entry"])),
            take_profit=Decimal(str(row["take_profit"])),
            stop_loss=Decimal(str(row["stop_loss"])),
        )

    def get_run(self, signal_fingerprint: str) -> sqlite3.Row | None:
        if not self._database_path.is_file():
            return None
        try:
            with self._connection(readonly=True) as connection:
                return connection.execute(
                    """
                    SELECT * FROM demo_signal_bridge_runs
                    WHERE signal_fingerprint = ?
                    """,
                    (signal_fingerprint,),
                ).fetchone()
        except sqlite3.OperationalError as exc:
            if "no such table" in str(exc):
                return None
            raise

    def record_run(
        self,
        signal: ScopedStoredSignal,
        snapshot_at: datetime,
        strategy_equity: Decimal,
        risk_command_id: str,
        decision_status: str,
        decision_reason: str,
        intent_count: int,
    ) -> bool:
        with self._connection() as connection:
            cursor = connection.execute(
                """
                INSERT OR IGNORE INTO demo_signal_bridge_runs (
                    signal_fingerprint, source_chat_id, source_topic_id,
                    source_sender_id, source_message_id, telegram_received_at,
                    snapshot_at, strategy_equity, risk_command_id,
                    decision_status, decision_reason, intent_count, recorded_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    signal.fingerprint,
                    signal.source_chat_id,
                    signal.source_topic_id,
                    signal.source_sender_id,
                    signal.source_message_id,
                    signal.telegram_received_at.isoformat(),
                    snapshot_at.isoformat(),
                    canonical_decimal(strategy_equity),
                    risk_command_id,
                    decision_status,
                    decision_reason,
                    intent_count,
                    datetime.now(timezone.utc).isoformat(),
                ),
            )
            return cursor.rowcount == 1

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
