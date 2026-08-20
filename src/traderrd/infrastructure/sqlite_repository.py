from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
import sqlite3

from traderrd.application.ingestion import (
    InboundTelegramEvent,
    IngestionOutcome,
    IngestionResult,
    QuoteCapture,
)
from traderrd.domain.models import ObservedSignal, SignalDraft, canonical_decimal
from traderrd.domain.parser import SignalParseError


class SQLiteSignalRepository:
    def __init__(self, database_path: str | Path) -> None:
        self._database_path = Path(database_path)

    def initialize(self) -> None:
        self._database_path.parent.mkdir(parents=True, exist_ok=True)
        with self._connection() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS inbound_messages (
                    id INTEGER PRIMARY KEY,
                    source_chat_id INTEGER NOT NULL,
                    source_message_id INTEGER NOT NULL,
                    source_topic_id INTEGER,
                    source_sender_id INTEGER,
                    raw_text TEXT NOT NULL,
                    telegram_received_at TEXT NOT NULL,
                    telegram_edited_at TEXT,
                    first_seen_at TEXT NOT NULL,
                    last_seen_at TEXT NOT NULL,
                    outcome TEXT NOT NULL CHECK (outcome IN (
                        'pending', 'stored', 'duplicate_message',
                        'duplicate_signal', 'invalid'
                    )),
                    error_code TEXT,
                    error_detail TEXT,
                    signal_fingerprint TEXT,
                    UNIQUE (source_chat_id, source_message_id)
                );

                CREATE TABLE IF NOT EXISTS inbound_revisions (
                    id INTEGER PRIMARY KEY,
                    inbound_message_id INTEGER NOT NULL
                        REFERENCES inbound_messages(id) ON DELETE CASCADE,
                    raw_text TEXT NOT NULL,
                    observed_at TEXT NOT NULL,
                    telegram_edited_at TEXT,
                    is_edit INTEGER NOT NULL CHECK (is_edit IN (0, 1)),
                    outcome TEXT NOT NULL CHECK (outcome IN (
                        'stored', 'duplicate_message', 'duplicate_signal', 'invalid'
                    )),
                    error_code TEXT,
                    error_detail TEXT,
                    signal_fingerprint TEXT
                );

                CREATE TABLE IF NOT EXISTS signals (
                    id INTEGER PRIMARY KEY,
                    inbound_message_id INTEGER NOT NULL UNIQUE
                        REFERENCES inbound_messages(id) ON DELETE CASCADE,
                    fingerprint TEXT NOT NULL UNIQUE,
                    direction TEXT NOT NULL CHECK (direction IN ('LONG', 'SHORT')),
                    symbol TEXT NOT NULL,
                    timeframe_minutes INTEGER NOT NULL,
                    entry TEXT NOT NULL,
                    take_profit TEXT NOT NULL,
                    stop_loss TEXT NOT NULL,
                    signal_timestamp TEXT NOT NULL,
                    source_chat_id INTEGER NOT NULL,
                    source_message_id INTEGER NOT NULL,
                    source_topic_id INTEGER,
                    source_sender_id INTEGER,
                    telegram_received_at TEXT NOT NULL,
                    telegram_edited_at TEXT,
                    stored_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS market_quote_captures (
                    id INTEGER PRIMARY KEY,
                    signal_fingerprint TEXT NOT NULL,
                    source_chat_id INTEGER NOT NULL,
                    source_message_id INTEGER NOT NULL,
                    provider TEXT NOT NULL,
                    category TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    outcome TEXT NOT NULL CHECK (outcome IN ('captured', 'failed')),
                    best_bid TEXT,
                    best_ask TEXT,
                    last_price TEXT,
                    mark_price TEXT,
                    provider_timestamp TEXT,
                    captured_at TEXT NOT NULL,
                    error_code TEXT,
                    error_detail TEXT,
                    recorded_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS backfill_checkpoints (
                    source_chat_id INTEGER PRIMARY KEY,
                    last_message_id INTEGER NOT NULL CHECK (last_message_id > 0),
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS topic_backfill_checkpoints (
                    source_chat_id INTEGER NOT NULL,
                    source_topic_id INTEGER NOT NULL,
                    last_message_id INTEGER NOT NULL CHECK (last_message_id > 0),
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (source_chat_id, source_topic_id)
                );

                CREATE TABLE IF NOT EXISTS audit_reprocessing_attempts (
                    id INTEGER PRIMARY KEY,
                    inbound_message_id INTEGER NOT NULL
                        REFERENCES inbound_messages(id) ON DELETE CASCADE,
                    attempted_at TEXT NOT NULL,
                    outcome TEXT NOT NULL CHECK (outcome IN (
                        'stored', 'duplicate_signal', 'invalid'
                    )),
                    error_code TEXT,
                    signal_fingerprint TEXT
                );

                CREATE INDEX IF NOT EXISTS idx_inbound_outcome
                    ON inbound_messages(outcome);
                CREATE INDEX IF NOT EXISTS idx_signals_symbol_timestamp
                    ON signals(symbol, signal_timestamp);
                CREATE INDEX IF NOT EXISTS idx_quote_captures_fingerprint
                    ON market_quote_captures(signal_fingerprint);
                CREATE INDEX IF NOT EXISTS idx_reprocessing_inbound
                    ON audit_reprocessing_attempts(inbound_message_id);
                """
            )
            self._ensure_column(
                connection, "inbound_messages", "source_topic_id", "INTEGER"
            )
            self._ensure_column(
                connection, "inbound_messages", "source_sender_id", "INTEGER"
            )
            self._ensure_column(connection, "signals", "source_topic_id", "INTEGER")
            self._ensure_column(connection, "signals", "source_sender_id", "INTEGER")

    def record_event(
        self,
        event: InboundTelegramEvent,
        draft: SignalDraft | None,
        parse_error: SignalParseError | None,
    ) -> IngestionResult:
        now = datetime.now(timezone.utc).isoformat()
        with self._connection() as connection:
            existing = connection.execute(
                """
                SELECT id, raw_text
                FROM inbound_messages
                WHERE source_chat_id = ? AND source_message_id = ?
                """,
                (event.source_chat_id, event.source_message_id),
            ).fetchone()

            if existing is not None and (
                not event.is_edit or existing["raw_text"] == event.raw_text
            ):
                inbound_id = int(existing["id"])
                result = IngestionResult(
                    outcome=IngestionOutcome.DUPLICATE_MESSAGE,
                    source_chat_id=event.source_chat_id,
                    source_message_id=event.source_message_id,
                    fingerprint=draft.fingerprint if draft is not None else None,
                    error_code=parse_error.code if parse_error is not None else None,
                    detail="Telegram message ID was already observed",
                )
                connection.execute(
                    """
                    UPDATE inbound_messages
                    SET last_seen_at = ?
                    WHERE id = ?
                    """,
                    (now, inbound_id),
                )
                self._insert_revision(connection, inbound_id, event, result, now)
                return result

            if existing is None:
                cursor = connection.execute(
                    """
                    INSERT INTO inbound_messages (
                        source_chat_id, source_message_id,
                        source_topic_id, source_sender_id, raw_text,
                        telegram_received_at, telegram_edited_at,
                        first_seen_at, last_seen_at, outcome
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending')
                    """,
                    (
                        event.source_chat_id,
                        event.source_message_id,
                        event.source_topic_id,
                        event.source_sender_id,
                        event.raw_text,
                        event.telegram_received_at.isoformat(),
                        self._iso(event.telegram_edited_at),
                        now,
                        now,
                    ),
                )
                inbound_id = int(cursor.lastrowid)
            else:
                inbound_id = int(existing["id"])
                connection.execute(
                    "DELETE FROM signals WHERE inbound_message_id = ?", (inbound_id,)
                )

            if parse_error is not None:
                result = IngestionResult(
                    outcome=IngestionOutcome.INVALID,
                    source_chat_id=event.source_chat_id,
                    source_message_id=event.source_message_id,
                    error_code=parse_error.code,
                    detail=str(parse_error),
                )
            else:
                if draft is None:
                    raise ValueError("draft and parse_error cannot both be absent")
                observed = ObservedSignal(
                    draft=draft,
                    source_chat_id=event.source_chat_id,
                    source_message_id=event.source_message_id,
                    telegram_received_at=event.telegram_received_at,
                    source_topic_id=event.source_topic_id,
                    source_sender_id=event.source_sender_id,
                    telegram_edited_at=event.telegram_edited_at,
                )
                result = self._store_signal(connection, inbound_id, observed, now)

            self._update_inbound(connection, inbound_id, event, result, now)
            self._insert_revision(connection, inbound_id, event, result, now)
            return result

    def record_quote_capture(
        self,
        event: InboundTelegramEvent,
        signal_fingerprint: str,
        capture: QuoteCapture,
    ) -> None:
        quote = capture.quote
        with self._connection() as connection:
            connection.execute(
                """
                INSERT INTO market_quote_captures (
                    signal_fingerprint, source_chat_id, source_message_id,
                    provider, category, symbol, outcome, best_bid, best_ask,
                    last_price, mark_price, provider_timestamp, captured_at,
                    error_code, error_detail, recorded_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    signal_fingerprint,
                    event.source_chat_id,
                    event.source_message_id,
                    capture.provider,
                    capture.category,
                    capture.symbol,
                    capture.outcome.value,
                    canonical_decimal(quote.best_bid) if quote else None,
                    canonical_decimal(quote.best_ask) if quote else None,
                    canonical_decimal(quote.last_price) if quote else None,
                    (
                        canonical_decimal(quote.mark_price)
                        if quote and quote.mark_price is not None
                        else None
                    ),
                    quote.provider_timestamp.isoformat() if quote else None,
                    capture.captured_at.isoformat(),
                    capture.error_code,
                    capture.error_detail,
                    datetime.now(timezone.utc).isoformat(),
                ),
            )

    def get_backfill_checkpoint(self, source_chat_id: int, source_topic_id: int) -> int:
        with self._connection() as connection:
            row = connection.execute(
                """
                SELECT last_message_id
                FROM topic_backfill_checkpoints
                WHERE source_chat_id = ? AND source_topic_id = ?
                """,
                (source_chat_id, source_topic_id),
            ).fetchone()
            return int(row["last_message_id"]) if row is not None else 0

    def advance_backfill_checkpoint(
        self,
        source_chat_id: int,
        source_topic_id: int,
        source_message_id: int,
    ) -> None:
        if min(source_chat_id, source_topic_id, source_message_id) <= 0:
            raise ValueError("Backfill chat, topic, and message IDs must be positive")
        with self._connection() as connection:
            connection.execute(
                """
                INSERT INTO topic_backfill_checkpoints (
                    source_chat_id, source_topic_id, last_message_id, updated_at
                ) VALUES (?, ?, ?, ?)
                ON CONFLICT(source_chat_id, source_topic_id) DO UPDATE SET
                    last_message_id = excluded.last_message_id,
                    updated_at = excluded.updated_at
                WHERE excluded.last_message_id
                    > topic_backfill_checkpoints.last_message_id
                """,
                (
                    source_chat_id,
                    source_topic_id,
                    source_message_id,
                    datetime.now(timezone.utc).isoformat(),
                ),
            )

    def list_reprocessable_messages(
        self, limit: int | None = None
    ) -> list[InboundTelegramEvent]:
        query = """
            SELECT source_chat_id, source_message_id, raw_text,
                   source_topic_id, source_sender_id,
                   telegram_received_at, telegram_edited_at
            FROM inbound_messages
            WHERE outcome IN ('invalid', 'duplicate_message')
            ORDER BY source_chat_id, source_message_id
        """
        parameters: tuple[int, ...] = ()
        if limit is not None:
            query += " LIMIT ?"
            parameters = (limit,)
        with self._connection() as connection:
            rows = connection.execute(query, parameters).fetchall()
        return [
            InboundTelegramEvent(
                source_chat_id=int(row["source_chat_id"]),
                source_message_id=int(row["source_message_id"]),
                raw_text=str(row["raw_text"]),
                telegram_received_at=datetime.fromisoformat(
                    row["telegram_received_at"]
                ),
                source_topic_id=(
                    int(row["source_topic_id"])
                    if row["source_topic_id"] is not None
                    else None
                ),
                source_sender_id=(
                    int(row["source_sender_id"])
                    if row["source_sender_id"] is not None
                    else None
                ),
                is_edit=row["telegram_edited_at"] is not None,
                telegram_edited_at=(
                    datetime.fromisoformat(row["telegram_edited_at"])
                    if row["telegram_edited_at"] is not None
                    else None
                ),
            )
            for row in rows
        ]

    def reprocess_event(
        self,
        event: InboundTelegramEvent,
        draft: SignalDraft | None,
        parse_error: SignalParseError | None,
    ) -> IngestionResult:
        now = datetime.now(timezone.utc).isoformat()
        with self._connection() as connection:
            inbound = connection.execute(
                """
                SELECT id, outcome
                FROM inbound_messages
                WHERE source_chat_id = ? AND source_message_id = ?
                """,
                (event.source_chat_id, event.source_message_id),
            ).fetchone()
            if inbound is None or inbound["outcome"] not in {
                "invalid",
                "duplicate_message",
            }:
                raise ValueError("Inbound message is not eligible for reprocessing")
            inbound_id = int(inbound["id"])
            existing_signal = connection.execute(
                """
                SELECT fingerprint FROM signals WHERE inbound_message_id = ?
                """,
                (inbound_id,),
            ).fetchone()

            if existing_signal is not None:
                result = IngestionResult(
                    outcome=IngestionOutcome.STORED,
                    source_chat_id=event.source_chat_id,
                    source_message_id=event.source_message_id,
                    fingerprint=str(existing_signal["fingerprint"]),
                )
            elif parse_error is not None:
                result = IngestionResult(
                    outcome=IngestionOutcome.INVALID,
                    source_chat_id=event.source_chat_id,
                    source_message_id=event.source_message_id,
                    error_code=parse_error.code,
                    detail=str(parse_error),
                )
            else:
                if draft is None:
                    raise ValueError("draft and parse_error cannot both be absent")
                result = self._store_signal(
                    connection,
                    inbound_id,
                    ObservedSignal(
                        draft=draft,
                        source_chat_id=event.source_chat_id,
                        source_message_id=event.source_message_id,
                        telegram_received_at=event.telegram_received_at,
                        source_topic_id=event.source_topic_id,
                        source_sender_id=event.source_sender_id,
                        telegram_edited_at=event.telegram_edited_at,
                    ),
                    now,
                )

            self._update_inbound(connection, inbound_id, event, result, now)
            connection.execute(
                """
                INSERT INTO audit_reprocessing_attempts (
                    inbound_message_id, attempted_at, outcome,
                    error_code, signal_fingerprint
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (
                    inbound_id,
                    now,
                    result.outcome.value,
                    result.error_code,
                    result.fingerprint,
                ),
            )
            return result

    def _store_signal(
        self,
        connection: sqlite3.Connection,
        inbound_id: int,
        signal: ObservedSignal,
        now: str,
    ) -> IngestionResult:
        draft = signal.draft
        try:
            connection.execute(
                """
                INSERT INTO signals (
                    inbound_message_id, fingerprint, direction, symbol,
                    timeframe_minutes, entry, take_profit, stop_loss,
                    signal_timestamp, source_chat_id, source_message_id,
                    source_topic_id, source_sender_id,
                    telegram_received_at, telegram_edited_at, stored_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    inbound_id,
                    signal.fingerprint,
                    draft.direction.value,
                    draft.symbol,
                    draft.timeframe_minutes,
                    canonical_decimal(draft.entry),
                    canonical_decimal(draft.take_profit),
                    canonical_decimal(draft.stop_loss),
                    draft.signal_timestamp.isoformat(timespec="minutes"),
                    signal.source_chat_id,
                    signal.source_message_id,
                    signal.source_topic_id,
                    signal.source_sender_id,
                    signal.telegram_received_at.isoformat(),
                    self._iso(signal.telegram_edited_at),
                    now,
                ),
            )
        except sqlite3.IntegrityError:
            duplicate = connection.execute(
                "SELECT 1 FROM signals WHERE fingerprint = ?", (signal.fingerprint,)
            ).fetchone()
            if duplicate is None:
                raise
            return IngestionResult(
                outcome=IngestionOutcome.DUPLICATE_SIGNAL,
                source_chat_id=signal.source_chat_id,
                source_message_id=signal.source_message_id,
                fingerprint=signal.fingerprint,
                detail="Signal content fingerprint was already stored",
            )
        return IngestionResult(
            outcome=IngestionOutcome.STORED,
            source_chat_id=signal.source_chat_id,
            source_message_id=signal.source_message_id,
            fingerprint=signal.fingerprint,
        )

    @staticmethod
    def _update_inbound(
        connection: sqlite3.Connection,
        inbound_id: int,
        event: InboundTelegramEvent,
        result: IngestionResult,
        now: str,
    ) -> None:
        connection.execute(
            """
            UPDATE inbound_messages
            SET raw_text = ?, telegram_received_at = ?, telegram_edited_at = ?,
                source_topic_id = ?, source_sender_id = ?,
                last_seen_at = ?, outcome = ?, error_code = ?, error_detail = ?,
                signal_fingerprint = ?
            WHERE id = ?
            """,
            (
                event.raw_text,
                event.telegram_received_at.isoformat(),
                SQLiteSignalRepository._iso(event.telegram_edited_at),
                event.source_topic_id,
                event.source_sender_id,
                now,
                result.outcome.value,
                result.error_code,
                result.detail,
                result.fingerprint,
                inbound_id,
            ),
        )

    @staticmethod
    def _insert_revision(
        connection: sqlite3.Connection,
        inbound_id: int,
        event: InboundTelegramEvent,
        result: IngestionResult,
        observed_at: str,
    ) -> None:
        connection.execute(
            """
            INSERT INTO inbound_revisions (
                inbound_message_id, raw_text, observed_at, telegram_edited_at,
                is_edit, outcome, error_code, error_detail, signal_fingerprint
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                inbound_id,
                event.raw_text,
                observed_at,
                SQLiteSignalRepository._iso(event.telegram_edited_at),
                int(event.is_edit),
                result.outcome.value,
                result.error_code,
                result.detail,
                result.fingerprint,
            ),
        )

    def count(self, table: str) -> int:
        if table not in {
            "inbound_messages",
            "inbound_revisions",
            "signals",
            "market_quote_captures",
            "backfill_checkpoints",
            "topic_backfill_checkpoints",
            "audit_reprocessing_attempts",
        }:
            raise ValueError("Unsupported table")
        with self._connection() as connection:
            row = connection.execute(f"SELECT COUNT(*) AS count FROM {table}").fetchone()
            return int(row["count"])

    def get_inbound(self, source_chat_id: int, source_message_id: int) -> sqlite3.Row | None:
        with self._connection() as connection:
            return connection.execute(
                """
                SELECT * FROM inbound_messages
                WHERE source_chat_id = ? AND source_message_id = ?
                """,
                (source_chat_id, source_message_id),
            ).fetchone()

    def get_quote_captures(self, signal_fingerprint: str) -> list[sqlite3.Row]:
        with self._connection() as connection:
            return connection.execute(
                """
                SELECT * FROM market_quote_captures
                WHERE signal_fingerprint = ?
                ORDER BY id
                """,
                (signal_fingerprint,),
            ).fetchall()

    @contextmanager
    def _connection(self) -> Iterator[sqlite3.Connection]:
        connection = sqlite3.connect(self._database_path, timeout=5)
        try:
            connection.row_factory = sqlite3.Row
            connection.execute("PRAGMA foreign_keys = ON")
            connection.execute("PRAGMA busy_timeout = 5000")
            connection.execute("PRAGMA journal_mode = WAL")
            with connection:
                yield connection
        finally:
            connection.close()

    @staticmethod
    def _ensure_column(
        connection: sqlite3.Connection,
        table: str,
        column: str,
        definition: str,
    ) -> None:
        columns = {
            str(row["name"])
            for row in connection.execute(f"PRAGMA table_info({table})").fetchall()
        }
        if column not in columns:
            connection.execute(
                f"ALTER TABLE {table} ADD COLUMN {column} {definition}"
            )

    @staticmethod
    def _iso(value: datetime | None) -> str | None:
        return value.isoformat() if value is not None else None
