from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
from decimal import Decimal
import json
from pathlib import Path
import sqlite3
from typing import Iterator

from traderrd.domain.models import Direction, canonical_decimal
from traderrd.domain.simulation import (
    HistoricalCandle,
    SimulationOutcome,
    SimulationResult,
    SimulationSettings,
    SimulationSignal,
)
from traderrd.infrastructure.bybit_kline import KlineEvidence


TERMINAL_OUTCOMES = {
    SimulationOutcome.NO_ENTRY_BEFORE_EXPIRY.value,
    SimulationOutcome.TAKE_PROFIT_FIRST.value,
    SimulationOutcome.STOP_LOSS_FIRST.value,
    SimulationOutcome.NO_EXIT_WITHIN_HORIZON.value,
    SimulationOutcome.INTRABAR_AMBIGUITY.value,
}


class SQLiteHistoricalSimulationRepository:
    def __init__(self, database_path: str | Path) -> None:
        self._database_path = Path(database_path)

    def initialize(self) -> None:
        with self._connection() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS historical_simulation_outcomes (
                    id INTEGER PRIMARY KEY,
                    signal_fingerprint TEXT NOT NULL,
                    model_version TEXT NOT NULL,
                    source_chat_id INTEGER NOT NULL,
                    source_topic_id INTEGER NOT NULL,
                    source_message_id INTEGER NOT NULL,
                    symbol TEXT NOT NULL,
                    outcome TEXT NOT NULL CHECK (outcome IN (
                        'no_entry_before_expiry', 'take_profit_first',
                        'stop_loss_first', 'no_exit_within_horizon',
                        'intrabar_ambiguity', 'data_unavailable'
                    )),
                    evaluation_start TEXT NOT NULL,
                    entry_expiry TEXT NOT NULL,
                    exit_horizon_end TEXT,
                    entry_candle_time TEXT,
                    exit_candle_time TEXT,
                    candle_count INTEGER NOT NULL,
                    gross_return_percent TEXT,
                    ambiguity_reason TEXT,
                    provider TEXT NOT NULL,
                    category TEXT NOT NULL,
                    interval_minutes INTEGER NOT NULL,
                    requested_start TEXT NOT NULL,
                    requested_end TEXT NOT NULL,
                    page_count INTEGER NOT NULL,
                    raw_row_count INTEGER NOT NULL,
                    dataset_sha256 TEXT,
                    provider_timestamps_json TEXT NOT NULL,
                    decisive_candles_json TEXT NOT NULL,
                    error_code TEXT,
                    error_detail TEXT,
                    simulated_at TEXT NOT NULL,
                    source_scope_evidence TEXT NOT NULL,
                    UNIQUE (signal_fingerprint, model_version)
                );

                CREATE TABLE IF NOT EXISTS historical_simulation_attempts (
                    id INTEGER PRIMARY KEY,
                    signal_fingerprint TEXT NOT NULL,
                    model_version TEXT NOT NULL,
                    attempted_at TEXT NOT NULL,
                    outcome TEXT NOT NULL,
                    error_code TEXT,
                    dataset_sha256 TEXT
                );

                CREATE INDEX IF NOT EXISTS idx_simulation_outcome
                    ON historical_simulation_outcomes(outcome);
                CREATE INDEX IF NOT EXISTS idx_simulation_attempt_fingerprint
                    ON historical_simulation_attempts(signal_fingerprint);
                """
            )

    def list_eligible_signals(
        self,
        source_chat_id: int,
        source_topic_id: int,
        limit: int | None = None,
    ) -> list[SimulationSignal]:
        with self._connection(readonly=True) as connection:
            checkpoint = connection.execute(
                """
                SELECT last_message_id FROM topic_backfill_checkpoints
                WHERE source_chat_id = ? AND source_topic_id = ?
                """,
                (source_chat_id, source_topic_id),
            ).fetchone()
            if checkpoint is None:
                return []
            query = """
                SELECT fingerprint, direction, symbol, entry, take_profit,
                       stop_loss, signal_timestamp, telegram_received_at,
                       source_chat_id, source_message_id
                FROM signals
                WHERE source_chat_id = ?
                  AND source_message_id >= ?
                  AND source_message_id <= ?
                ORDER BY source_message_id
            """
            parameters: list[int] = [
                source_chat_id,
                source_topic_id,
                int(checkpoint["last_message_id"]),
            ]
            if limit is not None:
                query += " LIMIT ?"
                parameters.append(limit)
            rows = connection.execute(query, parameters).fetchall()
        return [self._signal_from_row(row) for row in rows]

    def has_terminal_outcome(self, fingerprint: str, model_version: str) -> bool:
        with self._connection(readonly=True) as connection:
            row = connection.execute(
                """
                SELECT outcome FROM historical_simulation_outcomes
                WHERE signal_fingerprint = ? AND model_version = ?
                """,
                (fingerprint, model_version),
            ).fetchone()
        return row is not None and row["outcome"] in TERMINAL_OUTCOMES

    def record_success(
        self,
        signal: SimulationSignal,
        source_topic_id: int,
        model_version: str,
        settings: SimulationSettings,
        requested_start: datetime,
        requested_end: datetime,
        result: SimulationResult,
        evidence: KlineEvidence,
    ) -> None:
        now = datetime.now(timezone.utc).isoformat()
        values = self._base_values(
            signal,
            source_topic_id,
            model_version,
            settings,
            requested_start,
            requested_end,
            result.outcome.value,
            result.evaluation_start,
            result.entry_expiry,
            evidence,
            now,
        )
        values.update(
            {
                "exit_horizon_end": self._iso(result.exit_horizon_end),
                "entry_candle_time": self._candle_time(result.entry_candle),
                "exit_candle_time": self._candle_time(result.exit_candle),
                "candle_count": result.candle_count,
                "gross_return_percent": (
                    canonical_decimal(result.gross_return_percent)
                    if result.gross_return_percent is not None
                    else None
                ),
                "ambiguity_reason": result.ambiguity_reason,
                "decisive_candles_json": self._candles_json(
                    result.entry_candle, result.exit_candle
                ),
                "error_code": None,
                "error_detail": None,
            }
        )
        self._upsert(values)

    def record_failure(
        self,
        signal: SimulationSignal,
        source_topic_id: int,
        model_version: str,
        settings: SimulationSettings,
        requested_start: datetime,
        requested_end: datetime,
        error_code: str,
        error_detail: str,
        evidence: KlineEvidence | None = None,
    ) -> None:
        now = datetime.now(timezone.utc).isoformat()
        evidence = evidence or KlineEvidence([], 0, 0, "", ())
        values = self._base_values(
            signal,
            source_topic_id,
            model_version,
            settings,
            requested_start,
            requested_end,
            SimulationOutcome.DATA_UNAVAILABLE.value,
            requested_start,
            signal.telegram_received_at + settings.entry_window,
            evidence,
            now,
        )
        values.update(
            {
                "exit_horizon_end": None,
                "entry_candle_time": None,
                "exit_candle_time": None,
                "candle_count": len(evidence.candles),
                "gross_return_percent": None,
                "ambiguity_reason": None,
                "decisive_candles_json": "[]",
                "error_code": error_code,
                "error_detail": error_detail[:500],
            }
        )
        self._upsert(values)

    def count_outcomes(self) -> dict[str, int]:
        with self._connection(readonly=True) as connection:
            rows = connection.execute(
                """
                SELECT outcome, COUNT(*) AS count
                FROM historical_simulation_outcomes GROUP BY outcome
                """
            ).fetchall()
        return {str(row["outcome"]): int(row["count"]) for row in rows}

    def _upsert(self, values: dict[str, object]) -> None:
        columns = list(values)
        placeholders = ", ".join("?" for _ in columns)
        assignments = ", ".join(
            f"{column} = excluded.{column}"
            for column in columns
            if column not in {"signal_fingerprint", "model_version"}
        )
        with self._connection() as connection:
            connection.execute(
                f"""
                INSERT INTO historical_simulation_outcomes ({', '.join(columns)})
                VALUES ({placeholders})
                ON CONFLICT(signal_fingerprint, model_version) DO UPDATE SET
                    {assignments}
                """,
                [values[column] for column in columns],
            )
            connection.execute(
                """
                INSERT INTO historical_simulation_attempts (
                    signal_fingerprint, model_version, attempted_at,
                    outcome, error_code, dataset_sha256
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    values["signal_fingerprint"],
                    values["model_version"],
                    values["simulated_at"],
                    values["outcome"],
                    values["error_code"],
                    values["dataset_sha256"],
                ),
            )

    def _base_values(
        self,
        signal: SimulationSignal,
        source_topic_id: int,
        model_version: str,
        settings: SimulationSettings,
        requested_start: datetime,
        requested_end: datetime,
        outcome: str,
        evaluation_start: datetime,
        entry_expiry: datetime,
        evidence: KlineEvidence,
        now: str,
    ) -> dict[str, object]:
        return {
            "signal_fingerprint": signal.fingerprint,
            "model_version": model_version,
            "source_chat_id": signal.source_chat_id,
            "source_topic_id": source_topic_id,
            "source_message_id": signal.source_message_id,
            "symbol": signal.symbol,
            "outcome": outcome,
            "evaluation_start": evaluation_start.isoformat(),
            "entry_expiry": entry_expiry.isoformat(),
            "provider": "bybit",
            "category": "linear",
            "interval_minutes": settings.interval_minutes,
            "requested_start": requested_start.isoformat(),
            "requested_end": requested_end.isoformat(),
            "page_count": evidence.page_count,
            "raw_row_count": evidence.raw_row_count,
            "dataset_sha256": evidence.dataset_sha256 or None,
            "provider_timestamps_json": json.dumps(
                [value.isoformat() for value in evidence.provider_timestamps]
            ),
            "simulated_at": now,
            "source_scope_evidence": "topic_checkpoint_range",
        }

    @staticmethod
    def _signal_from_row(row: sqlite3.Row) -> SimulationSignal:
        return SimulationSignal(
            fingerprint=str(row["fingerprint"]),
            direction=Direction(str(row["direction"])),
            symbol=str(row["symbol"]),
            entry=Decimal(str(row["entry"])),
            take_profit=Decimal(str(row["take_profit"])),
            stop_loss=Decimal(str(row["stop_loss"])),
            signal_timestamp=datetime.fromisoformat(str(row["signal_timestamp"])),
            telegram_received_at=datetime.fromisoformat(
                str(row["telegram_received_at"])
            ),
            source_chat_id=int(row["source_chat_id"]),
            source_message_id=int(row["source_message_id"]),
        )

    @staticmethod
    def _candles_json(
        entry_candle: HistoricalCandle | None,
        exit_candle: HistoricalCandle | None,
    ) -> str:
        unique: dict[str, HistoricalCandle] = {}
        for candle in (entry_candle, exit_candle):
            if candle is not None:
                unique[candle.open_time.isoformat()] = candle
        rows = [
            {
                "open_time": candle.open_time.isoformat(),
                "open": canonical_decimal(candle.open_price),
                "high": canonical_decimal(candle.high_price),
                "low": canonical_decimal(candle.low_price),
                "close": canonical_decimal(candle.close_price),
                "volume": canonical_decimal(candle.volume),
                "turnover": canonical_decimal(candle.turnover),
            }
            for candle in unique.values()
        ]
        return json.dumps(rows, separators=(",", ":"), sort_keys=True)

    @staticmethod
    def _candle_time(candle: HistoricalCandle | None) -> str | None:
        return candle.open_time.isoformat() if candle is not None else None

    @staticmethod
    def _iso(value: datetime | None) -> str | None:
        return value.isoformat() if value is not None else None

    @contextmanager
    def _connection(
        self, readonly: bool = False
    ) -> Iterator[sqlite3.Connection]:
        if readonly:
            database = f"{self._database_path.resolve().as_uri()}?mode=ro"
            connection = sqlite3.connect(
                database,
                timeout=5,
                uri=True,
            )
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
