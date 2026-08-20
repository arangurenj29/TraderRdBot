from contextlib import closing
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from io import StringIO
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from traderrd.application.historical_simulation import HistoricalSimulationService
from traderrd.application.ingestion import InboundTelegramEvent, SignalIngestionService
from traderrd.domain.market_data import MarketDataError
from traderrd.domain.parser import SignalParser
from traderrd.domain.simulation import HistoricalCandle, SimulationSettings
from traderrd.historical_simulation_cli import run_historical_simulation
from traderrd.infrastructure.bybit_kline import KlineEvidence
from traderrd.infrastructure.simulation_repository import (
    SQLiteHistoricalSimulationRepository,
)
from traderrd.infrastructure.sqlite_repository import SQLiteSignalRepository
from tests.samples import LONG_SIGNAL


UTC = timezone.utc
CHAT_ID = 2180632014
TOPIC_ID = 231508
MODEL_VERSION = "test-model"


class FakeKlineProvider:
    def __init__(
        self,
        error: MarketDataError | None = None,
        minutes: int = 3,
    ) -> None:
        self.error = error
        self.minutes = minutes
        self.calls = 0

    def fetch_candles(
        self,
        symbol: str,
        interval_minutes: int,
        start: datetime,
        end: datetime,
    ) -> KlineEvidence:
        self.calls += 1
        if self.error is not None:
            raise self.error
        entry = Decimal("0.15688")
        take_profit = Decimal("0.15813504")
        candles = [
            make_candle(
                symbol,
                start,
                entry * Decimal("0.999"),
                entry * Decimal("1.001"),
            ),
            make_candle(
                symbol,
                start + timedelta(minutes=1),
                entry * Decimal("0.999"),
                take_profit * Decimal("1.001"),
            ),
            make_candle(
                symbol,
                start + timedelta(minutes=2),
                entry * Decimal("0.999"),
                entry * Decimal("1.001"),
            ),
        ]
        for minute in range(3, self.minutes):
            candles.append(
                make_candle(
                    symbol,
                    start + timedelta(minutes=minute),
                    entry * Decimal("0.999"),
                    entry * Decimal("1.001"),
                )
            )
        return KlineEvidence(
            candles=candles,
            page_count=1,
            raw_row_count=len(candles),
            dataset_sha256="a" * 64,
            provider_timestamps=(datetime(2026, 8, 17, tzinfo=UTC),),
        )


def make_candle(
    symbol: str,
    open_time: datetime,
    low: Decimal,
    high: Decimal,
) -> HistoricalCandle:
    midpoint = (low + high) / 2
    return HistoricalCandle(
        symbol=symbol,
        interval_minutes=1,
        open_time=open_time,
        open_price=midpoint,
        high_price=high,
        low_price=low,
        close_price=midpoint,
        volume=Decimal("10"),
        turnover=Decimal("1000"),
    )


class HistoricalSimulationIntegrationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.database_path = Path(self.directory.name) / "test.sqlite3"
        self.signal_repository = SQLiteSignalRepository(self.database_path)
        self.signal_repository.initialize()
        self.simulation_repository = SQLiteHistoricalSimulationRepository(
            self.database_path
        )
        self.simulation_repository.initialize()
        self._store_signal(TOPIC_ID + 10)
        self.signal_repository.advance_backfill_checkpoint(
            CHAT_ID, TOPIC_ID, TOPIC_ID + 100
        )

    def tearDown(self) -> None:
        self.directory.cleanup()

    def _store_signal(self, message_id: int, minute_suffix: int = 15) -> None:
        raw = LONG_SIGNAL.replace("02:15", f"02:{minute_suffix:02d}")
        service = SignalIngestionService(SignalParser(), self.signal_repository)
        result = service.ingest(
            InboundTelegramEvent(
                source_chat_id=CHAT_ID,
                source_message_id=message_id,
                raw_text=raw,
                telegram_received_at=datetime(2026, 8, 16, 2, 15, tzinfo=UTC),
            )
        )
        self.assertEqual(result.outcome.value, "stored")

    def _service(self, provider: FakeKlineProvider) -> HistoricalSimulationService:
        return HistoricalSimulationService(
            repository=self.simulation_repository,
            provider=provider,
            source_chat_id=CHAT_ID,
            source_topic_id=TOPIC_ID,
            settings=SimulationSettings(
                entry_window=timedelta(minutes=3),
                exit_horizon=timedelta(minutes=3),
            ),
            model_version=MODEL_VERSION,
        )

    def test_success_is_persisted_with_evidence_and_is_idempotent(self) -> None:
        provider = FakeKlineProvider()
        service = self._service(provider)

        first = service.run()
        second = service.run()

        self.assertEqual(first.take_profit_first, 1)
        self.assertEqual(first.resolved_gross_return_percent, Decimal("0.800"))
        self.assertEqual(second.skipped_existing, 1)
        self.assertEqual(provider.calls, 1)
        with closing(sqlite3.connect(self.database_path)) as connection:
            connection.row_factory = sqlite3.Row
            outcome = connection.execute(
                "SELECT * FROM historical_simulation_outcomes"
            ).fetchone()
            attempts = connection.execute(
                "SELECT COUNT(*) FROM historical_simulation_attempts"
            ).fetchone()[0]
        self.assertEqual(outcome["outcome"], "take_profit_first")
        self.assertEqual(outcome["source_topic_id"], TOPIC_ID)
        self.assertEqual(outcome["dataset_sha256"], "a" * 64)
        self.assertEqual(outcome["source_scope_evidence"], "topic_checkpoint_range")
        self.assertNotEqual(outcome["decisive_candles_json"], "[]")
        self.assertEqual(attempts, 1)

    def test_bounded_rerun_skips_terminal_prefix_and_advances(self) -> None:
        first_provider = FakeKlineProvider()
        self._service(first_provider).run(limit=1)
        self._store_signal(TOPIC_ID + 11, 16)

        second_provider = FakeKlineProvider()
        counters = self._service(second_provider).run(limit=1)

        self.assertEqual(counters.skipped_existing, 1)
        self.assertEqual(counters.attempted, 1)
        self.assertEqual(second_provider.calls, 1)

    def test_data_failure_is_audited_without_losing_signal_and_can_retry(self) -> None:
        failing = FakeKlineProvider(
            MarketDataError("rate_limited", "public rate limit")
        )

        first = self._service(failing).run()
        succeeding = FakeKlineProvider()
        second = self._service(succeeding).run()

        self.assertEqual(first.data_unavailable, 1)
        self.assertEqual(first.errors, 1)
        self.assertEqual(second.take_profit_first, 1)
        with closing(sqlite3.connect(self.database_path)) as connection:
            outcome = connection.execute(
                "SELECT outcome FROM historical_simulation_outcomes"
            ).fetchone()[0]
            attempts = connection.execute(
                "SELECT COUNT(*) FROM historical_simulation_attempts"
            ).fetchone()[0]
            signals = connection.execute("SELECT COUNT(*) FROM signals").fetchone()[0]
        self.assertEqual(outcome, "take_profit_first")
        self.assertEqual(attempts, 2)
        self.assertEqual(signals, 1)

    def test_topic_checkpoint_range_excludes_contaminated_prefix_and_tail(self) -> None:
        self._store_signal(TOPIC_ID - 1, 16)
        self._store_signal(TOPIC_ID + 101, 17)

        eligible = self.simulation_repository.list_eligible_signals(CHAT_ID, TOPIC_ID)

        self.assertEqual([item.source_message_id for item in eligible], [TOPIC_ID + 10])

    def test_dry_run_does_not_call_provider_or_create_simulation_tables(self) -> None:
        with closing(sqlite3.connect(self.database_path)) as connection:
            connection.execute("DROP TABLE historical_simulation_attempts")
            connection.execute("DROP TABLE historical_simulation_outcomes")
            connection.commit()
        provider = FakeKlineProvider()

        with patch("sys.stdout", new_callable=StringIO) as output:
            exit_code = run_historical_simulation(
                database_path=self.database_path,
                source_chat_id=CHAT_ID,
                source_topic_id=TOPIC_ID,
                exit_horizon_hours=24,
                limit=None,
                fetch_public_data=False,
                provider=provider,
            )

        self.assertEqual(exit_code, 0)
        self.assertEqual(provider.calls, 0)
        self.assertIn("eligible=1", output.getvalue())
        self.assertNotIn("XLMUSDT", output.getvalue())
        with closing(sqlite3.connect(self.database_path)) as connection:
            table = connection.execute(
                "SELECT 1 FROM sqlite_master "
                "WHERE type='table' AND name='historical_simulation_outcomes'"
            ).fetchone()
        self.assertIsNone(table)

    def test_explicit_public_data_cli_path_uses_injected_provider(self) -> None:
        provider = FakeKlineProvider(minutes=180)

        with patch("sys.stdout", new_callable=StringIO) as output:
            exit_code = run_historical_simulation(
                database_path=self.database_path,
                source_chat_id=CHAT_ID,
                source_topic_id=TOPIC_ID,
                exit_horizon_hours=24,
                limit=1,
                fetch_public_data=True,
                provider=provider,
            )

        self.assertEqual(exit_code, 0)
        self.assertEqual(provider.calls, 1)
        self.assertIn("take_profit_first=1", output.getvalue())
        self.assertNotIn("XLMUSDT", output.getvalue())


if __name__ == "__main__":
    unittest.main()
