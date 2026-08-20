from __future__ import annotations

from contextlib import closing
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
import sqlite3
import tempfile
import unittest

from traderrd.application.demo_bridge import DemoBridgeError, DemoSignalBridgeService
from traderrd.application.ingestion import InboundTelegramEvent, SignalIngestionService
from traderrd.domain.bridge import (
    BridgeStatus,
    DemoAccountOrder,
    DemoAccountPosition,
    DemoStrategyAccountSnapshot,
)
from traderrd.domain.execution import InstrumentRules
from traderrd.domain.models import Direction
from traderrd.domain.parser import SignalParser
from traderrd.infrastructure.bridge_repository import SQLiteDemoBridgeRepository
from traderrd.infrastructure.execution_repository import SQLiteDemoExecutionRepository
from traderrd.infrastructure.risk_repository import SQLiteRiskStateRepository
from traderrd.infrastructure.sqlite_repository import SQLiteSignalRepository
from tests.samples import LONG_SIGNAL, SHORT_SIGNAL


NOW = datetime(2026, 8, 17, 15, 0, tzinfo=timezone.utc)
SENDER_ID = 778899


class FakeSnapshotProvider:
    def __init__(
        self,
        equity: Decimal = Decimal("2000"),
        positions: tuple[DemoAccountPosition, ...] = (),
        orders: tuple[DemoAccountOrder, ...] = (),
    ) -> None:
        self.snapshot = DemoStrategyAccountSnapshot(
            equity=equity,
            captured_at=NOW,
            positions=positions,
            orders=orders,
        )
        self.calls: list[str] = []

    def fetch(self, symbol: str):
        self.calls.append(symbol)
        return self.snapshot, InstrumentRules(
            symbol=symbol,
            tick_size=Decimal("0.00001"),
            quantity_step=Decimal("0.001"),
            min_quantity=Decimal("0.001"),
            max_quantity=Decimal("1000000"),
            min_notional=Decimal("1"),
        )


class DemoBridgeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.database = Path(self.temporary_directory.name) / "bridge.sqlite3"
        signal_repository = SQLiteSignalRepository(self.database)
        signal_repository.initialize()
        self.ingestion = SignalIngestionService(SignalParser(), signal_repository)
        self._store(101, LONG_SIGNAL)

    def tearDown(self) -> None:
        self.temporary_directory.cleanup()

    def _store(
        self,
        message_id: int,
        body: str,
        *,
        topic_id: int = 231508,
        sender_id: int = SENDER_ID,
        received_at: datetime = NOW - timedelta(minutes=10),
    ) -> None:
        result = self.ingestion.ingest(
            InboundTelegramEvent(
                source_chat_id=2180632014,
                source_topic_id=topic_id,
                source_sender_id=sender_id,
                source_message_id=message_id,
                raw_text=body,
                telegram_received_at=received_at,
            )
        )
        self.assertEqual(result.outcome.value, "stored")

    def _service(self, provider: FakeSnapshotProvider) -> DemoSignalBridgeService:
        return DemoSignalBridgeService(
            SQLiteDemoBridgeRepository(self.database),
            SQLiteRiskStateRepository(self.database),
            SQLiteDemoExecutionRepository(self.database),
            provider,
            clock=lambda: NOW,
        )

    def test_preview_compounds_from_entire_mark_to_market_equity(self) -> None:
        result = self._service(FakeSnapshotProvider()).process(101, SENDER_ID)

        self.assertEqual(result.status, BridgeStatus.PREVIEW)
        self.assertEqual(result.equity, Decimal("2000"))
        self.assertEqual(
            result.decision.notional,
            Decimal("30") / Decimal("0.031"),
        )
        self.assertEqual(result.decision.projected_risk, Decimal("30"))
        self.assertEqual(result.intents[0].expires_at, NOW + timedelta(hours=2, minutes=50))

    def test_stale_signal_can_be_inspected_but_never_persisted(self) -> None:
        self._store(
            102,
            SHORT_SIGNAL,
            received_at=NOW - timedelta(hours=3),
        )
        result = self._service(FakeSnapshotProvider()).process(
            102, SENDER_ID, persist_risk=True
        )

        self.assertEqual(result.status, BridgeStatus.STALE)
        self.assertEqual(result.intents, ())
        self.assertFalse(self._table_exists("risk_engine_commands"))
        self.assertFalse(self._table_exists("demo_execution_intents"))

    def test_exact_topic_and_sender_provenance_is_required(self) -> None:
        self._store(103, SHORT_SIGNAL, topic_id=999999)
        provider = FakeSnapshotProvider()

        with self.assertRaisesRegex(DemoBridgeError, "exact source provenance"):
            self._service(provider).process(103, SENDER_ID)
        with self.assertRaisesRegex(DemoBridgeError, "exact source provenance"):
            self._service(provider).process(101, SENDER_ID + 1)
        self.assertEqual(provider.calls, [])

    def test_unmanaged_account_order_or_position_fails_closed(self) -> None:
        order_provider = FakeSnapshotProvider(
            orders=(DemoAccountOrder("external-order", "XLMUSDT"),)
        )
        with self.assertRaisesRegex(DemoBridgeError, "not managed"):
            self._service(order_provider).process(101, SENDER_ID)

        position_provider = FakeSnapshotProvider(
            positions=(
                DemoAccountPosition(
                    "XLMUSDT", Direction.LONG, Decimal("12")
                ),
            )
        )
        with self.assertRaisesRegex(DemoBridgeError, "do not match"):
            self._service(position_provider).process(101, SENDER_ID)

    def test_verified_unlinked_exchange_protection_does_not_block_bnb_lifecycle(self) -> None:
        provider = FakeSnapshotProvider(
            orders=(
                DemoAccountOrder(
                    None,
                    "BNBUSDT",
                    exchange_protective_child=True,
                ),
            )
        )

        result = self._service(provider).process(101, SENDER_ID, persist_risk=True)

        self.assertEqual(result.status, BridgeStatus.PERSISTED)
        self.assertEqual(len(result.intents), 1)

    def test_stale_account_snapshot_fails_closed(self) -> None:
        provider = FakeSnapshotProvider()
        provider.snapshot = DemoStrategyAccountSnapshot(
            equity=Decimal("2000"),
            captured_at=NOW - timedelta(minutes=1),
            positions=(),
            orders=(),
        )

        with self.assertRaises(DemoBridgeError) as raised:
            self._service(provider).process(101, SENDER_ID)

        self.assertEqual(raised.exception.code, "account_snapshot_stale")

    def test_preview_does_not_create_risk_execution_or_bridge_tables(self) -> None:
        self._service(FakeSnapshotProvider()).process(101, SENDER_ID)

        self.assertFalse(self._table_exists("risk_engine_commands"))
        self.assertFalse(self._table_exists("demo_execution_intents"))
        self.assertFalse(self._table_exists("demo_signal_bridge_runs"))

    def test_explicit_persist_is_audited_and_duplicate_is_idempotent(self) -> None:
        provider = FakeSnapshotProvider()
        service = self._service(provider)
        first = service.process(101, SENDER_ID, persist_risk=True)
        second = service.process(101, SENDER_ID, persist_risk=True)

        self.assertEqual(first.status, BridgeStatus.PERSISTED)
        self.assertEqual(len(first.intents), 1)
        self.assertEqual(second.status, BridgeStatus.DUPLICATE)
        self.assertEqual(provider.calls, ["XLMUSDT"])
        risk_counts = SQLiteRiskStateRepository(self.database).counts()
        self.assertEqual(risk_counts[0], 2)
        self.assertEqual(SQLiteDemoExecutionRepository(self.database).counts()[0], 1)

    def test_restart_uses_fresh_higher_equity_with_existing_lima_anchors(self) -> None:
        self._service(FakeSnapshotProvider(Decimal("2000"))).process(
            101, SENDER_ID, persist_risk=True
        )
        self._store(104, SHORT_SIGNAL)
        result = self._service(FakeSnapshotProvider(Decimal("3000"))).process(
            104, SENDER_ID, persist_risk=True
        )

        self.assertEqual(result.status, BridgeStatus.PERSISTED)
        self.assertEqual(result.decision.notional, Decimal("45") / Decimal("0.031"))
        state = SQLiteRiskStateRepository(self.database).load_state()
        self.assertEqual(state.equity, Decimal("3000"))
        self.assertEqual(state.daily_start_equity, Decimal("2000"))

    def test_result_contains_no_raw_signal_or_secret_material(self) -> None:
        rendered = repr(self._service(FakeSnapshotProvider()).process(101, SENDER_ID))
        self.assertNotIn("Take Profit", rendered)
        self.assertNotIn("api_secret", rendered)

    def _table_exists(self, name: str) -> bool:
        with closing(sqlite3.connect(self.database)) as connection:
            row = connection.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
                (name,),
            ).fetchone()
        return row is not None


if __name__ == "__main__":
    unittest.main()
