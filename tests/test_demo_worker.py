from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
import tempfile
import unittest

from traderrd.application.demo_worker import DemoSignalWorker, DemoWorkerError
from traderrd.application.ingestion import InboundTelegramEvent, SignalIngestionService
from traderrd.domain.bridge import DemoStrategyAccountSnapshot
from traderrd.domain.execution import InstrumentRules
from traderrd.domain.models import Direction
from traderrd.domain.parser import SignalParser
from traderrd.infrastructure.bridge_repository import SQLiteDemoBridgeRepository
from traderrd.infrastructure.execution_repository import SQLiteDemoExecutionRepository
from traderrd.infrastructure.risk_repository import SQLiteRiskStateRepository
from traderrd.infrastructure.sqlite_repository import SQLiteSignalRepository
from tests.samples import LONG_SIGNAL, SHORT_SIGNAL


UTC = timezone.utc
NOW = datetime(2026, 8, 18, 15, 0, tzinfo=UTC)


@dataclass
class FakeClientConfig:
    base_url: str = "https://api-demo.bybit.com"


class FakeClient:
    def __init__(self) -> None:
        self.config = FakeClientConfig()
        self.calls: list[tuple[str, str, dict[str, object]]] = []

    def request(self, method, path, params=None, body=None, private=True):
        values = body or params or {}
        self.calls.append((method, path, values))
        if method == "POST" and path == "/v5/order/create":
            return {"result": {"orderId": "demo-order-1"}}
        raise AssertionError((method, path))


class FakeSnapshotProvider:
    def __init__(self) -> None:
        self.calls: list[str] = []
        self.snapshot = DemoStrategyAccountSnapshot(
            equity=Decimal("1000"),
            captured_at=NOW,
            positions=(),
            orders=(),
        )

    def fetch(self, symbol: str):
        self.calls.append(symbol)
        return self.snapshot, InstrumentRules(
            symbol=symbol,
            tick_size=Decimal("0.1"),
            quantity_step=Decimal("0.001"),
            min_quantity=Decimal("0.001"),
            max_quantity=Decimal("1000000"),
            min_notional=Decimal("5"),
        )


class DemoWorkerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.database = Path(self.directory.name) / "worker.sqlite3"
        self.signals = SQLiteSignalRepository(self.database)
        self.signals.initialize()
        self.ingestion = SignalIngestionService(SignalParser(), self.signals)
        self.clock = lambda: NOW
        self._store(101, NOW - timedelta(minutes=5))

    def tearDown(self) -> None:
        self.directory.cleanup()

    def _store(
        self,
        message_id: int,
        received_at: datetime,
        *,
        sender_id: int = 8003985182,
        topic_id: int = 231508,
        body: str | None = None,
    ) -> None:
        result = self.ingestion.ingest(
            InboundTelegramEvent(
                source_chat_id=2180632014,
                source_topic_id=topic_id,
                source_sender_id=sender_id,
                source_message_id=message_id,
                raw_text=body or LONG_SIGNAL,
                telegram_received_at=received_at,
            )
        )
        self.assertEqual(result.outcome.value, "stored")

    def _worker(self, client=None) -> DemoSignalWorker:
        return DemoSignalWorker(
            SQLiteDemoBridgeRepository(self.database),
            SQLiteRiskStateRepository(self.database),
            SQLiteDemoExecutionRepository(self.database),
            FakeSnapshotProvider(),
            client or FakeClient(),
            clock=self.clock,
        )

    def test_first_apply_cycle_establishes_live_boundary_without_replaying_history(self):
        result = self._worker().run_cycle(apply=True)

        self.assertEqual(result.scanned, 0)
        self.assertEqual(result.cursor, 101)
        self.assertEqual(len(FakeClient().calls), 0)
        self.assertEqual(
            SQLiteDemoExecutionRepository(self.database).counts()[0], 0
        )

    def test_new_signal_is_persisted_and_submitted_once(self):
        worker = self._worker()
        worker.run_cycle(apply=True)
        self._store(102, NOW - timedelta(minutes=1), body=SHORT_SIGNAL)
        client = FakeClient()
        worker = self._worker(client)

        result = worker.run_cycle(apply=True)
        repeated = worker.run_cycle(apply=True)

        self.assertEqual(result.fresh, 1)
        self.assertEqual(result.risk_accepted, 1)
        self.assertEqual(result.orders_submitted, 1)
        self.assertEqual(repeated.scanned, 0)
        self.assertEqual(
            len([call for call in client.calls if call[1] == "/v5/order/create"]),
            1,
        )

    def test_stale_signal_advances_cursor_without_risk_or_order(self):
        worker = self._worker()
        worker.run_cycle(apply=True)
        self._store(102, NOW - timedelta(hours=3, seconds=1), body=SHORT_SIGNAL)

        result = worker.run_cycle(apply=True)

        self.assertEqual(result.stale, 1)
        self.assertEqual(result.cursor, 102)
        self.assertIsNone(SQLiteRiskStateRepository(self.database).load_state())

    def test_dry_run_never_advances_cursor_or_submits(self):
        worker = self._worker()
        worker.run_cycle(apply=True)
        self._store(102, NOW - timedelta(minutes=1), body=SHORT_SIGNAL)
        client = FakeClient()

        result = self._worker(client).run_cycle(apply=False)

        self.assertEqual(result.fresh, 1)
        self.assertFalse(result.database_writes)
        self.assertEqual(
            SQLiteDemoBridgeRepository(self.database).get_worker_cursor(
                2180632014, 231508, 8003985182
            ),
            101,
        )
        self.assertEqual(client.calls, [])

    def test_default_dry_run_needs_no_demo_credentials_or_network(self):
        result = DemoSignalWorker(
            SQLiteDemoBridgeRepository(self.database),
            SQLiteRiskStateRepository(self.database),
            SQLiteDemoExecutionRepository(self.database),
            None,
            None,
            clock=self.clock,
        ).run_cycle(apply=False)

        self.assertEqual(result.scanned, 0)
        self.assertFalse(result.database_writes)

    def test_apply_rejects_non_demo_client_before_writes(self):
        client = FakeClient()
        client.config.base_url = "https://api.bybit.com"

        with self.assertRaises(DemoWorkerError) as raised:
            self._worker(client).run_cycle(apply=True)

        self.assertEqual(raised.exception.code, "demo_only_guard")
        self.assertIsNone(
            SQLiteDemoBridgeRepository(self.database).get_worker_cursor(
                2180632014, 231508, 8003985182
            )
        )

    def test_wrong_sender_never_enters_worker_stream(self):
        worker = self._worker()
        worker.run_cycle(apply=True)
        self._store(102, NOW - timedelta(minutes=1), sender_id=999, body=SHORT_SIGNAL)

        result = worker.run_cycle(apply=True)

        self.assertEqual(result.scanned, 0)
        self.assertEqual(result.cursor, 101)


if __name__ == "__main__":
    unittest.main()
