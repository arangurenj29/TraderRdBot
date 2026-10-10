from contextlib import closing
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from io import StringIO
from pathlib import Path
import json
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from traderrd.demo_risk_epoch import run_demo_risk_epoch, _GetOnlyDemoClient
from traderrd.demo_runtime import _demo_operation_lock
from traderrd.domain.bridge import DemoStrategyAccountSnapshot
from traderrd.domain.risk import (
    InitializeRiskEngine, ApplyEquitySnapshot, PortfolioRiskEngine,
    ProposeRiskReservation, TradeProposal, CancelPendingReservation, StartDemoRiskEpoch,
    ConfirmPendingFill, ConfirmPositionClosed,
)
from traderrd.domain.models import Direction
from traderrd.domain.execution import ExecutionIntent, ExecutionIntentKind, ExecutionIntentState
from traderrd.infrastructure.risk_repository import SQLiteRiskStateRepository
from traderrd.infrastructure.execution_repository import SQLiteDemoExecutionRepository


class DemoRiskEpochTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.database = Path(self.temp.name) / "demo.sqlite3"
        self.risk = SQLiteRiskStateRepository(self.database)
        self.risk.initialize()
        SQLiteDemoExecutionRepository(self.database).initialize()
        self.start = datetime.now(timezone.utc) - timedelta(minutes=2)
        self.risk.execute(InitializeRiskEngine("init", self.start, Decimal("1000")), PortfolioRiskEngine())
        self.risk.execute(ApplyEquitySnapshot("decline", self.start + timedelta(minutes=1), Decimal("860")), PortfolioRiskEngine())

    def snapshot(self, **changes):
        values = dict(equity=Decimal("860"), captured_at=datetime.now(timezone.utc),
                      positions=(), orders=(), wallet_balance=Decimal("860"),
                      unrealised_pnl=Decimal("0"), position_initial_margin=Decimal("0"),
                      order_initial_margin=Decimal("0"))
        values.update(changes)
        return DemoStrategyAccountSnapshot(**values)

    def apply(self, snapshot=None, epoch_id="epoch-1", operator="operator"):
        with (patch("traderrd.demo_risk_epoch.load_demo_client") as load,
              patch("traderrd.demo_risk_epoch.BybitDemoAccountSnapshotProvider") as provider,
              patch("sys.stdout", new_callable=StringIO)):
            load.return_value.config.base_url = "https://api-demo.bybit.com"
            provider.return_value.fetch.return_value = (snapshot or self.snapshot(), object())
            result = run_demo_risk_epoch(self.database, epoch_id, operator, True)
            return result, load, provider

    def test_epoch_preserves_history_and_retry_never_refreshes_input(self):
        original_counts = self.risk.counts()
        result, _, _ = self.apply()
        self.assertEqual(result, 0)
        state = self.risk.load_state()
        self.assertEqual(state.equity, Decimal("860"))
        self.assertEqual(state.high_watermark, Decimal("860"))
        self.assertEqual(state.risk_epoch_id, "demo-risk-epoch:epoch-1")
        self.assertEqual(self.risk.counts()[0], original_counts[0] + 1)
        result, load, provider = self.apply()
        self.assertEqual(result, 0)
        load.assert_not_called()
        provider.assert_not_called()
        self.assertEqual(self.risk.counts()[0], original_counts[0] + 1)
        with closing(sqlite3.connect(self.database)) as connection:
            detail = connection.execute("SELECT detail FROM risk_engine_events WHERE event_type='demo_risk_epoch_started'").fetchone()[0]
        boundary = json.loads(detail)
        self.assertEqual(boundary["old"]["high_watermark"], "1000")
        self.assertEqual(boundary["new"]["high_watermark"], "860")

    def test_bad_evidence_never_changes_state(self):
        bad = (
            self.snapshot(captured_at=datetime.now(timezone.utc)-timedelta(seconds=11)),
            self.snapshot(captured_at=datetime.now(timezone.utc)+timedelta(seconds=10)),
            self.snapshot(wallet_balance=None),
            self.snapshot(wallet_balance=Decimal("861")),
            self.snapshot(unrealised_pnl=Decimal("1")),
            self.snapshot(position_initial_margin=Decimal("1")),
            self.snapshot(order_initial_margin=None),
            self.snapshot(positions=(object(),)),
            self.snapshot(orders=(object(),)),
            self.snapshot(equity=Decimal("840"), wallet_balance=Decimal("840")),
        )
        counts = self.risk.counts()
        for snapshot in bad:
            with self.subTest(snapshot=snapshot):
                self.assertEqual(self.apply(snapshot)[0], 1)
                self.assertEqual(self.risk.counts(), counts)
                self.assertEqual(self.risk.load_state().high_watermark, Decimal("1000"))

    def test_killed_engine_cannot_be_reset(self):
        self.risk.execute(ApplyEquitySnapshot("kill", datetime.now(timezone.utc)-timedelta(seconds=1), Decimal("840")), PortfolioRiskEngine())
        self.assertEqual(self.apply()[0], 1)
        self.assertEqual(self.risk.load_state().mode.value, "killed")

    def test_active_operation_blocks_before_credentials(self):
        with _demo_operation_lock(self.database):
            result, load, provider = self.apply()
        self.assertEqual(result, 1)
        load.assert_not_called()
        provider.assert_not_called()

    def test_dry_run_is_local_and_read_only(self):
        before = self.risk.counts()
        with (patch("traderrd.demo_risk_epoch.load_demo_client") as load,
              patch("sys.stdout", new_callable=StringIO)):
            self.assertEqual(run_demo_risk_epoch(self.database, None, None, False), 0)
        self.assertEqual(self.risk.counts(), before)
        load.assert_not_called()

    def test_missing_execution_schema_fails_closed(self):
        with closing(sqlite3.connect(self.database)) as connection:
            connection.execute("DROP TABLE demo_execution_intents")
            connection.commit()
        self.assertEqual(self.apply()[0], 1)

    def test_get_only_adapter_refuses_mutation(self):
        from unittest.mock import Mock
        client = Mock()
        client.config.base_url = "https://api-demo.bybit.com"
        guarded = _GetOnlyDemoClient(client)
        with self.assertRaises(ValueError):
            guarded.request("POST", "/v5/order/create")
        client.request.assert_not_called()

    def test_retry_operator_conflict_is_not_a_new_epoch(self):
        self.assertEqual(self.apply()[0], 0)
        counts = self.risk.counts()
        self.assertEqual(self.apply(operator="someone-else")[0], 1)
        self.assertEqual(self.risk.counts(), counts)

    def reserve(self):
        self.assertEqual(self.apply(epoch_id="fixture-epoch")[0], 0)
        return self.risk.execute(ProposeRiskReservation(
            "propose", datetime.now(timezone.utc), Decimal("860"),
            TradeProposal("signal", "BTCUSDT", Direction.LONG,
                          Decimal("100"), Decimal("101"), Decimal("97")),
        ), PortfolioRiskEngine())

    def entry(self, reservation_id):
        return ExecutionIntent("entry", "trd-demo-entry", "propose", reservation_id,
                               ExecutionIntentKind.ENTRY, ExecutionIntentState.PLANNED,
                               "BTCUSDT", Direction.LONG, Decimal("1"), Decimal("100"),
                               Decimal("101"), Decimal("97"), None)

    def test_active_reservation_blocks_before_evidence(self):
        self.assertEqual(self.reserve().decision.status, "accepted")
        result, load, _ = self.apply()
        self.assertEqual(result, 1)
        load.assert_not_called()

    def test_unfinished_local_intents_and_risk_handoff_block(self):
        reserved = self.reserve()
        execution = SQLiteDemoExecutionRepository(self.database)
        execution.save_planned(self.entry(reserved.decision.reservation_id))
        self.risk.execute(CancelPendingReservation("cancel", datetime.now(timezone.utc),
                                                  reserved.decision.reservation_id), PortfolioRiskEngine())
        self.assertEqual(self.apply()[0], 1)
        execution.transition("entry", ExecutionIntentState.CANCELLED, "cancelled", "fixture")
        self.assertEqual(self.apply()[0], 1)
        execution.mark_risk_sync_completed("entry")
        self.assertEqual(self.apply()[0], 0)
        self.assertIn(reserved.decision.reservation_id, self.risk.load_state().reservations)

    def test_transaction_rechecks_intent_created_after_initial_local_check(self):
        def fetched(symbol):
            SQLiteDemoExecutionRepository(self.database).save_planned(self.entry("unknown"))
            return self.snapshot(), object()

        counts = self.risk.counts()
        with (patch("traderrd.demo_risk_epoch.load_demo_client") as load,
              patch("traderrd.demo_risk_epoch.BybitDemoAccountSnapshotProvider") as provider,
              patch("sys.stdout", new_callable=StringIO)):
            load.return_value.config.base_url = "https://api-demo.bybit.com"
            provider.return_value.fetch.side_effect = fetched
            self.assertEqual(run_demo_risk_epoch(self.database, "epoch-1", "operator", True), 1)
        self.assertEqual(self.risk.counts(), counts)

    def test_repository_rejects_epoch_through_normal_execute(self):
        now = datetime.now(timezone.utc)
        command = StartDemoRiskEpoch("epoch", now, Decimal("860"), "operator", "initial",
                                     now, Decimal("860"), Decimal("0"), "BTCUSDT")
        with self.assertRaisesRegex(ValueError, "dedicated operator"):
            self.risk.execute(command, PortfolioRiskEngine())

    def test_closed_trade_performance_survives_new_boundary(self):
        reserved = self.reserve()
        reservation_id = reserved.decision.reservation_id
        execution = SQLiteDemoExecutionRepository(self.database)
        entry = execution.save_planned(self.entry(reservation_id))
        self.risk.execute(ConfirmPendingFill("fill", datetime.now(timezone.utc), reservation_id), PortfolioRiskEngine())
        self.risk.execute(ConfirmPositionClosed("close", datetime.now(timezone.utc), reservation_id, Decimal("860")), PortfolioRiskEngine())
        execution.record_attributed_outcome(
            entry, exchange_order_id="owned-order", closed_at=datetime.now(timezone.utc),
            closed_pnl=Decimal("3.5"), open_fee=Decimal("0.1"), close_fee=Decimal("0.1"),
        )
        execution.transition(entry.intent_id, ExecutionIntentState.POSITION_CLOSED, "closed", "fixture")
        with closing(sqlite3.connect(self.database)) as connection:
            before = connection.execute("SELECT * FROM demo_performance_outcomes").fetchall()
        self.assertEqual(self.apply()[0], 0)
        with closing(sqlite3.connect(self.database)) as connection:
            self.assertEqual(connection.execute("SELECT * FROM demo_performance_outcomes").fetchall(), before)
        self.assertEqual(self.risk.load_state().reservations[reservation_id].status.value, "closed")

    def test_incomplete_provider_and_delayed_audit_fail_without_boundary(self):
        from traderrd.infrastructure.bybit_demo import DemoExecutionError
        counts = self.risk.counts()
        with (patch("traderrd.demo_risk_epoch.load_demo_client") as load,
              patch("traderrd.demo_risk_epoch.BybitDemoAccountSnapshotProvider") as provider,
              patch("sys.stdout", new_callable=StringIO) as output):
            load.return_value.config.base_url = "https://api-demo.bybit.com"
            provider.return_value.fetch.side_effect = DemoExecutionError("account_snapshot_incomplete", "secret-in-error")
            self.assertEqual(run_demo_risk_epoch(self.database, "epoch-1", "operator", True), 1)
            self.assertNotIn("secret-in-error", output.getvalue())
        self.assertEqual(self.risk.counts(), counts)
        with patch("traderrd.infrastructure.risk_repository.datetime", wraps=datetime) as delayed:
            delayed.now.return_value = datetime.now(timezone.utc) + timedelta(seconds=20)
            self.assertEqual(self.apply()[0], 1)
        self.assertEqual(self.risk.counts(), counts)

    def test_wrong_endpoint_cannot_supply_epoch_evidence(self):
        from unittest.mock import Mock
        client = Mock()
        client.config.base_url = "https://api.bybit.com"
        with self.assertRaisesRegex(ValueError, "Demo Trading"):
            _GetOnlyDemoClient(client)
        client.request.assert_not_called()

    def test_legacy_state_and_command_codec_preserve_epoch_identity(self):
        from traderrd.application.risk_engine import (
            command_from_dict, command_to_dict, state_from_dict, state_to_dict,
        )
        legacy = state_to_dict(self.risk.load_state())
        legacy.pop("risk_epoch_id")
        self.assertEqual(state_from_dict(legacy).risk_epoch_id, "initial")
        self.assertEqual(self.apply()[0], 0)
        command = self.risk.get_command("demo-risk-epoch:epoch-1")
        self.assertEqual(command_from_dict(command_to_dict(command)), command)

    def test_staged_reversal_fails_without_advancing_domain_time(self):
        from traderrd.domain.risk import ReversalIntent
        state = self.risk.load_state()
        state.reversal_intents["BTCUSDT"] = ReversalIntent(
            "closing", TradeProposal("inverse", "BTCUSDT", Direction.SHORT,
                                     Decimal("100"), Decimal("97"), Decimal("103")),
        )
        before = state.as_of
        now = datetime.now(timezone.utc)
        command = StartDemoRiskEpoch("epoch", now, Decimal("860"), "operator", "initial",
                                     now, Decimal("860"), Decimal("0"), "BTCUSDT")
        with self.assertRaisesRegex(ValueError, "reversals"):
            PortfolioRiskEngine().process(state, command)
        self.assertEqual(state.as_of, before)

    def test_dry_run_missing_database_creates_no_schema(self):
        missing = Path(self.temp.name) / "missing.sqlite3"
        with patch("sys.stdout", new_callable=StringIO):
            self.assertEqual(run_demo_risk_epoch(missing, None, None, False), 1)
        self.assertFalse(missing.exists())
