from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
import sqlite3
import json
from unittest.mock import patch

from traderrd.application.risk_engine import RiskEngineService, decision_to_dict
from traderrd.application.demo_execution import (
    DemoExecutionPlanner,
    DemoExecutionService,
)
from traderrd.application.demo_monitor import DemoLifecycleMonitor, DemoMonitorError
from traderrd.domain.bridge import DemoAccountOrder, DemoStrategyAccountSnapshot
from traderrd.domain.execution import (
    ExecutionIntent,
    ExecutionIntentKind,
    ExecutionIntentState,
    InstrumentRules,
)
from traderrd.domain.models import Direction
from traderrd.domain.risk import (
    InitializeRiskEngine,
    RiskDecision,
    RiskAction,
    RiskActionType,
    PortfolioRiskEngine,
    ApplyEquitySnapshot,
    CancelPendingReservation,
    ProposeRiskReservation,
    TradeProposal,
)
from traderrd.infrastructure.bybit_demo import DemoExecutionError
from traderrd.infrastructure.execution_repository import (
    SQLiteDemoExecutionRepository,
)
from traderrd.infrastructure.risk_repository import SQLiteRiskStateRepository


UTC = timezone.utc
START = datetime(2026, 8, 17, 14, 0, tzinfo=UTC)
RULES = InstrumentRules(
    symbol="BTCUSDT",
    tick_size=Decimal("0.1"),
    quantity_step=Decimal("0.001"),
    min_quantity=Decimal("0.001"),
    max_quantity=Decimal("100"),
    min_notional=Decimal("5"),
)


class FakeExecutionClient:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, dict[str, object]]] = []
        self.order_link_id = ""
        self.position_calls = 0
        self.order_status = "Filled"
        self.flat_position = False
        self.position_side = "Buy"
        self.position_rows: list[dict[str, object]] | None = None
        self.realtime_empty = False
        self.closed_pnl_rows: list[dict[str, object]] | None = None

    def request(
        self,
        method: str,
        path: str,
        params: dict[str, str] | None = None,
        body: dict[str, object] | None = None,
        private: bool = True,
    ) -> dict[str, object]:
        values: dict[str, object] = body or params or {}
        self.calls.append((method, path, values))
        if method == "POST" and path == "/v5/order/create":
            self.order_link_id = str(values["orderLinkId"])
            return {"retCode": 0, "result": {"orderId": "exchange-order-1"}}
        if method == "POST" and path in {
            "/v5/order/cancel",
            "/v5/position/trading-stop",
        }:
            return {"retCode": 0, "result": {}}
        if path in {"/v5/order/realtime", "/v5/order/history"}:
            link = str(values["orderLinkId"])
            if path == "/v5/order/realtime" and self.realtime_empty:
                return {"retCode": 0, "result": {"list": []}}
            return {
                "retCode": 0,
                "result": {
                    "list": [
                        {"orderId": "exchange-order-1", "symbol": str(values["symbol"]), "orderLinkId": link, "orderStatus": self.order_status, "cumExecQty": "4.838" if self.order_status == "Filled" else "0"}
                    ]
                },
            }
        if path == "/v5/execution/list":
            return {
                "retCode": 0,
                "result": {"list": [{"execId": "execution-1", "execQty": "4.838", "orderId": "exchange-order-1", "orderLinkId": str(values["orderLinkId"]), "symbol": str(values["symbol"]), "side": "Sell" if str(values["orderLinkId"]).startswith("trd-demo-c-") else "Buy"}]},
            }
        if path == "/v5/position/closed-pnl":
            if self.closed_pnl_rows is not None:
                return {"retCode": 0, "result": {"list": self.closed_pnl_rows}}
            start = int(str(values["startTime"]))
            end = int(str(values["endTime"]))
            return {
                "retCode": 0,
                "result": {"list": [{
                    "symbol": str(values["symbol"]),
                    "orderId": "closed-order-1",
                    "side": "Sell",
                    "execType": "Trade",
                    "closedSize": "4.838",
                    "closedPnl": "12.5",
                    "openFee": "0.2",
                    "closeFee": "0.3",
                    "createdTime": str(start),
                    "updatedTime": str(end),
                }]},
            }
        if path == "/v5/position/list":
            self.position_calls += 1
            if self.position_rows is not None:
                return {"retCode": 0, "result": {"list": self.position_rows}}
            row: dict[str, object] = {
                "positionIdx": 0,
                "size": "0" if self.flat_position else "4.838",
                "side": self.position_side,
            }
            if self.position_calls > 1:
                row.update({"takeProfit": "100.8", "stopLoss": "97"})
            return {"retCode": 0, "result": {"list": [row]}}
        raise AssertionError((method, path))


class FakeSnapshotProvider:
    def __init__(self, snapshot: DemoStrategyAccountSnapshot) -> None:
        self.snapshot = snapshot

    def fetch(self, symbol: str):
        return self.snapshot, RULES


def make_risk_database(path: Path) -> None:
    repository = SQLiteRiskStateRepository(path)
    repository.initialize()
    service = RiskEngineService(repository)
    service.execute(InitializeRiskEngine("init", START, Decimal("1000")))
    service.execute(
        ProposeRiskReservation(
            "risk-proposal",
            START + timedelta(minutes=1),
            Decimal("1000"),
            TradeProposal(
                "signal-1",
                "BTCUSDT",
                Direction.LONG,
                Decimal("100"),
                Decimal("100.8"),
                Decimal("97"),
            ),
        )
    )


class DemoExecutionLifecycleTests(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.database_path = Path(self.directory.name) / "demo.sqlite3"
        make_risk_database(self.database_path)
        self.execution_repository = SQLiteDemoExecutionRepository(
            self.database_path
        )
        self.execution_repository.initialize()
        self.client = FakeExecutionClient()
        self.service = DemoExecutionService(
            self.client,  # type: ignore[arg-type]
            self.execution_repository,
            RULES,
            clock=lambda: START + timedelta(minutes=2),
        )
        self.entry = DemoExecutionPlanner(self.database_path).preview(
            RULES, "risk-proposal"
        )[0]

    def test_late_fill_retains_risk_after_expiry(self) -> None:
        self.service.submit(self.entry)
        snapshot = DemoStrategyAccountSnapshot(
            equity=Decimal("1000"), captured_at=START + timedelta(hours=4),
            positions=(), orders=(),
        )
        monitor = DemoLifecycleMonitor(
            self.execution_repository, SQLiteRiskStateRepository(self.database_path),
            self.client, FakeSnapshotProvider(snapshot), clock=lambda: snapshot.captured_at,
        )
        monitor.run_cycle()
        state = SQLiteRiskStateRepository(self.database_path).load_state()
        self.assertEqual(state.reservations[self.entry.risk_reservation_id].status.value, "filled")
        self.assertGreater(state.total_reserved_risk, 0)
        with sqlite3.connect(self.database_path) as connection:
            persisted = connection.execute(
                "SELECT captured_at, equity FROM demo_account_snapshots WHERE singleton_id = 1"
            ).fetchone()
        self.assertEqual(persisted, (snapshot.captured_at.isoformat(), "1000"))

    def test_expired_unsent_entry_never_reaches_exchange(self) -> None:
        self.service._clock = lambda: START + timedelta(hours=4)
        stored = self.service.submit(self.entry)
        self.assertEqual(stored.state, ExecutionIntentState.CANCELLED)
        self.assertFalse(self.client.calls)

    def test_partial_fill_is_protected_and_cancelled_remainder_stays_monitorable(self) -> None:
        self.execution_repository.save_planned(self.entry)
        self.client.order_status = "PartiallyFilled"
        original = self.client.request
        def request(method, path, params=None, body=None, **kwargs):
            if path == "/v5/order/cancel":
                self.client.order_status = "Cancelled"
            payload = original(method, path, params, body, **kwargs)
            if path in {"/v5/order/realtime", "/v5/order/history"}:
                payload["result"]["list"][0]["cumExecQty"] = "2"
            if path == "/v5/execution/list":
                payload["result"]["list"][0]["execQty"] = "2"
            if path == "/v5/position/list":
                payload["result"]["list"][0]["size"] = "2"
            return payload
        self.client.request = request
        result = self.service.reconcile(self.entry.intent_id)
        self.assertEqual(result.state, ExecutionIntentState.PROTECTION_VERIFIED)
        self.assertEqual(result.filled_quantity, Decimal("2"))
        self.assertEqual(result.quantity, self.entry.quantity)
        self.assertIn(result, self.execution_repository.monitorable_intents())
        self.assertTrue(any(path == "/v5/order/cancel" for _, path, _ in self.client.calls))

    def test_removed_protection_is_not_reported_verified(self) -> None:
        self.execution_repository.save_planned(self.entry)
        self.service.reconcile(self.entry.intent_id)
        self.client.position_rows = [{
            "positionIdx": 0, "size": "4.838", "side": "Buy",
            "takeProfit": "0", "stopLoss": "0",
        }]
        result = self.service.reconcile(self.entry.intent_id)
        self.assertEqual(result.state, ExecutionIntentState.RECONCILIATION_REQUIRED)

    def test_close_recovers_after_risk_commit_before_execution_transition(self) -> None:
        self.execution_repository.save_planned(self.entry)
        snapshot = DemoStrategyAccountSnapshot(
            equity=Decimal("1000"), captured_at=START + timedelta(minutes=2),
            positions=(), orders=(),
        )
        self.execution_repository.transition(self.entry.intent_id, ExecutionIntentState.ACKNOWLEDGED, "test", "test")
        monitor = DemoLifecycleMonitor(
            self.execution_repository, SQLiteRiskStateRepository(self.database_path),
            self.client, FakeSnapshotProvider(snapshot), clock=lambda: snapshot.captured_at,
        )
        monitor.run_cycle()
        self.client.flat_position = True
        transition = self.execution_repository.transition
        def crash(intent_id, state, *args, **kwargs):
            if state is ExecutionIntentState.POSITION_CLOSED:
                raise RuntimeError("simulated crash after risk commit")
            return transition(intent_id, state, *args, **kwargs)
        with patch.object(self.execution_repository, "transition", side_effect=crash):
            with self.assertRaisesRegex(RuntimeError, "simulated crash"):
                monitor.run_cycle()
        before = SQLiteRiskStateRepository(self.database_path).counts()
        monitor._clock = lambda: START + timedelta(minutes=3)
        monitor._snapshots.snapshot = replace(snapshot, equity=Decimal("1010"))
        monitor.run_cycle()
        self.assertEqual(self.execution_repository.get(self.entry.intent_id).state, ExecutionIntentState.POSITION_CLOSED)
        self.assertEqual(SQLiteRiskStateRepository(self.database_path).counts(), before)

    def test_expired_ambiguous_submission_is_reconciled_without_resubmission(self) -> None:
        self.execution_repository.save_planned(self.entry)
        self.execution_repository.mark_submission_attempt(self.entry.intent_id)
        self.service._clock = lambda: START + timedelta(hours=4)
        result = self.service.submit(self.entry)
        self.assertEqual(result.state, ExecutionIntentState.PROTECTION_VERIFIED)
        self.assertFalse(any(path == "/v5/order/create" for _, path, _ in self.client.calls))

    def test_expired_ambiguous_missing_order_is_not_cancelled_or_reposted(self) -> None:
        self.execution_repository.save_planned(self.entry)
        self.execution_repository.mark_submission_attempt(self.entry.intent_id)
        self.service._clock = lambda: START + timedelta(hours=4)
        original = self.client.request
        def request(method, path, params=None, body=None, **kwargs):
            payload = original(method, path, params, body, **kwargs)
            if path in {"/v5/order/realtime", "/v5/order/history"}:
                payload["result"]["list"] = []
            return payload
        self.client.request = request
        with self.assertRaises(DemoExecutionError):
            self.service.submit(self.entry)
        self.assertEqual(self.execution_repository.get(self.entry.intent_id).state, ExecutionIntentState.RECONCILIATION_REQUIRED)
        self.assertFalse(any(method == "POST" for method, _, _ in self.client.calls))
        self.assertGreater(SQLiteRiskStateRepository(self.database_path).load_state().total_reserved_risk, 0)

    def test_cancelled_order_with_nonzero_position_does_not_release_risk(self) -> None:
        self.service.submit(self.entry)
        self.client.order_status = "Cancelled"
        result = self.service.reconcile(self.entry.intent_id)
        self.assertEqual(result.state, ExecutionIntentState.RECONCILIATION_REQUIRED)
        self.assertGreater(SQLiteRiskStateRepository(self.database_path).load_state().total_reserved_risk, 0)

    def test_partial_fill_cancel_race_protects_final_increased_quantity(self) -> None:
        self.execution_repository.save_planned(self.entry)
        self.client.order_status = "PartiallyFilled"
        quantity = "2"
        original = self.client.request
        def request(method, path, params=None, body=None, **kwargs):
            nonlocal quantity
            if path == "/v5/order/cancel":
                quantity = "3"
                self.client.order_status = "Cancelled"
            payload = original(method, path, params, body, **kwargs)
            if path in {"/v5/order/realtime", "/v5/order/history"}:
                payload["result"]["list"][0]["cumExecQty"] = quantity
            if path == "/v5/execution/list":
                payload["result"]["list"][0]["execQty"] = quantity
            if path == "/v5/position/list":
                payload["result"]["list"][0]["size"] = quantity
            return payload
        self.client.request = request
        result = self.service.reconcile(self.entry.intent_id)
        self.assertEqual(result.filled_quantity, Decimal("3"))
        self.assertEqual(result.state, ExecutionIntentState.PROTECTION_VERIFIED)
        self.assertEqual(len([1 for _, path, _ in self.client.calls if path == "/v5/position/trading-stop"]), 2)
        reloaded = SQLiteDemoExecutionRepository(self.database_path).get(self.entry.intent_id)
        self.assertEqual(reloaded.filled_quantity, Decimal("3"))
        self.assertEqual(self.execution_repository.save_planned(self.entry), reloaded)

    def test_protected_quantity_and_nonzero_tp_sl_mismatches_fail_closed(self) -> None:
        self.execution_repository.save_planned(self.entry)
        self.service.reconcile(self.entry.intent_id)
        for changes in ({"size": "1"}, {"takeProfit": "101"}, {"stopLoss": "98"}):
            with self.subTest(changes=changes):
                self.execution_repository.transition(self.entry.intent_id, ExecutionIntentState.PROTECTION_VERIFIED, "test", "test")
                self.client.position_rows = [{
                    "positionIdx": 0, "size": "4.838", "side": "Buy",
                    "takeProfit": "100.8", "stopLoss": "97", **changes,
                }]
                result = self.service.reconcile(self.entry.intent_id)
                self.assertEqual(result.state, ExecutionIntentState.RECONCILIATION_REQUIRED)

    def test_terminal_cancel_risk_handoff_survives_restart(self) -> None:
        self.service._clock = lambda: START + timedelta(hours=4)
        self.service.submit(self.entry)
        snapshot = DemoStrategyAccountSnapshot(
            equity=Decimal("1000"), captured_at=START + timedelta(hours=4),
            positions=(), orders=(),
        )
        monitor = DemoLifecycleMonitor(
            self.execution_repository, SQLiteRiskStateRepository(self.database_path),
            self.client, FakeSnapshotProvider(snapshot), clock=lambda: snapshot.captured_at,
        )
        with patch.object(self.execution_repository, "mark_risk_sync_completed", side_effect=RuntimeError("simulated crash")):
            with self.assertRaisesRegex(RuntimeError, "simulated crash"):
                monitor.run_cycle()
        before = SQLiteRiskStateRepository(self.database_path).counts()
        monitor._clock = lambda: START + timedelta(hours=5)
        monitor.run_cycle()
        self.assertEqual(self.execution_repository.monitorable_intents(), [])
        self.assertEqual(SQLiteRiskStateRepository(self.database_path).counts(), before)

    def test_rejected_risk_fill_is_visible_and_keeps_reservation(self) -> None:
        self.service.submit(self.entry)
        snapshot = DemoStrategyAccountSnapshot(
            equity=Decimal("1000"), captured_at=START + timedelta(minutes=2),
            positions=(), orders=(),
        )
        monitor = DemoLifecycleMonitor(
            self.execution_repository, SQLiteRiskStateRepository(self.database_path),
            self.client, FakeSnapshotProvider(snapshot), clock=lambda: snapshot.captured_at,
        )
        with patch.object(monitor._risk_engine, "_confirm_fill", return_value=(RiskDecision("rejected", "test_guard"), [])):
            with self.assertRaises(DemoMonitorError) as error:
                monitor.run_cycle()
        self.assertEqual(error.exception.code, "risk_sync_rejected")
        self.assertGreater(SQLiteRiskStateRepository(self.database_path).load_state().total_reserved_risk, 0)

    def _stage_inverse(self):
        self.service.submit(self.entry)
        return SQLiteRiskStateRepository(self.database_path).execute(
            ProposeRiskReservation("inverse", START + timedelta(minutes=2), Decimal("1000"),
                TradeProposal("inverse-signal", "BTCUSDT", Direction.SHORT,
                    Decimal("100"), Decimal("99"), Decimal("103"))),
            PortfolioRiskEngine(),
        )

    def _monitor_at(self, minute=3):
        snapshot = DemoStrategyAccountSnapshot(
            equity=Decimal("1000"), captured_at=START + timedelta(minutes=minute),
            positions=(), orders=(),
        )
        return DemoLifecycleMonitor(
            self.execution_repository, SQLiteRiskStateRepository(self.database_path),
            self.client, FakeSnapshotProvider(snapshot), clock=lambda: snapshot.captured_at,
        )

    def test_inverse_fill_close_plan_recovers_after_risk_commit(self) -> None:
        self._stage_inverse()
        monitor = self._monitor_at()
        with patch.object(self.execution_repository, "save_planned", side_effect=RuntimeError("plan interrupted")):
            with self.assertRaisesRegex(RuntimeError, "plan interrupted"):
                monitor.run_cycle()
        monitor = self._monitor_at(4)
        monitor.run_cycle()
        monitor.run_cycle()
        plans = self.execution_repository.planned_intents()
        self.assertEqual(len(plans), 1)
        self.assertEqual(plans[0].kind, ExecutionIntentKind.CLOSE_POSITION)
        state = SQLiteRiskStateRepository(self.database_path).load_state()
        self.assertNotIn("inverse-signal", state.reservations)
        self.assertGreater(state.total_reserved_risk, 0)

    def test_inverse_cancel_admits_exactly_one_opposite_after_flat(self) -> None:
        self._stage_inverse()
        self.client.order_status = "Cancelled"
        self.client.flat_position = True
        monitor = self._monitor_at()
        monitor.run_cycle()
        monitor.run_cycle()
        plans = self.execution_repository.planned_intents()
        self.assertEqual(len(plans), 1)
        self.assertEqual(plans[0].direction, Direction.SHORT)
        state = SQLiteRiskStateRepository(self.database_path).load_state()
        self.assertEqual(state.reservations[self.entry.risk_reservation_id].status.value, "cancelled")
        self.assertEqual(state.reservations["inverse-signal"].status.value, "pending")

    def test_unsent_plan_cannot_submit_after_pause_or_kill(self) -> None:
        for equity in ("940", "850"):
            with self.subTest(equity=equity):
                risk = SQLiteRiskStateRepository(self.database_path)
                risk.execute(ApplyEquitySnapshot("halt-" + equity, START + timedelta(minutes=2), Decimal(equity)), PortfolioRiskEngine())
                result = self.service.submit(self.entry)
                self.assertEqual(result.state, ExecutionIntentState.CANCELLED)
                self.assertFalse(self.client.calls)
                self.assertGreater(risk.load_state().total_reserved_risk, 0)

    def test_historical_inactive_reservation_refuses_unsent_plan(self) -> None:
        risk = SQLiteRiskStateRepository(self.database_path)
        risk.execute(CancelPendingReservation("historical", START + timedelta(minutes=2), self.entry.risk_reservation_id), PortfolioRiskEngine())
        with self.assertRaises(DemoExecutionError) as error:
            self.service.submit(self.entry)
        self.assertEqual(error.exception.code, "risk_submission_guard")
        self.assertFalse(self.client.calls)

    def test_reduce_only_close_cannot_leave_live_entry_remainder(self) -> None:
        self.execution_repository.save_planned(self.entry)
        self.execution_repository.transition(self.entry.intent_id, ExecutionIntentState.FILLED, "test", "test")
        self.client.order_status = "PartiallyFilled"
        close = replace(self.entry, intent_id="close-partial", order_link_id="trd-demo-c-partial",
                        kind=ExecutionIntentKind.CLOSE_POSITION, price=None)
        with self.assertRaises(DemoExecutionError) as error:
            self.service.submit(close)
        self.assertEqual(error.exception.code, "close_guard")
        self.assertFalse(any(path == "/v5/order/create" for _, path, _ in self.client.calls))

    def _historical_cancel_confirmation(self):
        self.service.submit(self.entry)
        self.execution_repository.transition(self.entry.intent_id, ExecutionIntentState.CANCELLED,
                                             "order_cancelled", "Cancelled")
        risk = SQLiteRiskStateRepository(self.database_path)
        command_id = f"demo-monitor-cancel:{self.entry.intent_id}"
        result = risk.execute(CancelPendingReservation(command_id, START + timedelta(minutes=2),
                              self.entry.risk_reservation_id, "demo_entry_cancelled"), PortfolioRiskEngine())
        old_decision = replace(result.decision, actions=(RiskAction(
            RiskActionType.CANCEL_PENDING, self.entry.risk_reservation_id,
            self.entry.symbol, self.entry.direction, self.entry.quantity,
            reason="demo_entry_cancelled"),))
        # A previous release serialized this already-completed cancellation as
        # an action. Model that persisted historical row, not a new command.
        with sqlite3.connect(self.database_path) as connection:
            connection.execute("UPDATE risk_engine_commands SET decision_json=? WHERE command_id=?",
                               (json.dumps(decision_to_dict(old_decision)), command_id))
        self.client.flat_position = True
        self.client.order_status = "Cancelled"
        return command_id, risk.get_command_result(command_id)

    def test_historical_cancel_result_does_not_materialize_another_cancel(self) -> None:
        command_id, result = self._historical_cancel_confirmation()
        before = SQLiteRiskStateRepository(self.database_path).counts()
        self._monitor_at().run_cycle()
        self.assertEqual(self.execution_repository.planned_intents(), [])
        self.assertEqual(SQLiteRiskStateRepository(self.database_path).counts(), before)

    def test_sixteen_persisted_redundant_cancels_retire_without_post_or_risk_replay(self) -> None:
        from traderrd.application.demo_worker import DemoSignalWorker
        command_id, result = self._historical_cancel_confirmation()
        template = DemoExecutionPlanner.plan_decision(RULES, command_id, result.decision, result.state)[0]
        worker = object.__new__(DemoSignalWorker)
        worker._client = self.client
        worker._execution = self.execution_repository
        worker._clock = lambda: START + timedelta(minutes=3)
        plans = [replace(template, intent_id=f"historical-cancel-{index}") for index in range(16)]
        for plan in plans:
            self.execution_repository.save_planned(plan)
        before = SQLiteRiskStateRepository(self.database_path).counts()
        self.client.calls.clear()
        for plan in plans:
            self.assertEqual(worker._submit_intent(plan, RULES), (0, 0))
            self.assertEqual(self.execution_repository.get(plan.intent_id).state, ExecutionIntentState.CANCELLED)
        self.assertFalse(any(method == "POST" for method, _, _ in self.client.calls))
        self.assertEqual(self.execution_repository.planned_intents(), [])
        self.assertFalse(any(item.kind is ExecutionIntentKind.CANCEL_ENTRY for item in self.execution_repository.monitorable_intents()))
        self.assertEqual(SQLiteRiskStateRepository(self.database_path).counts(), before)

    def test_closed_parent_suppresses_materialization_and_retires_twelve_persisted_cancels(self) -> None:
        self.test_operator_close_recovers_pagination_without_stale_protection_and_is_idempotent()
        risk = SQLiteRiskStateRepository(self.database_path)
        command_id = f"demo-operator-close:{self.entry.intent_id}"
        result = risk.get_command_result(command_id)
        result = replace(result, decision=replace(result.decision, actions=(RiskAction(
            RiskActionType.CANCEL_PENDING, self.entry.risk_reservation_id,
            self.entry.symbol, self.entry.direction, self.entry.quantity, reason="historical"),)))
        self._monitor_at()._materialize_follow_up_actions(command_id, result, RULES)
        self.assertEqual(self.execution_repository.planned_intents(), [])
        template = DemoExecutionPlanner.plan_decision(RULES, command_id, result.decision, result.state)[0]
        before = risk.counts()
        self.client.realtime_empty = True
        self.client.calls.clear()
        for index in range(12):
            plan = replace(template, intent_id=f"post-close-cancel-{index}")
            self.execution_repository.save_planned(plan)
            self.assertEqual(self.service.submit(plan).state, ExecutionIntentState.CANCELLED)
            restarted = DemoExecutionService(self.client, self.execution_repository, RULES)
            self.assertEqual(restarted.reconcile(plan.intent_id).state, ExecutionIntentState.CANCELLED)
        self._monitor_at().run_cycle()
        self.assertEqual(risk.counts(), before)
        self.assertEqual(self.execution_repository.planned_intents(), [])
        self.assertFalse(any(method == "POST" for method, _, _ in self.client.calls))
        self.assertEqual(self.execution_repository.get(self.entry.intent_id).state, ExecutionIntentState.POSITION_CLOSED)

    def test_closed_parent_cancel_consumes_terminal_order_pages_and_rejects_ambiguity(self) -> None:
        self.test_operator_close_recovers_pagination_without_stale_protection_and_is_idempotent()
        risk = SQLiteRiskStateRepository(self.database_path)
        command_id = f"demo-operator-close:{self.entry.intent_id}"
        result = risk.get_command_result(command_id)
        decision = replace(result.decision, actions=(RiskAction(
            RiskActionType.CANCEL_PENDING, self.entry.risk_reservation_id,
            self.entry.symbol, self.entry.direction, self.entry.quantity, reason="historical"),))
        template = DemoExecutionPlanner.plan_decision(RULES, command_id, decision, result.state)[0]
        original = self.client.request
        row = {"orderLinkId": self.entry.order_link_id, "orderId": "exchange-order-1",
               "symbol": "BTCUSDT", "orderStatus": "Filled", "cumExecQty": "4.838"}
        pages = []
        def request(method, path, params=None, body=None, **kwargs):
            if path == "/v5/order/realtime" and params.get("openOnly") == "0":
                return {"result": pages.pop(0)}
            return original(method, path, params, body, **kwargs)
        self.client.request = request
        before = risk.counts()
        for index, evidence in enumerate((
            [{"list": [row], "nextPageCursor": "next"}, {"list": []}],
            [{"list": [row], "nextPageCursor": "cycle"}, {"list": [], "nextPageCursor": "cycle"}],
            [{"list": [], "nextPageCursor": str(n)} for n in range(10)],
            [{"list": [row], "nextPageCursor": "next"}, {"list": [row]}],
            [{"list": [row], "nextPageCursor": "next"}, {"list": [{**row, "orderLinkId": "external"}]}],
        )):
            pages[:] = evidence
            plan = replace(template, intent_id=f"paged-retire-{index}")
            self.execution_repository.save_planned(plan)
            self.client.calls.clear()
            with self.subTest(index=index):
                if index == 0:
                    self.assertEqual(self.service.submit(plan).state, ExecutionIntentState.CANCELLED)
                    self.assertEqual(self.service.reconcile(plan.intent_id).state, ExecutionIntentState.CANCELLED)
                else:
                    with self.assertRaises(DemoExecutionError):
                        self.service.submit(plan)
                    self.assertNotEqual(self.execution_repository.get(plan.intent_id).state, ExecutionIntentState.CANCELLED)
                self.assertFalse(any(method == "POST" for method, _, _ in self.client.calls))
                self.assertEqual(risk.counts(), before)

    def test_closed_parent_cancel_refuses_reopened_exposure_and_incomplete_accounting(self) -> None:
        self.test_operator_close_recovers_pagination_without_stale_protection_and_is_idempotent()
        risk = SQLiteRiskStateRepository(self.database_path)
        command_id = f"demo-operator-close:{self.entry.intent_id}"
        result = risk.get_command_result(command_id)
        decision = replace(result.decision, actions=(RiskAction(
            RiskActionType.CANCEL_PENDING, self.entry.risk_reservation_id,
            self.entry.symbol, self.entry.direction, self.entry.quantity, reason="historical"),))
        plan = DemoExecutionPlanner.plan_decision(RULES, command_id, decision, result.state)[0]
        self.client.calls.clear()
        self.client.flat_position = False
        with self.assertRaises(DemoExecutionError):
            self.service.submit(plan)
        self.client.flat_position = True
        with patch.object(self.execution_repository, "outcome_is_attributed", return_value=False):
            with self.assertRaises(DemoExecutionError):
                self.service.reconcile(plan.intent_id)
        self.assertFalse(any(method == "POST" for method, _, _ in self.client.calls))
        self.assertEqual(self.execution_repository.get(plan.intent_id).state, ExecutionIntentState.RECONCILIATION_REQUIRED)

    def test_historical_cancel_with_current_exposure_does_not_retire(self) -> None:
        command_id, result = self._historical_cancel_confirmation()
        plan = DemoExecutionPlanner.plan_decision(RULES, command_id, result.decision, result.state)[0]
        self.client.flat_position = False
        self.client.calls.clear()
        with self.assertRaises(DemoExecutionError) as raised:
            self.service.submit(plan)
        self.assertEqual(raised.exception.code, "cancel_reconciliation")
        self.assertEqual(self.execution_repository.get(plan.intent_id).state, ExecutionIntentState.RECONCILIATION_REQUIRED)
        self.assertFalse(any(method == "POST" for method, _, _ in self.client.calls))
        self.assertEqual(self.execution_repository.get(self.entry.intent_id).state, ExecutionIntentState.CANCELLED)

    def test_historical_cancel_without_exchange_history_retires_from_current_flat_open_check(self) -> None:
        command_id, result = self._historical_cancel_confirmation()
        plan = DemoExecutionPlanner.plan_decision(RULES, command_id, result.decision, result.state)[0]
        self.client.realtime_empty = True
        self.client.calls.clear()
        self.assertEqual(self.service.submit(plan).state, ExecutionIntentState.CANCELLED)
        self.assertFalse(any(method == "POST" or path == "/v5/order/history" for method, path, _ in self.client.calls))

    def test_historical_cancel_with_live_order_fails_closed_without_post(self) -> None:
        command_id, result = self._historical_cancel_confirmation()
        plan = DemoExecutionPlanner.plan_decision(RULES, command_id, result.decision, result.state)[0]
        self.client.order_status = "New"
        self.client.calls.clear()
        with self.assertRaises(DemoExecutionError) as raised:
            self.service.submit(plan)
        self.assertEqual(raised.exception.code, "cancel_reconciliation")
        self.assertFalse(any(method == "POST" for method, _, _ in self.client.calls))

    def test_redundant_cancel_retirement_crash_does_not_replay_risk_command(self) -> None:
        command_id, result = self._historical_cancel_confirmation()
        plan = DemoExecutionPlanner.plan_decision(RULES, command_id, result.decision, result.state)[0]
        before = SQLiteRiskStateRepository(self.database_path).counts()
        with patch.object(self.execution_repository, "mark_risk_sync_completed", side_effect=RuntimeError("handoff interrupted")):
            with self.assertRaisesRegex(RuntimeError, "handoff interrupted"):
                self.service.submit(plan)
        self._monitor_at().run_cycle()
        self._monitor_at().run_cycle()
        self.assertEqual(SQLiteRiskStateRepository(self.database_path).counts(), before)
        self.assertEqual(self.execution_repository.planned_intents(), [])
        self.assertEqual(self.execution_repository.monitorable_intents(), [])

    def _execution_pages(self, pages):
        self.service.submit(self.entry)
        original = self.client.request
        calls = []
        def request(method, path, params=None, body=None, **kwargs):
            if path == "/v5/execution/list":
                calls.append(params)
                page = pages[len(calls) - 1]
                if isinstance(page, Exception):
                    raise page
                return {"result": page}
            return original(method, path, params, body, **kwargs)
        self.client.request = request
        return calls

    def test_execution_cursor_with_terminal_empty_page_confirms_fill(self) -> None:
        calls = self._execution_pages([
            {"list": [{"execId": "one", "execQty": "4.838"}], "nextPageCursor": "next"},
            {"list": []},
        ])
        self.assertEqual(self.service.reconcile(self.entry.intent_id).state, ExecutionIntentState.PROTECTION_VERIFIED)
        self.assertEqual(calls[1]["cursor"], "next")

    def test_execution_multi_page_fill_sums_exact_quantity(self) -> None:
        self._execution_pages([
            {"list": [{"execId": "one", "execQty": "2"}], "nextPageCursor": "next"},
            {"list": [{"execId": "two", "execQty": "2.838"}]},
        ])
        self.assertEqual(self.service.reconcile(self.entry.intent_id).filled_quantity, Decimal("4.838"))

    def test_execution_repeated_cursor_fails_closed(self) -> None:
        self._execution_pages([
            {"list": [{"execId": "one", "execQty": "4.838"}], "nextPageCursor": "same"},
            {"list": [], "nextPageCursor": "same"},
        ])
        with self.assertRaises(DemoExecutionError):
            self.service.reconcile(self.entry.intent_id)
        self.assertIsNone(self.execution_repository.get(self.entry.intent_id).filled_quantity)

    def test_execution_page_budget_exhaustion_fails_closed(self) -> None:
        calls = self._execution_pages([{"list": [], "nextPageCursor": str(n)} for n in range(10)])
        with self.assertRaises(DemoExecutionError):
            self.service.reconcile(self.entry.intent_id)
        self.assertEqual(len(calls), 10)

    def test_execution_duplicate_across_pages_is_not_double_counted(self) -> None:
        self._execution_pages([
            {"list": [{"execId": "same", "execQty": "2"}], "nextPageCursor": "next"},
            {"list": [{"execId": "same", "execQty": "2.838"}]},
        ])
        self.assertEqual(self.service.reconcile(self.entry.intent_id).state, ExecutionIntentState.RECONCILIATION_REQUIRED)
        self.assertIsNone(self.execution_repository.get(self.entry.intent_id).filled_quantity)

    def test_execution_second_page_network_failure_does_not_persist_partial_evidence(self) -> None:
        self._execution_pages([
            {"list": [{"execId": "one", "execQty": "4.838"}], "nextPageCursor": "next"},
            DemoExecutionError("network", "unavailable"),
        ])
        with self.assertRaises(DemoExecutionError):
            self.service.reconcile(self.entry.intent_id)
        self.assertIsNone(self.execution_repository.get(self.entry.intent_id).filled_quantity)

    def test_operator_close_recovers_pagination_without_stale_protection_and_is_idempotent(self) -> None:
        self.service.submit(self.entry)
        self.execution_repository.transition(self.entry.intent_id, ExecutionIntentState.RECONCILIATION_REQUIRED,
                                             "incomplete_execution_evidence", "test")
        self.client.config = SimpleNamespace(base_url="https://api-demo.bybit.com")
        original = self.client.request
        def request(method, path, params=None, body=None, **kwargs):
            if path == "/v5/position/trading-stop":
                raise AssertionError("stale protection must not be installed before authorized close")
            if path == "/v5/execution/list" and params["orderLinkId"] == self.entry.order_link_id:
                return {"result": {"list": []}} if params.get("cursor") else {"result": {
                    "list": [{"execId": "entry-fill", "execQty": "4.838"}], "nextPageCursor": "terminal-empty"}}
            if path == "/v5/order/create" and body.get("reduceOnly"):
                self.client.flat_position = True
            return original(method, path, params, body, **kwargs)
        self.client.request = request
        monitor = self._monitor_at()
        closed = monitor.close_owned_position(self.entry.intent_id)
        before = SQLiteRiskStateRepository(self.database_path).counts()
        self.assertEqual(closed.state, ExecutionIntentState.CLOSE_CONFIRMED)
        self.assertEqual(self.execution_repository.get(self.entry.intent_id).filled_quantity, Decimal("4.838"))
        self.assertEqual(SQLiteRiskStateRepository(self.database_path).load_state().reservations[self.entry.risk_reservation_id].status.value, "closed")
        self.assertTrue(self.execution_repository.outcome_is_attributed(self.entry.risk_reservation_id))
        execution_before = self.execution_repository.counts()
        self.assertEqual(monitor.close_owned_position(self.entry.intent_id), closed)
        self.assertEqual(self.execution_repository.counts(), execution_before)
        self.assertEqual(SQLiteRiskStateRepository(self.database_path).counts(), before)
        closes = [body for method, path, body in self.client.calls if path == "/v5/order/create" and body.get("reduceOnly")]
        self.assertEqual(len(closes), 1)
        self.assertTrue(closes[0]["orderLinkId"].startswith("trd-demo-c-"))

    def test_operator_close_rejects_foreign_duplicate_partial_and_ambiguous_executions(self) -> None:
        self.service.submit(self.entry)
        self.client.config = SimpleNamespace(base_url="https://api-demo.bybit.com")
        original = self.client.request
        evidence = []
        def request(method, path, params=None, body=None, **kwargs):
            if path == "/v5/order/create" and body.get("reduceOnly"):
                self.client.flat_position = True
            if path == "/v5/execution/list" and params["orderLinkId"] != self.entry.order_link_id:
                return {"result": {"list": evidence}}
            return original(method, path, params, body, **kwargs)
        self.client.request = request
        monitor = self._monitor_at()
        for changes in (
            {"execId": "foreign", "execQty": "0", "orderLinkId": "external", "symbol": "ETHUSDT"},
            {"execQty": "1"}, {"side": "Buy"}, {"orderId": "foreign"},
            {"execId": ""}, {"orderLinkId": ""},
        ):
            close = self.execution_repository.find_close(self.entry.risk_reservation_id)
            link = close.order_link_id if close else "external"
            evidence[:] = [{"execId": "close-fill", "execQty": "4.838", "orderLinkId": link,
                            "symbol": "BTCUSDT", "side": "Sell", "orderId": "exchange-order-1", **changes}]
            if changes.get("execId") == "foreign":
                evidence.append(dict(evidence[0]))
            with self.subTest(changes=changes):
                result = monitor.close_owned_position(self.entry.intent_id)
                self.assertEqual(result.state, ExecutionIntentState.RECONCILIATION_REQUIRED)
                self.assertFalse(self.execution_repository.outcome_is_attributed(self.entry.risk_reservation_id))
                self.assertNotEqual(SQLiteRiskStateRepository(self.database_path).load_state().reservations[self.entry.risk_reservation_id].status.value, "closed")

    def test_operator_close_holds_shared_runtime_lock_through_operation(self) -> None:
        from traderrd.demo_runtime import _exclusive_runtime_lock, _LOCK_NAME, DemoRuntimeAlreadyRunningError
        from traderrd.demo_cli import run_demo_reconcile
        def operation(*args, **kwargs):
            with self.assertRaises(DemoRuntimeAlreadyRunningError):
                with _exclusive_runtime_lock(Path(self.database_path).parent / _LOCK_NAME):
                    pass
            return 0
        with patch("traderrd.demo_cli._run_demo_reconcile", side_effect=operation):
            self.assertEqual(run_demo_reconcile(self.database_path, self.entry.intent_id, True,
                                               close_owned_position=True), 0)
        with _exclusive_runtime_lock(Path(self.database_path).parent / _LOCK_NAME):
            pass

    def test_operator_close_refuses_active_supervisor_before_loading_credentials(self) -> None:
        self.service.submit(self.entry)
        from traderrd.demo_runtime import _exclusive_runtime_lock, _LOCK_NAME
        from traderrd.demo_cli import run_demo_reconcile
        with _exclusive_runtime_lock(Path(self.database_path).parent / _LOCK_NAME), \
             patch("traderrd.demo_cli.load_demo_client") as load:
            self.assertEqual(run_demo_reconcile(self.database_path, self.entry.intent_id, True,
                                               close_owned_position=True), 1)
            load.assert_not_called()

    def test_operator_close_requires_demo_endpoint_and_exact_position(self) -> None:
        self.service.submit(self.entry)
        self.client.config = SimpleNamespace(base_url="https://api.bybit.com")
        with self.assertRaises(DemoMonitorError):
            self._monitor_at().close_owned_position(self.entry.intent_id)
        self.client.config.base_url = "https://api-demo.bybit.com"
        self.client.position_side = "Sell"
        with self.assertRaises(DemoExecutionError):
            self._monitor_at().close_owned_position(self.entry.intent_id)
        self.assertFalse(any(body.get("reduceOnly") for _, _, body in self.client.calls))

    def test_operator_close_recovers_uncertain_post_once_and_preserves_single_close_record(self) -> None:
        self.service.submit(self.entry)
        self.client.config = SimpleNamespace(base_url="https://api-demo.bybit.com")
        original = self.client.request
        failed = False
        def request(method, path, params=None, body=None, **kwargs):
            nonlocal failed
            if path == "/v5/position/trading-stop":
                raise AssertionError("operator close cannot install protection")
            if path == "/v5/order/create" and body.get("reduceOnly"):
                self.client.flat_position = True
                if not failed:
                    failed = True
                    original(method, path, params, body, **kwargs)
                    raise DemoExecutionError("network", "POST outcome uncertain")
            return original(method, path, params, body, **kwargs)
        self.client.request = request
        monitor = self._monitor_at()
        with self.assertRaises(DemoExecutionError):
            monitor.close_owned_position(self.entry.intent_id)
        closed = monitor.close_owned_position(self.entry.intent_id)
        self.assertEqual(closed.state, ExecutionIntentState.CLOSE_CONFIRMED)
        self.assertEqual(self.execution_repository.get(self.entry.intent_id).state, ExecutionIntentState.POSITION_CLOSED)
        monitor.run_cycle()
        with sqlite3.connect(self.database_path) as connection:
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM risk_engine_commands WHERE command_type='confirm_close'").fetchone()[0], 1)
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM demo_performance_outcomes WHERE status='attributed'").fetchone()[0], 1)
        self.assertEqual(len([1 for _, path, body in self.client.calls if path == "/v5/order/create" and body.get("reduceOnly")]), 1)

    def test_operator_close_cli_requires_explicit_apply_and_matching_command(self) -> None:
        from traderrd.cli import main
        for args in (["demo-reconcile", "--close-owned-position", "--intent-id", "owned"],
                     ["status", "--close-owned-position", "--apply-demo-reconciliation"]):
            with self.subTest(args=args), patch("sys.argv", ["traderrd", *args]), self.assertRaises(SystemExit):
                main()
        with patch("sys.argv", ["traderrd", "demo-reconcile", "--intent-id", "owned",
                                "--apply-demo-reconciliation", "--close-owned-position"]), \
             patch("traderrd.demo_cli.run_demo_reconcile", return_value=0) as invoke:
            self.assertEqual(main(), 0)
            self.assertTrue(invoke.call_args.kwargs["close_owned_position"])

    def test_execution_foreign_evidence_on_later_page_is_rejected(self) -> None:
        self._execution_pages([
            {"list": [{"execId": "one", "execQty": "2"}], "nextPageCursor": "next"},
            {"list": [{"execId": "two", "execQty": "2.838", "orderLinkId": "external"}]},
        ])
        self.assertEqual(self.service.reconcile(self.entry.intent_id).state, ExecutionIntentState.RECONCILIATION_REQUIRED)
        self.assertIsNone(self.execution_repository.get(self.entry.intent_id).filled_quantity)

    def tearDown(self) -> None:
        self.directory.cleanup()

    def test_planner_creates_deterministic_post_only_entry(self) -> None:
        second = DemoExecutionPlanner(self.database_path).preview(
            RULES, "risk-proposal"
        )[0]

        self.assertEqual(self.entry, second)
        self.assertTrue(self.entry.order_link_id.startswith("trd-demo-e-"))
        self.assertEqual(self.entry.kind, ExecutionIntentKind.ENTRY)
        self.assertEqual(self.entry.price, Decimal("100"))
        self.assertEqual(self.entry.expires_at, START + timedelta(hours=3, minutes=1))

    def test_submission_acknowledgement_is_not_treated_as_fill(self) -> None:
        stored = self.service.submit(self.entry)

        self.assertEqual(stored.state, ExecutionIntentState.ACKNOWLEDGED)
        payload = self.client.calls[0][2]
        self.assertEqual(payload["orderType"], "Limit")
        self.assertEqual(payload["timeInForce"], "PostOnly")
        self.assertEqual(payload["positionIdx"], 0)
        self.assertNotIn("reduceOnly", payload)

        repeated = self.service.submit(self.entry)
        self.assertEqual(repeated.state, ExecutionIntentState.ACKNOWLEDGED)
        self.assertEqual(len(self.client.calls), 1)

    def test_fill_requires_execution_position_and_tp_sl_verification(self) -> None:
        acknowledged = self.service.submit(self.entry)

        reconciled = self.service.reconcile(acknowledged.intent_id)

        self.assertEqual(reconciled.state, ExecutionIntentState.PROTECTION_VERIFIED)
        paths = [path for _, path, _ in self.client.calls]
        self.assertIn("/v5/execution/list", paths)
        self.assertIn("/v5/position/trading-stop", paths)
        stop_payload = next(
            values
            for _, path, values in self.client.calls
            if path == "/v5/position/trading-stop"
        )
        self.assertEqual(stop_payload["takeProfit"], "100.8")
        self.assertEqual(stop_payload["stopLoss"], "97")

    def test_reconciliation_falls_back_to_owned_order_history(self) -> None:
        acknowledged = self.service.submit(self.entry)
        self.client.realtime_empty = True

        reconciled = self.service.reconcile(acknowledged.intent_id)

        self.assertEqual(reconciled.state, ExecutionIntentState.PROTECTION_VERIFIED)
        self.assertTrue(
            any(path == "/v5/order/history" for _, path, _ in self.client.calls)
        )

    def test_expiry_cancels_only_owned_link_without_market_fallback(self) -> None:
        acknowledged = self.service.submit(self.entry)
        self.client.order_status = "Cancelled"
        self.client.flat_position = True

        cancelled = self.service.cancel_expired(
            acknowledged.expires_at + timedelta(seconds=1)  # type: ignore[operator]
        )

        self.assertEqual(cancelled, 1)
        cancel_payload = next(
            values
            for method, path, values in self.client.calls
            if method == "POST" and path == "/v5/order/cancel"
        )
        self.assertEqual(cancel_payload["orderLinkId"], self.entry.order_link_id)
        self.assertFalse(
            any(
                values.get("orderType") == "Market"
                for _, _, values in self.client.calls
            )
        )

    def test_close_is_reduce_only_and_requires_owned_filled_entry(self) -> None:
        self.execution_repository.save_planned(self.entry)
        self.execution_repository.record_fill_quantity(self.entry.intent_id, self.entry.quantity)
        self.execution_repository.transition(
            self.entry.intent_id,
            ExecutionIntentState.FILLED,
            "fill_confirmed",
            "test",
            "entry-order",
        )
        close = ExecutionIntent(
            intent_id="close-intent",
            order_link_id="trd-demo-c-owned-position",
            risk_command_id="close-risk-command",
            risk_reservation_id=self.entry.risk_reservation_id,
            kind=ExecutionIntentKind.CLOSE_POSITION,
            state=ExecutionIntentState.PLANNED,
            symbol="BTCUSDT",
            direction=Direction.LONG,
            quantity=self.entry.quantity,
            price=None,
            take_profit=None,
            stop_loss=None,
            expires_at=None,
        )

        acknowledged = self.service.submit(close)

        self.assertEqual(acknowledged.state, ExecutionIntentState.ACKNOWLEDGED)
        payload = self.client.calls[-1][2]
        self.assertTrue(payload["reduceOnly"])
        self.assertEqual(payload["orderType"], "Market")
        self.assertEqual(payload["side"], "Sell")

        self.client.flat_position = True
        confirmed = self.service.reconcile(acknowledged.intent_id)
        self.assertEqual(confirmed.state, ExecutionIntentState.CLOSE_CONFIRMED)

    def test_namespace_guard_refuses_external_order(self) -> None:
        external = ExecutionIntent(
            intent_id="external",
            order_link_id="someone-else",
            risk_command_id="risk",
            risk_reservation_id="reservation",
            kind=ExecutionIntentKind.ENTRY,
            state=ExecutionIntentState.PLANNED,
            symbol="BTCUSDT",
            direction=Direction.LONG,
            quantity=Decimal("1"),
            price=Decimal("100"),
            take_profit=Decimal("101"),
            stop_loss=Decimal("97"),
            expires_at=START + timedelta(hours=3),
        )

        with self.assertRaises(DemoExecutionError) as raised:
            self.service.submit(external)
        self.assertEqual(raised.exception.code, "ownership_guard")
        self.assertEqual(self.client.calls, [])

    def test_monitor_reconciles_fill_and_updates_risk_state(self) -> None:
        acknowledged = self.service.submit(self.entry)
        snapshot = DemoStrategyAccountSnapshot(
            equity=Decimal("1000"),
            captured_at=START + timedelta(minutes=2),
            positions=(),
            orders=(DemoAccountOrder(acknowledged.order_link_id, "BTCUSDT"),),
        )
        monitor = DemoLifecycleMonitor(
            self.execution_repository,
            SQLiteRiskStateRepository(self.database_path),
            self.client,  # type: ignore[arg-type]
            FakeSnapshotProvider(snapshot),  # type: ignore[arg-type]
            clock=lambda: START + timedelta(minutes=2),
        )

        before = SQLiteRiskStateRepository(self.database_path).counts()
        result = monitor.run_cycle()
        after = SQLiteRiskStateRepository(self.database_path).counts()

        self.assertEqual(result.inspected, 1)
        self.assertEqual(result.protected, 1)
        self.assertEqual(result.risk_updates, 1)
        self.assertEqual(after[0], before[0] + 1)
        state = SQLiteRiskStateRepository(self.database_path).load_state()
        self.assertIsNotNone(state)
        self.assertEqual(
            state.reservations[self.entry.risk_reservation_id].status.value,
            "filled",
        )

    def test_monitor_closes_flat_protected_position_without_inferred_exit_reason(self) -> None:
        acknowledged = self.service.submit(self.entry)
        first_snapshot = DemoStrategyAccountSnapshot(
            equity=Decimal("1000"),
            captured_at=START + timedelta(minutes=2),
            positions=(),
            orders=(DemoAccountOrder(acknowledged.order_link_id, "BTCUSDT"),),
        )
        monitor = DemoLifecycleMonitor(
            self.execution_repository,
            SQLiteRiskStateRepository(self.database_path),
            self.client,  # type: ignore[arg-type]
            FakeSnapshotProvider(first_snapshot),  # type: ignore[arg-type]
            clock=lambda: START + timedelta(minutes=2),
        )
        monitor.run_cycle()

        self.client.flat_position = True
        second_snapshot = DemoStrategyAccountSnapshot(
            equity=Decimal("990"),
            captured_at=START + timedelta(minutes=3),
            positions=(),
            orders=(DemoAccountOrder(acknowledged.order_link_id, "BTCUSDT"),),
        )
        result = DemoLifecycleMonitor(
            self.execution_repository,
            SQLiteRiskStateRepository(self.database_path),
            self.client,  # type: ignore[arg-type]
            FakeSnapshotProvider(second_snapshot),  # type: ignore[arg-type]
            clock=lambda: START + timedelta(minutes=3),
        ).run_cycle()

        self.assertEqual(result.inspected, 1)
        self.assertEqual(result.protected, 0)
        self.assertEqual(result.risk_updates, 1)
        self.assertEqual(
            self.execution_repository.get(acknowledged.intent_id).state,
            ExecutionIntentState.POSITION_CLOSED,
        )
        risk = SQLiteRiskStateRepository(self.database_path).load_state()
        self.assertIsNotNone(risk)
        self.assertEqual(risk.reservations[self.entry.risk_reservation_id].status.value, "closed")
        self.assertEqual(risk.equity, Decimal("990"))
        with sqlite3.connect(self.database_path) as connection:
            events = connection.execute(
                "SELECT event_type, detail FROM demo_execution_events "
                "WHERE intent_id = ? ORDER BY id",
                (acknowledged.intent_id,),
            ).fetchall()
        self.assertIn(
            ("position_closed_detected", "owned_position_flat; exit_reason_unknown"),
            events,
        )
        self.assertIn(
            ("position_closed_risk_released", "risk_close_confirmed; exit_reason_unknown"),
            events,
        )
        with sqlite3.connect(self.database_path) as connection:
            outcome = connection.execute(
                "SELECT status, closed_pnl, open_fee, close_fee "
                "FROM demo_performance_outcomes WHERE risk_reservation_id = ?",
                (self.entry.risk_reservation_id,),
            ).fetchone()
        self.assertEqual(outcome, ("attributed", "12.5", "0.2", "0.3"))

    def test_flat_position_without_complete_closed_pnl_stays_unresolved_and_reserved(self) -> None:
        acknowledged = self.service.submit(self.entry)
        snapshot = DemoStrategyAccountSnapshot(
            equity=Decimal("1000"),
            captured_at=START + timedelta(minutes=2),
            positions=(),
            orders=(DemoAccountOrder(acknowledged.order_link_id, "BTCUSDT"),),
        )
        monitor = DemoLifecycleMonitor(
            self.execution_repository,
            SQLiteRiskStateRepository(self.database_path),
            self.client,  # type: ignore[arg-type]
            FakeSnapshotProvider(snapshot),  # type: ignore[arg-type]
            clock=lambda: START + timedelta(minutes=2),
        )
        monitor.run_cycle()
        self.client.flat_position = True
        self.client.closed_pnl_rows = [{
            "symbol": "BTCUSDT", "orderId": "close", "side": "Sell",
            "execType": "Trade", "closedSize": "4.838", "closedPnl": "1",
            # Explicitly incomplete: no fee fields or timing evidence.
        }]
        result = DemoLifecycleMonitor(
            self.execution_repository,
            SQLiteRiskStateRepository(self.database_path),
            self.client,  # type: ignore[arg-type]
            FakeSnapshotProvider(snapshot),  # type: ignore[arg-type]
            clock=lambda: START + timedelta(minutes=3),
        ).run_cycle()

        self.assertEqual(result.risk_updates, 0)
        self.assertEqual(
            self.execution_repository.get(acknowledged.intent_id).state,
            ExecutionIntentState.POSITION_CLOSED_PENDING,
        )
        risk = SQLiteRiskStateRepository(self.database_path).load_state()
        self.assertIsNotNone(risk)
        self.assertEqual(risk.reservations[self.entry.risk_reservation_id].status.value, "filled")
        with sqlite3.connect(self.database_path) as connection:
            outcome = connection.execute(
                "SELECT status, unresolved_reason FROM demo_performance_outcomes"
            ).fetchone()
        self.assertEqual(outcome, ("unresolved", "incomplete_closed_pnl"))

    def test_monitor_does_not_reconfirm_filled_protected_position_on_later_cycle(self) -> None:
        acknowledged = self.service.submit(self.entry)
        snapshot = DemoStrategyAccountSnapshot(
            equity=Decimal("1000"),
            captured_at=START + timedelta(minutes=2),
            positions=(),
            orders=(DemoAccountOrder(acknowledged.order_link_id, "BTCUSDT"),),
        )
        monitor = DemoLifecycleMonitor(
            self.execution_repository,
            SQLiteRiskStateRepository(self.database_path),
            self.client,  # type: ignore[arg-type]
            FakeSnapshotProvider(snapshot),  # type: ignore[arg-type]
            clock=lambda: START + timedelta(minutes=2),
        )
        monitor.run_cycle()
        command_counts = SQLiteRiskStateRepository(self.database_path).counts()

        result = DemoLifecycleMonitor(
            self.execution_repository,
            SQLiteRiskStateRepository(self.database_path),
            self.client,  # type: ignore[arg-type]
            FakeSnapshotProvider(snapshot),  # type: ignore[arg-type]
            clock=lambda: START + timedelta(minutes=3),
        ).run_cycle()

        self.assertEqual(result.protected, 1)
        self.assertEqual(
            self.execution_repository.get(acknowledged.intent_id).state,
            ExecutionIntentState.PROTECTION_VERIFIED,
        )
        risk = SQLiteRiskStateRepository(self.database_path).load_state()
        self.assertIsNotNone(risk)
        self.assertEqual(risk.reservations[self.entry.risk_reservation_id].status.value, "filled")
        self.assertEqual(SQLiteRiskStateRepository(self.database_path).counts(), command_counts)

    def test_flat_protected_close_is_restart_safe_and_idempotent(self) -> None:
        acknowledged = self.service.submit(self.entry)
        snapshot = DemoStrategyAccountSnapshot(
            equity=Decimal("1000"),
            captured_at=START + timedelta(minutes=2),
            positions=(),
            orders=(DemoAccountOrder(acknowledged.order_link_id, "BTCUSDT"),),
        )
        monitor = DemoLifecycleMonitor(
            self.execution_repository,
            SQLiteRiskStateRepository(self.database_path),
            self.client,  # type: ignore[arg-type]
            FakeSnapshotProvider(snapshot),  # type: ignore[arg-type]
            clock=lambda: START + timedelta(minutes=2),
        )
        monitor.run_cycle()
        self.execution_repository.transition(
            acknowledged.intent_id,
            ExecutionIntentState.POSITION_CLOSED_PENDING,
            "position_closed_detected",
            "owned_position_flat; exit_reason_unknown",
        )
        self.client.flat_position = True
        before = SQLiteRiskStateRepository(self.database_path).counts()

        restarted = DemoLifecycleMonitor(
            self.execution_repository,
            SQLiteRiskStateRepository(self.database_path),
            self.client,  # type: ignore[arg-type]
            FakeSnapshotProvider(snapshot),  # type: ignore[arg-type]
            clock=lambda: START + timedelta(minutes=3),
        )
        result = restarted.run_cycle()
        after_close = SQLiteRiskStateRepository(self.database_path).counts()
        repeated = restarted.run_cycle()

        self.assertEqual(result.risk_updates, 1)
        self.assertEqual(repeated.inspected, 0)
        self.assertEqual(
            self.execution_repository.get(acknowledged.intent_id).state,
            ExecutionIntentState.POSITION_CLOSED,
        )
        self.assertEqual(after_close[0], before[0] + 1)
        self.assertEqual(
            SQLiteRiskStateRepository(self.database_path).counts(), after_close
        )

    def test_protected_position_side_mismatch_fails_closed(self) -> None:
        acknowledged = self.service.submit(self.entry)
        snapshot = DemoStrategyAccountSnapshot(
            equity=Decimal("1000"),
            captured_at=START + timedelta(minutes=2),
            positions=(),
            orders=(DemoAccountOrder(acknowledged.order_link_id, "BTCUSDT"),),
        )
        monitor = DemoLifecycleMonitor(
            self.execution_repository,
            SQLiteRiskStateRepository(self.database_path),
            self.client,  # type: ignore[arg-type]
            FakeSnapshotProvider(snapshot),  # type: ignore[arg-type]
            clock=lambda: START + timedelta(minutes=2),
        )
        monitor.run_cycle()
        self.client.position_side = "Sell"

        result = monitor.run_cycle()

        self.assertEqual(result.risk_updates, 0)
        self.assertEqual(
            self.execution_repository.get(acknowledged.intent_id).state,
            ExecutionIntentState.RECONCILIATION_REQUIRED,
        )
        risk = SQLiteRiskStateRepository(self.database_path).load_state()
        self.assertIsNotNone(risk)
        self.assertEqual(risk.reservations[self.entry.risk_reservation_id].status.value, "filled")

    def test_protected_position_unknown_state_fails_closed(self) -> None:
        acknowledged = self.service.submit(self.entry)
        snapshot = DemoStrategyAccountSnapshot(
            equity=Decimal("1000"),
            captured_at=START + timedelta(minutes=2),
            positions=(),
            orders=(DemoAccountOrder(acknowledged.order_link_id, "BTCUSDT"),),
        )
        monitor = DemoLifecycleMonitor(
            self.execution_repository,
            SQLiteRiskStateRepository(self.database_path),
            self.client,  # type: ignore[arg-type]
            FakeSnapshotProvider(snapshot),  # type: ignore[arg-type]
            clock=lambda: START + timedelta(minutes=2),
        )
        monitor.run_cycle()
        command_counts = SQLiteRiskStateRepository(self.database_path).counts()
        self.client.position_rows = []

        with self.assertRaises(DemoExecutionError) as raised:
            monitor.run_cycle()

        self.assertEqual(raised.exception.code, "position_reconciliation")
        self.assertEqual(
            self.execution_repository.get(acknowledged.intent_id).state,
            ExecutionIntentState.PROTECTION_VERIFIED,
        )
        risk = SQLiteRiskStateRepository(self.database_path).load_state()
        self.assertIsNotNone(risk)
        self.assertEqual(risk.reservations[self.entry.risk_reservation_id].status.value, "filled")
        self.assertEqual(
            SQLiteRiskStateRepository(self.database_path).counts(), command_counts
        )

    def test_pending_flat_confirmation_reopens_to_reconciliation_required(self) -> None:
        acknowledged = self.service.submit(self.entry)
        snapshot = DemoStrategyAccountSnapshot(
            equity=Decimal("1000"),
            captured_at=START + timedelta(minutes=2),
            positions=(),
            orders=(DemoAccountOrder(acknowledged.order_link_id, "BTCUSDT"),),
        )
        monitor = DemoLifecycleMonitor(
            self.execution_repository,
            SQLiteRiskStateRepository(self.database_path),
            self.client,  # type: ignore[arg-type]
            FakeSnapshotProvider(snapshot),  # type: ignore[arg-type]
            clock=lambda: START + timedelta(minutes=2),
        )
        monitor.run_cycle()
        self.execution_repository.transition(
            acknowledged.intent_id,
            ExecutionIntentState.POSITION_CLOSED_PENDING,
            "position_closed_detected",
            "owned_position_flat; exit_reason_unknown",
        )

        result = monitor.run_cycle()

        self.assertEqual(result.risk_updates, 0)
        self.assertEqual(
            self.execution_repository.get(acknowledged.intent_id).state,
            ExecutionIntentState.RECONCILIATION_REQUIRED,
        )
        risk = SQLiteRiskStateRepository(self.database_path).load_state()
        self.assertIsNotNone(risk)
        self.assertEqual(risk.reservations[self.entry.risk_reservation_id].status.value, "filled")

    def test_monitor_reconciles_cancelled_entry_and_releases_risk(self) -> None:
        acknowledged = self.service.submit(self.entry)
        self.client.order_status = "Cancelled"
        self.client.flat_position = True
        snapshot = DemoStrategyAccountSnapshot(
            equity=Decimal("1000"),
            captured_at=START + timedelta(minutes=2),
            positions=(),
            orders=(),
        )
        monitor = DemoLifecycleMonitor(
            self.execution_repository,
            SQLiteRiskStateRepository(self.database_path),
            self.client,  # type: ignore[arg-type]
            FakeSnapshotProvider(snapshot),  # type: ignore[arg-type]
            clock=lambda: START + timedelta(minutes=2),
        )

        result = monitor.run_cycle()

        self.assertEqual(result.cancelled, 1)
        self.assertEqual(result.risk_updates, 1)
        state = SQLiteRiskStateRepository(self.database_path).load_state()
        self.assertIsNotNone(state)
        self.assertEqual(
            state.reservations[self.entry.risk_reservation_id].status.value,
            "cancelled",
        )


if __name__ == "__main__":
    unittest.main()
