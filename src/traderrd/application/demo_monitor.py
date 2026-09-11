from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime, timezone
from typing import Callable

from traderrd.application.demo_bridge import DemoAccountSnapshotProvider
from traderrd.application.demo_execution import DemoExecutionService
from traderrd.application.demo_execution import DemoExecutionPlanner
from traderrd.domain.bridge import DemoStrategyAccountSnapshot
from traderrd.domain.execution import ExecutionIntentState, InstrumentRules
from traderrd.domain.risk import (
    CancelPendingReservation,
    ConfirmPendingFill,
    ConfirmPositionClosed,
    PortfolioRiskEngine,
    RiskActionType,
    ReservationStatus,
    RequestPositionClose,
)
from traderrd.infrastructure.execution_repository import (
    SQLiteDemoExecutionRepository,
)
from traderrd.infrastructure.bybit_demo import BybitV5DemoClient
from traderrd.infrastructure.bybit_demo import DemoExecutionError
from traderrd.infrastructure.risk_repository import SQLiteRiskStateRepository


class DemoMonitorError(RuntimeError):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


@dataclass(frozen=True, slots=True)
class DemoMonitorResult:
    inspected: int
    working: int
    filled: int
    protected: int
    cancelled: int
    risk_updates: int


class DemoLifecycleMonitor:
    """Reconcile owned Demo intents and synchronize risk reservations."""

    def __init__(
        self,
        execution_repository: SQLiteDemoExecutionRepository,
        risk_repository: SQLiteRiskStateRepository,
        client: BybitV5DemoClient,
        snapshot_provider: DemoAccountSnapshotProvider,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._execution = execution_repository
        self._risk = risk_repository
        self._client = client
        self._snapshots = snapshot_provider
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._risk_engine = PortfolioRiskEngine()

    def close_owned_position(self, intent_id: str):
        """Explicit operator recovery: evidence, durable close request, then flat/PnL."""
        if getattr(getattr(self._client, "config", None), "base_url", None) != "https://api-demo.bybit.com":
            raise DemoMonitorError("demo_only_guard", "Operator close is restricted to Bybit Demo Trading")
        entry = self._execution.get(intent_id)
        if entry is None or entry.kind.value != "entry":
            raise DemoMonitorError("close_guard", "Select an owned entry intent")
        snapshot, rules = self._snapshots.fetch(entry.symbol)
        self._validate_ownership(snapshot)
        now = self._clock()
        state = self._risk.load_state()
        reservation = state.reservations.get(entry.risk_reservation_id) if state else None
        if reservation is None or reservation.status not in {
            ReservationStatus.PENDING, ReservationStatus.FILLED,
            ReservationStatus.CLOSE_REQUESTED, ReservationStatus.CLOSED,
        }:
            raise DemoMonitorError("close_guard", "Owned reservation requires reconciliation")
        service = DemoExecutionService(self._client, self._execution, rules, clock=self._clock)
        close = self._execution.find_close(entry.risk_reservation_id)
        if reservation.status is ReservationStatus.CLOSED:
            if any(position.symbol == entry.symbol and position.quantity != 0 for position in snapshot.positions):
                raise DemoMonitorError("close_guard", "Closed reservation has reopened exposure")
            if not self._execution.outcome_is_attributed(entry.risk_reservation_id):
                raise DemoMonitorError("close_guard", "Closed reservation has no attributed outcome")
            self._execution.transition(entry.intent_id, ExecutionIntentState.POSITION_CLOSED,
                "operator_close_risk_released", "risk_close_confirmed; exit_reason_unknown")
            if close is not None:
                self._execution.mark_risk_sync_completed(close.intent_id)
            return close or self._execution.get(entry.intent_id)
        already_flat = None
        if close is None or (close.state is ExecutionIntentState.PLANNED
                             and not self._execution.has_submission_attempt(close.intent_id)):
            filled = service.reconcile_for_operator_close(entry.intent_id)
            if filled.state is ExecutionIntentState.POSITION_CLOSED_PENDING:
                already_flat = filled
                filled = replace(filled, state=ExecutionIntentState.FILLED)
            self._sync_risk(filled, snapshot, rules, now)
        state = self._risk.load_state()
        command_id = f"demo-operator-close:{entry.intent_id}"
        command = self._risk.get_command(command_id) or RequestPositionClose(
            command_id, max(self._clock(), snapshot.captured_at, state.as_of), entry.risk_reservation_id,
        )
        if not isinstance(command, RequestPositionClose) or command.reservation_id != entry.risk_reservation_id:
            raise DemoMonitorError("close_guard", "Operator close command identity conflicts")
        result = self._risk.execute(command, self._risk_engine)
        if result.decision.status not in {"accepted", "duplicate"}:
            raise DemoMonitorError("risk_sync_rejected", "Operator close was not accepted by risk ledger")
        if already_flat is not None:
            if close is not None:
                self._execution.transition(close.intent_id, ExecutionIntentState.CANCELLED,
                    "operator_close_not_needed", "owned_position_already_flat")
            if service.attribute_closed_outcome(already_flat, self._clock()):
                self._sync_risk(already_flat, snapshot, rules, self._clock())
            return self._execution.get(entry.intent_id)
        close = self._execution.find_close(entry.risk_reservation_id)
        if close is None:
            self._materialize_follow_up_actions(command_id, result, rules)
            close = self._execution.find_close(entry.risk_reservation_id)
        if close is None:
            raise DemoMonitorError("close_guard", "Owned close plan unavailable")
        if close.state is ExecutionIntentState.PLANNED:
            close = service.submit(close)
        close = service.reconcile(close.intent_id)
        if close.state is ExecutionIntentState.CLOSE_CONFIRMED and service.attribute_closed_outcome(close, self._clock()):
            fresh, _ = self._snapshots.fetch(entry.symbol)
            self._validate_ownership(fresh)
            self._sync_risk(close, fresh, rules, self._clock())
            self._execution.transition(entry.intent_id, ExecutionIntentState.POSITION_CLOSED,
                "operator_close_risk_released", "risk_close_confirmed; exit_reason_unknown")
        return close

    def run_cycle(self) -> DemoMonitorResult:
        now = self._clock()
        if now.tzinfo is None:
            raise ValueError("Monitor clock must be timezone-aware")
        intents = self._execution.monitorable_intents()
        if not intents:
            return DemoMonitorResult(0, 0, 0, 0, 0, 0)

        snapshots: dict[str, tuple[DemoStrategyAccountSnapshot, InstrumentRules]] = {}
        for symbol in {intent.symbol for intent in intents}:
            snapshot, rules = self._snapshots.fetch(symbol)
            self._validate_ownership(snapshot)
            snapshots[symbol] = (snapshot, rules)

        risk_updates = 0
        for intent in intents:
            snapshot, rules = snapshots[intent.symbol]
            service = DemoExecutionService(
                self._client,
                self._execution,
                rules,
                clock=self._clock,
            )
            try:
                reconciled = service.reconcile(intent.intent_id)
                if reconciled.state is ExecutionIntentState.POSITION_CLOSED_PENDING or (
                    reconciled.kind.value == "close_position"
                    and reconciled.state is ExecutionIntentState.CLOSE_CONFIRMED
                ):
                    # Do not release a reservation merely because the position
                    # is flat.  A complete, unique Bybit closed-PnL record is
                    # required first; otherwise the close remains visible as
                    # unresolved and risk remains fail-closed.
                    if not service.attribute_closed_outcome(reconciled, now):
                        continue
            except DemoExecutionError:
                current = self._execution.get(intent.intent_id)
                if current is not None and current.state not in {
                    ExecutionIntentState.PROTECTION_VERIFIED,
                    ExecutionIntentState.POSITION_CLOSED_PENDING,
                }:
                    risk_updates += self._sync_risk(current, snapshot, rules, now)
                raise
            risk_updates += self._sync_risk(reconciled, snapshot, rules, now)

        expired = self._execution.expirable_entries(now)
        for symbol in {intent.symbol for intent in expired}:
            _, rules = snapshots.get(symbol) or self._snapshots.fetch(symbol)
            DemoExecutionService(
                self._client,
                self._execution,
                rules,
                clock=self._clock,
            ).cancel_expired(now)
        for intent in expired:
            current = self._execution.get(intent.intent_id)
            if current is not None:
                snapshot, rules = snapshots[intent.symbol]
                risk_updates += self._sync_risk(current, snapshot, rules, now)

        final_states = [
            self._execution.get(intent.intent_id)
            for intent in intents
        ]
        states = [state.state for state in final_states if state is not None]
        return DemoMonitorResult(
            inspected=len(intents),
            working=sum(state is ExecutionIntentState.WORKING for state in states),
            filled=sum(state is ExecutionIntentState.FILLED for state in states),
            protected=sum(
                state is ExecutionIntentState.PROTECTION_VERIFIED
                for state in states
            ),
            cancelled=sum(
                state is ExecutionIntentState.CANCELLED for state in states
            ),
            risk_updates=risk_updates,
        )

    def _sync_risk(
        self,
        intent,
        snapshot: DemoStrategyAccountSnapshot,
        rules: InstrumentRules,
        now: datetime,
    ) -> int:
        state = self._risk.load_state()
        if state is None:
            raise DemoMonitorError(
                "risk_state_unavailable", "Risk engine state is not initialized"
            )
        reservation = state.reservations.get(intent.risk_reservation_id)
        if (
            intent.state is ExecutionIntentState.POSITION_CLOSED_PENDING
            and intent.filled_quantity is not None
            and reservation is not None
            and reservation.status is ReservationStatus.PENDING
        ):
            # A protected partial can close before the first risk-fill handoff.
            # Preserve that evidence before the separate close command.
            self._sync_risk(replace(intent, state=ExecutionIntentState.FILLED), snapshot, rules, now)
            state = self._risk.load_state()
        occurred_at = max(now, snapshot.captured_at, state.as_of)
        if intent.kind.value in {"entry", "cancel_entry"} and (
            intent.state in {ExecutionIntentState.FILLED, ExecutionIntentState.PROTECTION_VERIFIED}
            or (intent.state is ExecutionIntentState.RECONCILIATION_REQUIRED and intent.filled_quantity is not None)
        ):
            reservation = state.reservations.get(intent.risk_reservation_id)
            if reservation is None:
                raise DemoMonitorError(
                    "risk_reservation_missing",
                    "Filled entry has no matching risk reservation",
                )
            if reservation.status in {ReservationStatus.FILLED, ReservationStatus.CLOSE_REQUESTED}:
                # A protected position is reconciled every monitor cycle, but its
                # fill command must be issued exactly once. Its command payload
                # contains the cycle timestamp, so resubmission would violate the
                # durable idempotency contract rather than provide new evidence.
                prior = self._risk.get_command_result(f"demo-monitor-fill:{intent.intent_id}")
                if prior is not None:
                    self._materialize_follow_up_actions(f"demo-monitor-fill:{intent.intent_id}", prior, rules)
                return 0
            if reservation.status is not ReservationStatus.PENDING:
                raise DemoMonitorError(
                    "risk_reservation_unexpected_state",
                    "Filled entry has a risk reservation outside the pending state",
                )
            command = ConfirmPendingFill(
                command_id=f"demo-monitor-fill:{intent.intent_id}",
                occurred_at=occurred_at,
                reservation_id=intent.risk_reservation_id,
                mark_equity=snapshot.equity,
            )
        elif intent.kind.value in {"entry", "cancel_entry"} and intent.state in {
            ExecutionIntentState.CANCELLED,
            ExecutionIntentState.REJECTED,
        }:
            if intent.kind.value == "cancel_entry" and reservation is not None and reservation.status in {
                ReservationStatus.CANCELLED, ReservationStatus.EXPIRED, ReservationStatus.CLOSED,
            }:
                self._execution.mark_risk_sync_completed(intent.intent_id)
                return 0
            command = CancelPendingReservation(
                command_id=f"demo-monitor-cancel:{intent.intent_id}",
                occurred_at=occurred_at,
                reservation_id=intent.risk_reservation_id,
                reason="demo_entry_cancelled",
                mark_equity=snapshot.equity,
            )
        elif (
            intent.kind.value == "entry"
            and intent.state is ExecutionIntentState.POSITION_CLOSED_PENDING
        ):
            command = ConfirmPositionClosed(
                command_id=f"demo-monitor-position-closed:{intent.intent_id}",
                occurred_at=occurred_at,
                reservation_id=intent.risk_reservation_id,
                mark_equity=snapshot.equity,
            )
        elif (
            intent.kind.value == "close_position"
            and intent.state is ExecutionIntentState.CLOSE_CONFIRMED
        ):
            command = ConfirmPositionClosed(
                command_id=f"demo-monitor-close:{intent.intent_id}",
                occurred_at=occurred_at,
                reservation_id=intent.risk_reservation_id,
                mark_equity=snapshot.equity,
            )
        else:
            return 0
        persisted = self._risk.get_command(command.command_id)
        if persisted is not None:
            # Reuse the original snapshot/time after a crash, but leave the
            # repository's strict payload conflict check intact for identity.
            command = replace(command, occurred_at=persisted.occurred_at)
            if type(command) is type(persisted):
                command = replace(command, mark_equity=persisted.mark_equity)
        result = self._risk.execute(command, self._risk_engine)
        if result.decision.status not in {"accepted", "duplicate"}:
            raise DemoMonitorError(
                "risk_sync_rejected",
                "Exchange lifecycle update was not accepted by the risk ledger",
            )
        self._materialize_follow_up_actions(command.command_id, result, rules)
        if intent.state is ExecutionIntentState.POSITION_CLOSED_PENDING:
            self._execution.transition(
                intent.intent_id,
                ExecutionIntentState.POSITION_CLOSED,
                "position_closed_risk_released",
                "risk_close_confirmed; exit_reason_unknown",
            )
        if intent.state in {ExecutionIntentState.CANCELLED, ExecutionIntentState.REJECTED, ExecutionIntentState.CLOSE_CONFIRMED}:
            self._execution.mark_risk_sync_completed(intent.intent_id)
        return int(not result.duplicate_command)

    def _materialize_follow_up_actions(
        self, command_id: str, result, rules: InstrumentRules
    ) -> None:
        """Persist follow-up close/reversal plans for the signal worker."""
        def completed_parent(action) -> bool:
            if action.action_type is not RiskActionType.CANCEL_PENDING:
                return False
            entry = self._execution.find_entry(action.reservation_id)
            return entry is not None and entry.state in {
                ExecutionIntentState.CANCELLED, ExecutionIntentState.REJECTED,
                ExecutionIntentState.POSITION_CLOSED,
            }
        actions = tuple(
            action
            for action in result.decision.actions
            if not completed_parent(action)
            if action.action_type
            in {
                RiskActionType.CREATE_PENDING_POST_ONLY,
                RiskActionType.REQUEST_CLOSE_POSITION,
                RiskActionType.CANCEL_PENDING,
            }
        )
        if not actions:
            return
        decision = result.decision
        if actions != decision.actions:
            from dataclasses import replace

            decision = replace(decision, actions=actions)
        for symbol in {action.symbol for action in actions}:
            symbol_rules = rules if symbol == rules.symbol else self._snapshots.fetch(symbol)[1]
            for planned in DemoExecutionPlanner.plan_decision(
                symbol_rules, command_id, decision, result.state
            ):
                self._execution.save_planned(planned)

    def _validate_ownership(self, snapshot: DemoStrategyAccountSnapshot) -> None:
        known_links = self._execution.known_order_links()
        if any(
            not order.is_known_or_exchange_protective(known_links)
            for order in snapshot.orders
        ):
            raise DemoMonitorError(
                "unmanaged_orders", "Demo account contains an unmanaged order"
            )
        owned_keys = self._execution.owned_entry_keys()
        if any(
            (position.symbol, position.direction) not in owned_keys
            for position in snapshot.positions
        ):
            raise DemoMonitorError(
                "unmanaged_positions",
                "Demo account contains an unmanaged position",
            )
