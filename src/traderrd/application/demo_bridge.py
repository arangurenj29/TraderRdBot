from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Callable, Protocol

from traderrd.application.demo_execution import DemoExecutionPlanner
from traderrd.domain.bridge import (
    BridgeStatus,
    DemoBridgeResult,
    DemoStrategyAccountSnapshot,
    VALIDATED_SOURCE_CHAT_ID,
    VALIDATED_SOURCE_TOPIC_ID,
)
from traderrd.domain.execution import InstrumentRules
from traderrd.domain.risk import (
    InitializeRiskEngine,
    PortfolioRiskEngine,
    ProposeRiskReservation,
    TradeProposal,
)
from traderrd.infrastructure.bridge_repository import SQLiteDemoBridgeRepository
from traderrd.infrastructure.execution_repository import (
    SQLiteDemoExecutionRepository,
)
from traderrd.infrastructure.risk_repository import SQLiteRiskStateRepository


class DemoAccountSnapshotProvider(Protocol):
    def fetch(
        self, symbol: str
    ) -> tuple[DemoStrategyAccountSnapshot, InstrumentRules]: ...


class DemoBridgeError(RuntimeError):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


class DemoSignalBridgeService:
    """Supervised bridge from a scoped stored signal to planned Demo intents."""

    def __init__(
        self,
        bridge_repository: SQLiteDemoBridgeRepository,
        risk_repository: SQLiteRiskStateRepository,
        execution_repository: SQLiteDemoExecutionRepository,
        snapshot_provider: DemoAccountSnapshotProvider,
        estimated_cost_rate: Decimal = Decimal("0.001"),
        max_snapshot_age: timedelta = timedelta(seconds=30),
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        if (
            not estimated_cost_rate.is_finite()
            or estimated_cost_rate < 0
            or estimated_cost_rate >= 1
        ):
            raise ValueError("Estimated cost rate must be between zero and one")
        self._bridge = bridge_repository
        self._risk = risk_repository
        self._execution = execution_repository
        self._snapshot_provider = snapshot_provider
        self._estimated_cost_rate = estimated_cost_rate
        self._max_snapshot_age = max_snapshot_age
        self._clock = clock or (lambda: datetime.now(timezone.utc))

    def process(
        self,
        source_message_id: int,
        source_sender_id: int,
        persist_risk: bool = False,
    ) -> DemoBridgeResult:
        signal = self._bridge.get_scoped_signal(
            VALIDATED_SOURCE_CHAT_ID,
            VALIDATED_SOURCE_TOPIC_ID,
            source_sender_id,
            source_message_id,
        )
        if signal is None:
            raise DemoBridgeError(
                "scoped_signal_unavailable",
                "A validated signal with the exact source provenance is required",
            )
        existing = self._bridge.get_run(signal.fingerprint)
        if existing is not None:
            return DemoBridgeResult(
                BridgeStatus.DUPLICATE,
                "bridge_run_already_recorded",
                signal.fingerprint,
                risk_command_id=str(existing["risk_command_id"]),
                equity=Decimal(str(existing["strategy_equity"])),
                duplicate=True,
            )

        snapshot, rules = self._snapshot_provider.fetch(signal.symbol)
        self._validate_snapshot(snapshot)
        self._validate_account_ownership(snapshot)
        expires_at = signal.telegram_received_at + timedelta(hours=3)
        if snapshot.captured_at >= expires_at:
            return DemoBridgeResult(
                BridgeStatus.STALE,
                "telegram_entry_window_expired",
                signal.fingerprint,
                equity=snapshot.equity,
            )

        risk_command_id = f"demo-bridge-risk:{signal.fingerprint}"
        proposal = TradeProposal(
            signal_id=signal.fingerprint,
            symbol=signal.symbol,
            direction=signal.direction,
            entry=signal.entry,
            take_profit=signal.take_profit,
            stop_loss=signal.stop_loss,
            entry_expires_at=expires_at,
        )
        engine = PortfolioRiskEngine()

        if persist_risk:
            self._bridge.initialize()
            self._risk.initialize()
            self._execution.initialize()
            state = self._risk.load_state()
            if state is None:
                initialized = self._risk.execute(
                    InitializeRiskEngine(
                        command_id="demo-bridge-risk-initialize",
                        occurred_at=snapshot.captured_at,
                        equity=snapshot.equity,
                        estimated_cost_rate=self._estimated_cost_rate,
                    ),
                    engine,
                )
                state = initialized.state
            self._validate_cost_policy(state.policy.estimated_cost_rate)
            result = self._risk.get_command_result(risk_command_id)
            if result is None:
                result = self._risk.execute(
                    ProposeRiskReservation(
                        command_id=risk_command_id,
                        occurred_at=snapshot.captured_at,
                        mark_equity=snapshot.equity,
                        proposal=proposal,
                    ),
                    engine,
                )
        else:
            state = deepcopy(self._risk.load_state())
            if state is None:
                state = engine.process(
                    None,
                    InitializeRiskEngine(
                        command_id="demo-bridge-preview-initialize",
                        occurred_at=snapshot.captured_at,
                        equity=snapshot.equity,
                        estimated_cost_rate=self._estimated_cost_rate,
                    ),
                ).state
            self._validate_cost_policy(state.policy.estimated_cost_rate)
            result = engine.process(
                state,
                ProposeRiskReservation(
                    command_id=risk_command_id,
                    occurred_at=snapshot.captured_at,
                    mark_equity=snapshot.equity,
                    proposal=proposal,
                ),
            )

        intents = tuple(
            DemoExecutionPlanner.plan_decision(
                rules, risk_command_id, result.decision, result.state
            )
        )
        status = (
            BridgeStatus.REJECTED
            if result.decision.status == "rejected"
            else BridgeStatus.PERSISTED
            if persist_risk
            else BridgeStatus.PREVIEW
        )
        if persist_risk:
            for intent in intents:
                self._execution.save_planned(intent)
            inserted = self._bridge.record_run(
                signal,
                snapshot.captured_at,
                snapshot.equity,
                risk_command_id,
                result.decision.status,
                result.decision.reason,
                len(intents),
            )
            if not inserted:
                status = BridgeStatus.DUPLICATE
        return DemoBridgeResult(
            status=status,
            reason=result.decision.reason,
            signal_fingerprint=signal.fingerprint,
            risk_command_id=risk_command_id,
            equity=snapshot.equity,
            decision=result.decision,
            intents=intents,
            duplicate=False,
        )

    def validate_account_snapshot(
        self, snapshot: DemoStrategyAccountSnapshot
    ) -> None:
        """Fail closed when retrying a previously persisted signal."""
        self._validate_snapshot(snapshot)
        self._validate_account_ownership(snapshot)

    def _validate_snapshot(self, snapshot: DemoStrategyAccountSnapshot) -> None:
        if self._max_snapshot_age <= timedelta(0):
            raise ValueError("Snapshot age bound must be positive")
        now = self._clock()
        if now.tzinfo is None:
            raise ValueError("Bridge clock must be timezone-aware")
        age = now - snapshot.captured_at
        if age < timedelta(seconds=-3) or age > self._max_snapshot_age:
            raise DemoBridgeError(
                "account_snapshot_stale", "Demo account snapshot is stale"
            )

    def _validate_account_ownership(
        self, snapshot: DemoStrategyAccountSnapshot
    ) -> None:
        known_links = self._execution.known_order_links()
        if any(
            not order.is_known_or_exchange_protective(known_links)
            for order in snapshot.orders
        ):
            raise DemoBridgeError(
                "unmanaged_orders",
                "Demo account contains an order not managed by TraderRd",
            )
        managed_positions: dict[tuple[str, object], Decimal] = {}
        for intent in self._execution.managed_position_intents():
            key = (intent.symbol, intent.direction)
            managed_positions[key] = managed_positions.get(key, Decimal("0")) + (
                intent.quantity
            )
        account_positions: dict[tuple[str, object], Decimal] = {}
        for position in snapshot.positions:
            key = (position.symbol, position.direction)
            account_positions[key] = account_positions.get(key, Decimal("0")) + (
                position.quantity
            )
        if account_positions != managed_positions:
            raise DemoBridgeError(
                "unmanaged_positions",
                "Demo account positions do not match TraderRd managed state",
            )

    def _validate_cost_policy(self, stored_rate: Decimal) -> None:
        if stored_rate != self._estimated_cost_rate:
            raise DemoBridgeError(
                "risk_policy_mismatch",
                "Configured estimated costs differ from persistent risk state",
            )
