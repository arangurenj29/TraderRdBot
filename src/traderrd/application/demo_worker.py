from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Callable

from traderrd.application.demo_bridge import (
    DemoAccountSnapshotProvider,
    DemoBridgeError,
    DemoSignalBridgeService,
)
from traderrd.application.demo_execution import (
    DemoExecutionPlanner,
    DemoExecutionService,
)
from traderrd.domain.bridge import (
    BridgeStatus,
    VALIDATED_SOURCE_CHAT_ID,
    VALIDATED_SOURCE_TOPIC_ID,
)
from traderrd.domain.execution import ExecutionIntent, ExecutionIntentKind, ExecutionIntentState
from traderrd.infrastructure.bridge_repository import SQLiteDemoBridgeRepository
from traderrd.infrastructure.bybit_demo import (
    BybitV5DemoClient,
    DemoExecutionError,
)
from traderrd.infrastructure.execution_repository import (
    SQLiteDemoExecutionRepository,
)
from traderrd.infrastructure.risk_repository import SQLiteRiskStateRepository


VALIDATED_SOURCE_SENDER_ID = 8003985182


class DemoWorkerError(RuntimeError):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


@dataclass(frozen=True, slots=True)
class DemoWorkerResult:
    scanned: int
    fresh: int
    stale: int
    deferred: int
    duplicate: int
    risk_accepted: int
    risk_rejected: int
    orders_submitted: int
    blocked_dependencies: int
    cursor: int
    database_writes: bool


class DemoSignalWorker:
    """Consume only new, source-attested signals and submit Demo intents.

    The first applied cycle establishes a live boundary at the newest signal
    already in SQLite. This is intentional: backfill/history rows are never
    replayed as live trades after a worker restart.
    """

    def __init__(
        self,
        bridge_repository: SQLiteDemoBridgeRepository,
        risk_repository: SQLiteRiskStateRepository,
        execution_repository: SQLiteDemoExecutionRepository,
        snapshot_provider: DemoAccountSnapshotProvider | None,
        client: BybitV5DemoClient | None,
        *,
        source_sender_id: int = VALIDATED_SOURCE_SENDER_ID,
        max_signal_age: timedelta = timedelta(hours=3),
        future_skew: timedelta = timedelta(seconds=30),
        batch_size: int = 100,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        if source_sender_id <= 0:
            raise ValueError("Worker source sender ID must be positive")
        if max_signal_age <= timedelta(0):
            raise ValueError("Worker signal age bound must be positive")
        if future_skew < timedelta(0):
            raise ValueError("Worker future skew cannot be negative")
        if batch_size <= 0:
            raise ValueError("Worker batch size must be positive")
        self._bridge = bridge_repository
        self._risk = risk_repository
        self._execution = execution_repository
        self._snapshots = snapshot_provider
        self._client = client
        self._source_sender_id = source_sender_id
        self._max_signal_age = max_signal_age
        self._future_skew = future_skew
        self._batch_size = batch_size
        self._clock = clock or (lambda: datetime.now(timezone.utc))

    def run_cycle(self, *, apply: bool = False) -> DemoWorkerResult:
        now = self._clock()
        if now.tzinfo is None:
            raise ValueError("Worker clock must be timezone-aware")
        if apply:
            self._validate_demo_client()

        cursor = self._bridge.get_worker_cursor(
            VALIDATED_SOURCE_CHAT_ID,
            VALIDATED_SOURCE_TOPIC_ID,
            self._source_sender_id,
        )
        if cursor is None:
            baseline = self._bridge.max_scoped_message_id(
                VALIDATED_SOURCE_CHAT_ID,
                VALIDATED_SOURCE_TOPIC_ID,
                self._source_sender_id,
            )
            if apply:
                self._initialize_for_apply()
                cursor = self._bridge.initialize_worker_cursor(
                    VALIDATED_SOURCE_CHAT_ID,
                    VALIDATED_SOURCE_TOPIC_ID,
                    self._source_sender_id,
                    baseline,
                )
            else:
                cursor = baseline

        if not apply:
            return self._dry_run(cursor, now)

        if self._snapshots is None or self._client is None:
            raise DemoWorkerError(
                "demo_credentials_required",
                "Automatic Demo execution requires Demo account access",
            )

        submitted, blocked = self._retry_planned_intents()
        signals = self._bridge.list_scoped_signals_after(
            VALIDATED_SOURCE_CHAT_ID,
            VALIDATED_SOURCE_TOPIC_ID,
            self._source_sender_id,
            cursor,
            self._batch_size,
        )
        scanned = fresh = stale = deferred = duplicate = accepted = rejected = 0
        current_cursor = cursor
        for signal in signals:
            scanned += 1
            if signal.telegram_received_at > now + self._future_skew:
                deferred += 1
                break
            if signal.telegram_received_at < now - self._max_signal_age:
                stale += 1
                self._bridge.advance_worker_cursor(
                    VALIDATED_SOURCE_CHAT_ID,
                    VALIDATED_SOURCE_TOPIC_ID,
                    self._source_sender_id,
                    signal.source_message_id,
                )
                current_cursor = signal.source_message_id
                continue

            fresh += 1
            result, rules = self._persist_signal(signal)
            if result.status is BridgeStatus.DUPLICATE:
                duplicate += 1
            if result.status is BridgeStatus.STALE:
                stale += 1
            if result.decision is not None:
                if result.decision.status == "accepted":
                    accepted += 1
                elif result.decision.status == "rejected":
                    rejected += 1

            intents = list(result.intents)
            if result.status is BridgeStatus.DUPLICATE:
                if result.risk_command_id is None:
                    raise DemoWorkerError(
                        "duplicate_without_risk_command",
                        "Persisted signal bridge run has no risk command",
                    )
                intents = self._restore_intents(signal.symbol, result.risk_command_id)
                rules = self._rules_for(signal.symbol)
            elif intents:
                rules = self._rules_for(signal.symbol)

            submitted_now, blocked_now = self._submit_intents(intents, rules)
            submitted += submitted_now
            blocked += blocked_now
            self._bridge.advance_worker_cursor(
                VALIDATED_SOURCE_CHAT_ID,
                VALIDATED_SOURCE_TOPIC_ID,
                self._source_sender_id,
                signal.source_message_id,
            )
            current_cursor = signal.source_message_id

        return DemoWorkerResult(
            scanned=scanned,
            fresh=fresh,
            stale=stale,
            deferred=deferred,
            duplicate=duplicate,
            risk_accepted=accepted,
            risk_rejected=rejected,
            orders_submitted=submitted,
            blocked_dependencies=blocked,
            cursor=current_cursor,
            database_writes=True,
        )

    def _dry_run(self, cursor: int, now: datetime) -> DemoWorkerResult:
        signals = self._bridge.list_scoped_signals_after(
            VALIDATED_SOURCE_CHAT_ID,
            VALIDATED_SOURCE_TOPIC_ID,
            self._source_sender_id,
            cursor,
            self._batch_size,
        )
        fresh = stale = deferred = accepted = rejected = 0
        for signal in signals:
            if signal.telegram_received_at > now + self._future_skew:
                deferred += 1
                break
            if signal.telegram_received_at < now - self._max_signal_age:
                stale += 1
                continue
            fresh += 1
        if self._snapshots is None:
            return DemoWorkerResult(
                scanned=len(signals),
                fresh=fresh,
                stale=stale,
                deferred=deferred,
                duplicate=0,
                risk_accepted=0,
                risk_rejected=0,
                orders_submitted=0,
                blocked_dependencies=0,
                cursor=cursor,
                database_writes=False,
            )

        for signal in signals:
            if signal.telegram_received_at > now + self._future_skew:
                break
            if signal.telegram_received_at < now - self._max_signal_age:
                continue
            result = DemoSignalBridgeService(
                self._bridge,
                self._risk,
                self._execution,
                self._snapshots,
                clock=self._clock,
            ).process(
                signal.source_message_id,
                signal.source_sender_id,
                persist_risk=False,
            )
            if result.decision is not None:
                if result.decision.status == "accepted":
                    accepted += 1
                elif result.decision.status == "rejected":
                    rejected += 1
        return DemoWorkerResult(
            scanned=len(signals),
            fresh=fresh,
            stale=stale,
            deferred=deferred,
            duplicate=0,
            risk_accepted=accepted,
            risk_rejected=rejected,
            orders_submitted=0,
            blocked_dependencies=0,
            cursor=cursor,
            database_writes=False,
        )

    def _persist_signal(self, signal):
        service = DemoSignalBridgeService(
            self._bridge,
            self._risk,
            self._execution,
            self._snapshots,
            clock=self._clock,
        )
        try:
            return service.process(
                signal.source_message_id,
                signal.source_sender_id,
                persist_risk=True,
            ), None
        except DemoBridgeError:
            raise

    def _restore_intents(self, symbol: str, risk_command_id: str) -> list[ExecutionIntent]:
        rules = self._rules_for(symbol)
        return DemoExecutionPlanner(self._execution.database_path).preview(
            rules, risk_command_id
        )

    def _rules_for(self, symbol: str):
        snapshot, rules = self._snapshots.fetch(symbol)
        DemoSignalBridgeService(
            self._bridge,
            self._risk,
            self._execution,
            self._snapshots,
            clock=self._clock,
        ).validate_account_snapshot(snapshot)
        return rules

    def _retry_planned_intents(self) -> tuple[int, int]:
        submitted = blocked = 0
        for intent in self._execution.planned_intents():
            rules = self._rules_for(intent.symbol)
            current = self._execution.get(intent.intent_id)
            if current is None or current.state is not ExecutionIntentState.PLANNED:
                continue
            made, was_blocked = self._submit_intent(current, rules)
            submitted += made
            blocked += was_blocked
        return submitted, blocked

    def _submit_intents(self, intents: list[ExecutionIntent], rules) -> tuple[int, int]:
        submitted = blocked = 0
        for intent in intents:
            made, was_blocked = self._submit_intent(intent, rules)
            submitted += made
            blocked += was_blocked
        return submitted, blocked

    def _submit_intent(self, intent: ExecutionIntent, rules) -> tuple[int, int]:
        service = DemoExecutionService(self._client, self._execution, rules)
        current = self._execution.get(intent.intent_id)
        if current is None:
            current = self._execution.save_planned(intent)
        if current.state is not ExecutionIntentState.PLANNED:
            return 0, 0

        if intent.kind is ExecutionIntentKind.ENTRY:
            if not self._entry_dependencies_confirmed(intent, service):
                return 0, 1
        if (
            intent.kind is not ExecutionIntentKind.CANCEL_ENTRY
            and self._execution.has_submission_attempt(intent.intent_id)
        ):
            try:
                recovered = service.reconcile(intent.intent_id)
            except DemoExecutionError as exc:
                if exc.code != "order_reconciliation":
                    raise
            else:
                if recovered.state is not ExecutionIntentState.PLANNED:
                    return 0, 0
        if intent.kind is not ExecutionIntentKind.CANCEL_ENTRY:
            self._execution.mark_submission_attempt(intent.intent_id)
        before = current.state
        stored = service.submit(current)
        return int(before is ExecutionIntentState.PLANNED and stored.state is ExecutionIntentState.ACKNOWLEDGED), 0

    def _entry_dependencies_confirmed(
        self, intent: ExecutionIntent, service: DemoExecutionService
    ) -> bool:
        dependencies = [
            item
            for item in self._execution.intents_for_risk_command(intent.risk_command_id)
            if item.kind is ExecutionIntentKind.CANCEL_ENTRY
        ]
        for dependency in dependencies:
            current = self._execution.get(dependency.intent_id) or dependency
            if current.state is ExecutionIntentState.PLANNED:
                current = service.submit(current)
            if current.state is not ExecutionIntentState.CANCELLED:
                current = service.reconcile(current.intent_id)
            if current.state is not ExecutionIntentState.CANCELLED:
                return False
        return True

    def _initialize_for_apply(self) -> None:
        self._bridge.initialize()
        self._risk.initialize()
        self._execution.initialize()

    def _validate_demo_client(self) -> None:
        base_url = getattr(getattr(self._client, "config", None), "base_url", None)
        if base_url != "https://api-demo.bybit.com":
            raise DemoWorkerError(
                "demo_only_guard",
                "Automatic execution is restricted to Bybit Demo Trading",
            )
