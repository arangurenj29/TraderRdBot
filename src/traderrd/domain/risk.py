from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import date, datetime, timedelta
from decimal import Decimal
from enum import StrEnum
from zoneinfo import ZoneInfo

from traderrd.domain.models import Direction, canonical_decimal


LIMA = ZoneInfo("America/Lima")


class EngineMode(StrEnum):
    ACTIVE = "active"
    ENTRY_PAUSED = "entry_paused"
    KILLED = "killed"


class ReservationStatus(StrEnum):
    PENDING = "pending"
    FILLED = "filled"
    CLOSE_REQUESTED = "close_requested"
    CANCELLED = "cancelled"
    EXPIRED = "expired"
    CLOSED = "closed"


class RiskActionType(StrEnum):
    CREATE_PENDING_POST_ONLY = "create_pending_post_only"
    CANCEL_PENDING = "cancel_pending"
    REQUEST_CLOSE_POSITION = "request_close_position"


@dataclass(frozen=True, slots=True)
class RiskPolicy:
    leverage: Decimal = Decimal("10")
    max_notional_equity_fraction: Decimal = Decimal("0.50")
    target_trade_risk_fraction: Decimal = Decimal("0.015")
    max_reservations: int = 3
    max_reserved_risk_fraction: Decimal = Decimal("0.05")
    daily_loss_fraction: Decimal = Decimal("0.06")
    weekly_loss_fraction: Decimal = Decimal("0.10")
    max_drawdown_fraction: Decimal = Decimal("0.15")
    pending_entry_ttl: timedelta = timedelta(hours=3)
    estimated_cost_rate: Decimal = Decimal("0.001")

    def __post_init__(self) -> None:
        fractions = (
            self.max_notional_equity_fraction,
            self.target_trade_risk_fraction,
            self.max_reserved_risk_fraction,
            self.daily_loss_fraction,
            self.weekly_loss_fraction,
            self.max_drawdown_fraction,
        )
        if self.leverage != Decimal("10"):
            raise ValueError("TraderRd risk policy requires 10x isolated leverage")
        if any(
            not value.is_finite() or value <= 0 or value >= 1
            for value in fractions
        ):
            raise ValueError("Risk policy fractions must be between zero and one")
        if self.max_reservations != 3:
            raise ValueError("TraderRd risk policy requires three reservations")
        if self.pending_entry_ttl != timedelta(hours=3):
            raise ValueError("TraderRd pending entries require a three-hour expiry")
        fixed_fractions = {
            "max notional": (
                self.max_notional_equity_fraction,
                Decimal("0.50"),
            ),
            "target trade risk": (
                self.target_trade_risk_fraction,
                Decimal("0.015"),
            ),
            "max reserved risk": (
                self.max_reserved_risk_fraction,
                Decimal("0.05"),
            ),
            "daily loss": (self.daily_loss_fraction, Decimal("0.06")),
            "weekly loss": (self.weekly_loss_fraction, Decimal("0.10")),
            "drawdown": (self.max_drawdown_fraction, Decimal("0.15")),
        }
        for label, (actual, expected) in fixed_fractions.items():
            if actual != expected:
                raise ValueError(f"TraderRd {label} policy is fixed at {expected}")
        if (
            not self.estimated_cost_rate.is_finite()
            or self.estimated_cost_rate < 0
            or self.estimated_cost_rate >= 1
        ):
            raise ValueError("Estimated cost rate must be non-negative and below one")


@dataclass(frozen=True, slots=True)
class TradeProposal:
    signal_id: str
    symbol: str
    direction: Direction
    entry: Decimal
    take_profit: Decimal
    stop_loss: Decimal
    entry_expires_at: datetime | None = None

    def __post_init__(self) -> None:
        if not self.signal_id:
            raise ValueError("Signal ID is required")
        if not self.symbol or self.symbol != self.symbol.upper():
            raise ValueError("Risk proposal symbol must be uppercase")
        if any(
            not value.is_finite() or value <= 0
            for value in (self.entry, self.take_profit, self.stop_loss)
        ):
            raise ValueError("Risk proposal prices must be positive")
        if self.direction is Direction.LONG and not (
            self.take_profit > self.entry > self.stop_loss
        ):
            raise ValueError("LONG risk proposal prices are inconsistent")
        if self.direction is Direction.SHORT and not (
            self.stop_loss > self.entry > self.take_profit
        ):
            raise ValueError("SHORT risk proposal prices are inconsistent")
        if self.entry_expires_at is not None and self.entry_expires_at.tzinfo is None:
            raise ValueError("Entry expiry must be timezone-aware")


@dataclass(frozen=True, slots=True)
class RiskReservation:
    reservation_id: str
    proposal: TradeProposal
    status: ReservationStatus
    quantity: Decimal
    notional: Decimal
    isolated_margin: Decimal
    reserved_risk: Decimal
    price_risk_rate: Decimal
    estimated_cost: Decimal
    created_at: datetime
    expires_at: datetime


@dataclass(frozen=True, slots=True)
class ReversalIntent:
    closing_reservation_id: str
    proposal: TradeProposal


@dataclass(frozen=True, slots=True)
class RiskAction:
    action_type: RiskActionType
    reservation_id: str
    symbol: str
    direction: Direction
    quantity: Decimal
    limit_price: Decimal | None = None
    expires_at: datetime | None = None
    reason: str | None = None


@dataclass(frozen=True, slots=True)
class RiskDecision:
    status: str
    reason: str
    actions: tuple[RiskAction, ...] = ()
    reservation_id: str | None = None
    projected_risk: Decimal | None = None
    notional: Decimal | None = None
    quantity: Decimal | None = None


@dataclass(frozen=True, slots=True)
class RiskEvent:
    event_type: str
    entity_id: str | None
    detail: str


@dataclass(slots=True)
class PortfolioState:
    policy: RiskPolicy
    mode: EngineMode
    as_of: datetime
    equity: Decimal
    high_watermark: Decimal
    daily_anchor: date
    daily_start_equity: Decimal
    weekly_anchor: date
    weekly_start_equity: Decimal
    daily_halted: bool = False
    weekly_halted: bool = False
    reservations: dict[str, RiskReservation] = field(default_factory=dict)
    reversal_intents: dict[str, ReversalIntent] = field(default_factory=dict)

    @property
    def active_reservations(self) -> list[RiskReservation]:
        active = {
            ReservationStatus.PENDING,
            ReservationStatus.FILLED,
            ReservationStatus.CLOSE_REQUESTED,
        }
        return [item for item in self.reservations.values() if item.status in active]

    @property
    def total_reserved_risk(self) -> Decimal:
        return sum(
            (item.reserved_risk for item in self.active_reservations),
            Decimal("0"),
        )


@dataclass(frozen=True, slots=True)
class InitializeRiskEngine:
    command_id: str
    occurred_at: datetime
    equity: Decimal
    estimated_cost_rate: Decimal = Decimal("0.001")


@dataclass(frozen=True, slots=True)
class ApplyEquitySnapshot:
    command_id: str
    occurred_at: datetime
    equity: Decimal


@dataclass(frozen=True, slots=True)
class ProposeRiskReservation:
    command_id: str
    occurred_at: datetime
    mark_equity: Decimal
    proposal: TradeProposal


@dataclass(frozen=True, slots=True)
class ConfirmPendingFill:
    command_id: str
    occurred_at: datetime
    reservation_id: str
    mark_equity: Decimal | None = None


@dataclass(frozen=True, slots=True)
class CancelPendingReservation:
    command_id: str
    occurred_at: datetime
    reservation_id: str
    reason: str = "operator_cancelled"
    mark_equity: Decimal | None = None


@dataclass(frozen=True, slots=True)
class ConfirmPositionClosed:
    command_id: str
    occurred_at: datetime
    reservation_id: str
    mark_equity: Decimal


@dataclass(frozen=True, slots=True)
class RequestPositionClose:
    command_id: str
    occurred_at: datetime
    reservation_id: str


@dataclass(frozen=True, slots=True)
class ManualRearm:
    command_id: str
    occurred_at: datetime
    equity: Decimal
    operator_reference: str


RiskCommand = (
    InitializeRiskEngine
    | ApplyEquitySnapshot
    | ProposeRiskReservation
    | ConfirmPendingFill
    | CancelPendingReservation
    | ConfirmPositionClosed
    | RequestPositionClose
    | ManualRearm
)


@dataclass(frozen=True, slots=True)
class RiskTransition:
    state: PortfolioState
    decision: RiskDecision
    events: tuple[RiskEvent, ...]


class PortfolioRiskEngine:
    def process(
        self,
        state: PortfolioState | None,
        command: RiskCommand,
    ) -> RiskTransition:
        self._validate_command(command)
        if isinstance(command, InitializeRiskEngine):
            return self._initialize(state, command)
        if state is None:
            raise ValueError("Risk engine must be initialized first")
        if command.occurred_at < state.as_of:
            raise ValueError("Risk commands must be chronological")

        actions, events = self._advance_time(state, command.occurred_at)
        if isinstance(command, ApplyEquitySnapshot):
            circuit_actions, circuit_events = self._apply_snapshot(
                state, command.equity, command.occurred_at
            )
            actions.extend(circuit_actions)
            events.extend(circuit_events)
            return self._transition(
                state, "accepted", "snapshot_applied", actions, events
            )
        if isinstance(command, ProposeRiskReservation):
            circuit_actions, circuit_events = self._apply_snapshot(
                state, command.mark_equity, command.occurred_at
            )
            actions.extend(circuit_actions)
            events.extend(circuit_events)
            decision, new_events = self._propose(state, command.proposal, actions)
            events.extend(new_events)
            return RiskTransition(state, decision, tuple(events))
        if isinstance(command, (ConfirmPendingFill, CancelPendingReservation)) and command.mark_equity is not None:
            circuit_actions, circuit_events = self._apply_snapshot(state, command.mark_equity, command.occurred_at)
            actions.extend(circuit_actions)
            events.extend(circuit_events)
        if isinstance(command, ConfirmPendingFill):
            decision, new_events = self._confirm_fill(state, command.reservation_id)
        elif isinstance(command, CancelPendingReservation):
            decision, new_events = self._cancel_pending(
                state, command.reservation_id, command.reason, actions
            )
        elif isinstance(command, ConfirmPositionClosed):
            circuit_actions, circuit_events = self._apply_snapshot(
                state, command.mark_equity, command.occurred_at
            )
            actions.extend(circuit_actions)
            events.extend(circuit_events)
            decision, new_events = self._confirm_close(
                state, command.reservation_id, actions
            )
        elif isinstance(command, RequestPositionClose):
            reservation = state.reservations.get(command.reservation_id)
            if reservation is None or reservation.status not in {ReservationStatus.FILLED, ReservationStatus.CLOSE_REQUESTED}:
                decision, new_events = self._rejected("reservation_not_filled"), []
            else:
                state.reservations[reservation.reservation_id] = replace(reservation, status=ReservationStatus.CLOSE_REQUESTED)
                state.reversal_intents.pop(reservation.proposal.symbol, None)
                decision = RiskDecision("accepted", "operator_close_requested", (self._close_action(reservation, "operator_requested"),))
                new_events = [RiskEvent("operator_close_requested", reservation.reservation_id, "explicit_owned_close")]
        else:
            decision, new_events = self._manual_rearm(state, command)
        events.extend(new_events)
        if actions:
            decision = replace(decision, actions=tuple(dict.fromkeys((*actions, *decision.actions))))
        return RiskTransition(state, decision, tuple(events))

    def _initialize(
        self,
        state: PortfolioState | None,
        command: InitializeRiskEngine,
    ) -> RiskTransition:
        if state is not None:
            raise ValueError("Risk engine is already initialized")
        if not command.equity.is_finite() or command.equity <= 0:
            raise ValueError("Initial equity must be positive")
        local = command.occurred_at.astimezone(LIMA)
        policy = RiskPolicy(estimated_cost_rate=command.estimated_cost_rate)
        initialized = PortfolioState(
            policy=policy,
            mode=EngineMode.ACTIVE,
            as_of=command.occurred_at,
            equity=command.equity,
            high_watermark=command.equity,
            daily_anchor=local.date(),
            daily_start_equity=command.equity,
            weekly_anchor=_week_start(local.date()),
            weekly_start_equity=command.equity,
        )
        return RiskTransition(
            initialized,
            RiskDecision("accepted", "engine_initialized"),
            (RiskEvent("engine_initialized", None, "manual_equity_input"),),
        )

    def _advance_time(
        self,
        state: PortfolioState,
        occurred_at: datetime,
    ) -> tuple[list[RiskAction], list[RiskEvent]]:
        state.as_of = occurred_at
        actions: list[RiskAction] = []
        events: list[RiskEvent] = []
        for reservation in state.active_reservations:
            if (
                reservation.status is ReservationStatus.PENDING
                and reservation.expires_at <= occurred_at
            ):
                # Expiry requests cancellation; only exchange confirmation can
                # release the reservation. A fill may race with cancellation.
                actions.append(self._cancel_action(reservation, "entry_expired"))
                events.append(
                    RiskEvent(
                        "pending_expired",
                        reservation.reservation_id,
                        "three_hour_expiry",
                    )
                )
        return actions, events

    def _apply_snapshot(
        self,
        state: PortfolioState,
        equity: Decimal,
        occurred_at: datetime,
    ) -> tuple[list[RiskAction], list[RiskEvent]]:
        if not equity.is_finite() or equity <= 0:
            raise ValueError("Mark-to-market equity must be positive")
        local_date = occurred_at.astimezone(LIMA).date()
        week = _week_start(local_date)
        events: list[RiskEvent] = []
        if local_date != state.daily_anchor:
            state.daily_anchor = local_date
            state.daily_start_equity = equity
            state.daily_halted = False
            events.append(RiskEvent("daily_period_reset", None, local_date.isoformat()))
        if week != state.weekly_anchor:
            state.weekly_anchor = week
            state.weekly_start_equity = equity
            state.weekly_halted = False
            events.append(RiskEvent("weekly_period_reset", None, week.isoformat()))
        state.equity = equity
        state.high_watermark = max(state.high_watermark, equity)

        if state.mode is EngineMode.KILLED:
            return [], events
        drawdown = _loss_fraction(state.high_watermark, equity)
        if drawdown >= state.policy.max_drawdown_fraction:
            actions, trip_events = self._trip_kill_switch(state)
            events.extend(trip_events)
            return actions, events

        daily_loss = _loss_fraction(state.daily_start_equity, equity)
        weekly_loss = _loss_fraction(state.weekly_start_equity, equity)
        if daily_loss >= state.policy.daily_loss_fraction:
            state.daily_halted = True
        if weekly_loss >= state.policy.weekly_loss_fraction:
            state.weekly_halted = True
        if state.daily_halted or state.weekly_halted:
            actions = self._pause_entries(state)
            state.mode = EngineMode.ENTRY_PAUSED
            reason = "daily_weekly_entry_pause"
            events.append(RiskEvent("entry_pause_activated", None, reason))
            return actions, events
        state.mode = EngineMode.ACTIVE
        return [], events

    def _propose(
        self,
        state: PortfolioState,
        proposal: TradeProposal,
        actions: list[RiskAction],
    ) -> tuple[RiskDecision, list[RiskEvent]]:
        events: list[RiskEvent] = []
        if state.mode is not EngineMode.ACTIVE:
            return self._rejected(state.mode.value, actions), events
        if proposal.symbol in state.reversal_intents:
            return self._rejected("inverse_confirmation_already_pending", actions), events
        existing_signal = state.reservations.get(proposal.signal_id)
        if existing_signal is not None:
            return self._rejected("duplicate_signal", actions), events

        same_symbol = [
            item
            for item in state.active_reservations
            if item.proposal.symbol == proposal.symbol
        ]
        for reservation in same_symbol:
            if reservation.proposal.direction is proposal.direction:
                return self._rejected("same_symbol_exposure_exists", actions), events

        filled_opposite = next(
            (
                item
                for item in same_symbol
                if item.status
                in {ReservationStatus.FILLED, ReservationStatus.CLOSE_REQUESTED}
            ),
            None,
        )
        if filled_opposite is not None:
            if filled_opposite.status is ReservationStatus.CLOSE_REQUESTED:
                return self._rejected("inverse_close_already_pending", actions), events
            if filled_opposite.status is ReservationStatus.FILLED:
                state.reservations[filled_opposite.reservation_id] = replace(
                    filled_opposite, status=ReservationStatus.CLOSE_REQUESTED
                )
                actions.append(self._close_action(filled_opposite, "inverse_signal"))
                events.append(
                    RiskEvent(
                        "inverse_close_requested",
                        filled_opposite.reservation_id,
                        proposal.signal_id,
                    )
                )
            state.reversal_intents[proposal.symbol] = ReversalIntent(
                filled_opposite.reservation_id, proposal
            )
            return (
                RiskDecision(
                    "staged",
                    "close_confirmation_required_before_inverse_entry",
                    tuple(actions),
                ),
                events,
            )

        for reservation in same_symbol:
            if reservation.status is ReservationStatus.PENDING:
                state.reversal_intents[proposal.symbol] = ReversalIntent(
                    reservation.reservation_id,
                    replace(proposal, entry_expires_at=proposal.entry_expires_at or (
                        state.as_of + state.policy.pending_entry_ttl)),
                )
                actions.append(self._cancel_action(reservation, "inverse_signal"))
                return RiskDecision(
                    "staged", "cancel_confirmation_required_before_inverse_entry", tuple(actions)
                ), [RiskEvent("inverse_cancel_requested", reservation.reservation_id, proposal.signal_id)]
        decision, admission_events = self._admit(state, proposal, actions)
        events.extend(admission_events)
        return decision, events

    def _admit(
        self,
        state: PortfolioState,
        proposal: TradeProposal,
        actions: list[RiskAction],
    ) -> tuple[RiskDecision, list[RiskEvent]]:
        if state.mode is not EngineMode.ACTIVE:
            return self._rejected(state.mode.value, actions), []
        if len(state.active_reservations) >= state.policy.max_reservations:
            return self._rejected("max_reservations", actions), []

        price_risk_rate = abs(proposal.entry - proposal.stop_loss) / proposal.entry
        combined_rate = price_risk_rate + state.policy.estimated_cost_rate
        target_risk = state.equity * state.policy.target_trade_risk_fraction
        risk_sized_notional = target_risk / combined_rate
        capped_notional = state.equity * state.policy.max_notional_equity_fraction
        notional = min(risk_sized_notional, capped_notional)
        estimated_cost = notional * state.policy.estimated_cost_rate
        projected_risk = notional * price_risk_rate + estimated_cost
        projected_total = state.total_reserved_risk + projected_risk

        if projected_total > state.equity * state.policy.max_reserved_risk_fraction:
            return self._rejected("max_total_reserved_risk", actions), []
        projected_equity = state.equity - projected_risk
        if _loss_fraction(state.daily_start_equity, projected_equity) >= (
            state.policy.daily_loss_fraction
        ):
            return self._rejected("projected_daily_loss_limit", actions), []
        if _loss_fraction(state.weekly_start_equity, projected_equity) >= (
            state.policy.weekly_loss_fraction
        ):
            return self._rejected("projected_weekly_loss_limit", actions), []
        if _loss_fraction(state.high_watermark, projected_equity) >= (
            state.policy.max_drawdown_fraction
        ):
            return self._rejected("projected_drawdown_limit", actions), []

        expires_at = proposal.entry_expires_at or (
            state.as_of + state.policy.pending_entry_ttl
        )
        if expires_at <= state.as_of:
            return self._rejected("entry_window_expired", actions), []
        if expires_at > state.as_of + state.policy.pending_entry_ttl:
            return self._rejected("entry_window_exceeds_policy", actions), []

        reservation = RiskReservation(
            reservation_id=proposal.signal_id,
            proposal=proposal,
            status=ReservationStatus.PENDING,
            quantity=notional / proposal.entry,
            notional=notional,
            isolated_margin=notional / state.policy.leverage,
            reserved_risk=projected_risk,
            price_risk_rate=price_risk_rate,
            estimated_cost=estimated_cost,
            created_at=state.as_of,
            expires_at=expires_at,
        )
        state.reservations[reservation.reservation_id] = reservation
        actions.append(
            RiskAction(
                RiskActionType.CREATE_PENDING_POST_ONLY,
                reservation.reservation_id,
                proposal.symbol,
                proposal.direction,
                reservation.quantity,
                limit_price=proposal.entry,
                expires_at=reservation.expires_at,
                reason="risk_approved",
            )
        )
        return (
            RiskDecision(
                "accepted",
                "risk_approved",
                tuple(actions),
                reservation.reservation_id,
                projected_risk,
                notional,
                reservation.quantity,
            ),
            (
                RiskEvent(
                    "reservation_created",
                    reservation.reservation_id,
                    canonical_decimal(projected_risk),
                ),
            ),
        )

    def _confirm_fill(
        self, state: PortfolioState, reservation_id: str
    ) -> tuple[RiskDecision, list[RiskEvent]]:
        reservation = state.reservations.get(reservation_id)
        if reservation is None:
            return self._rejected("reservation_not_found"), []
        if reservation.status in {ReservationStatus.FILLED, ReservationStatus.CLOSE_REQUESTED}:
            return RiskDecision("duplicate", "fill_already_confirmed"), []
        if reservation.status is not ReservationStatus.PENDING:
            return self._rejected("reservation_not_pending"), []
        reversal = state.reversal_intents.get(reservation.proposal.symbol)
        must_close = state.mode is EngineMode.KILLED or (
            reversal is not None and reversal.closing_reservation_id == reservation_id
        )
        state.reservations[reservation_id] = replace(
            reservation,
            status=ReservationStatus.CLOSE_REQUESTED if must_close else ReservationStatus.FILLED,
        )
        actions = (self._close_action(reservation, "fill_after_cancel_request"),) if must_close else ()
        return (
            RiskDecision("accepted", "fill_confirmed", actions, reservation_id=reservation_id),
            [RiskEvent("position_filled", reservation_id, "exchange_confirmation")],
        )

    def _cancel_pending(
        self,
        state: PortfolioState,
        reservation_id: str,
        reason: str,
        actions: list[RiskAction],
    ) -> tuple[RiskDecision, list[RiskEvent]]:
        reservation = state.reservations.get(reservation_id)
        if reservation is None:
            return self._rejected("reservation_not_found", actions), []
        if reservation.status in {
            ReservationStatus.CANCELLED,
            ReservationStatus.EXPIRED,
        }:
            return (
                RiskDecision("duplicate", "pending_already_inactive", tuple(actions)),
                [],
            )
        if reservation.status is not ReservationStatus.PENDING:
            return self._rejected("reservation_not_pending", actions), []
        state.reservations[reservation_id] = replace(
            reservation, status=ReservationStatus.CANCELLED
        )
        events = [RiskEvent("pending_cancelled", reservation_id, reason)]
        return self._complete_reversal(state, reservation, actions, events, "pending_cancelled")

    def _confirm_close(
        self,
        state: PortfolioState,
        reservation_id: str,
        actions: list[RiskAction],
    ) -> tuple[RiskDecision, list[RiskEvent]]:
        reservation = state.reservations.get(reservation_id)
        if reservation is None:
            return self._rejected("reservation_not_found", actions), []
        if reservation.status is ReservationStatus.CLOSED:
            return (
                RiskDecision("duplicate", "close_already_confirmed", tuple(actions)),
                [],
            )
        if reservation.status not in {
            ReservationStatus.FILLED,
            ReservationStatus.CLOSE_REQUESTED,
        }:
            return self._rejected("reservation_not_filled", actions), []
        state.reservations[reservation_id] = replace(
            reservation, status=ReservationStatus.CLOSED
        )
        events = [RiskEvent("position_closed", reservation_id, "exchange_confirmation")]
        return self._complete_reversal(state, reservation, actions, events, "close_confirmed")

    def _complete_reversal(self, state, reservation, actions, events, reason):
        intent = state.reversal_intents.get(reservation.proposal.symbol)
        if intent is None or intent.closing_reservation_id != reservation.reservation_id:
            return RiskDecision("accepted", reason, tuple(actions)), events
        state.reversal_intents.pop(reservation.proposal.symbol)
        if state.mode is EngineMode.ACTIVE:
            decision, admission_events = self._admit(state, intent.proposal, actions)
            events.extend(admission_events)
            if decision.status == "accepted":
                return decision, events
            events.append(RiskEvent("inverse_entry_not_admitted", intent.proposal.signal_id, decision.reason))
        # Cancellation/closure succeeded even when the opposite proposal no
        # longer passes admission. Never strand the completed exchange handoff.
        return RiskDecision("accepted", reason, tuple(actions)), events

    def _manual_rearm(
        self, state: PortfolioState, command: ManualRearm
    ) -> tuple[RiskDecision, list[RiskEvent]]:
        if state.mode is not EngineMode.KILLED:
            return self._rejected("kill_switch_not_active"), []
        if not command.operator_reference.strip():
            return self._rejected("operator_reference_required"), []
        if state.active_reservations:
            return self._rejected("positions_must_be_closed_before_rearm"), []
        if not command.equity.is_finite() or command.equity <= 0:
            raise ValueError("Manual rearm equity must be positive")
        local = command.occurred_at.astimezone(LIMA).date()
        state.mode = EngineMode.ACTIVE
        state.equity = command.equity
        state.high_watermark = command.equity
        state.daily_anchor = local
        state.daily_start_equity = command.equity
        state.weekly_anchor = _week_start(local)
        state.weekly_start_equity = command.equity
        state.daily_halted = False
        state.weekly_halted = False
        return (
            RiskDecision("accepted", "manual_rearm_completed"),
            [RiskEvent("manual_rearm", None, command.operator_reference)],
        )

    def _trip_kill_switch(
        self, state: PortfolioState
    ) -> tuple[list[RiskAction], list[RiskEvent]]:
        state.mode = EngineMode.KILLED
        state.daily_halted = True
        state.weekly_halted = True
        state.reversal_intents.clear()
        actions: list[RiskAction] = []
        events = [RiskEvent("kill_switch_tripped", None, "fifteen_percent_drawdown")]
        for reservation in state.active_reservations:
            if reservation.status is ReservationStatus.PENDING:
                actions.append(self._cancel_action(reservation, "drawdown_kill_switch"))
            elif reservation.status is ReservationStatus.FILLED:
                state.reservations[reservation.reservation_id] = replace(
                    reservation, status=ReservationStatus.CLOSE_REQUESTED
                )
                actions.append(self._close_action(reservation, "drawdown_kill_switch"))
        return actions, events

    def _pause_entries(self, state: PortfolioState) -> list[RiskAction]:
        state.reversal_intents.clear()
        actions: list[RiskAction] = []
        for reservation in state.active_reservations:
            if reservation.status is ReservationStatus.PENDING:
                actions.append(self._cancel_action(reservation, "period_loss_limit"))
        return actions

    @staticmethod
    def _cancel_action(reservation: RiskReservation, reason: str) -> RiskAction:
        return RiskAction(
            RiskActionType.CANCEL_PENDING,
            reservation.reservation_id,
            reservation.proposal.symbol,
            reservation.proposal.direction,
            reservation.quantity,
            reason=reason,
        )

    @staticmethod
    def _close_action(reservation: RiskReservation, reason: str) -> RiskAction:
        return RiskAction(
            RiskActionType.REQUEST_CLOSE_POSITION,
            reservation.reservation_id,
            reservation.proposal.symbol,
            reservation.proposal.direction,
            reservation.quantity,
            reason=reason,
        )

    @staticmethod
    def _rejected(
        reason: str, actions: list[RiskAction] | None = None
    ) -> RiskDecision:
        return RiskDecision("rejected", reason, tuple(actions or ()))

    @staticmethod
    def _transition(
        state: PortfolioState,
        status: str,
        reason: str,
        actions: list[RiskAction],
        events: list[RiskEvent],
    ) -> RiskTransition:
        return RiskTransition(
            state,
            RiskDecision(status, reason, tuple(actions)),
            tuple(events),
        )

    @staticmethod
    def _validate_command(command: RiskCommand) -> None:
        if not command.command_id:
            raise ValueError("Risk command ID is required")
        if command.occurred_at.tzinfo is None:
            raise ValueError("Risk command timestamp must be timezone-aware")


def _week_start(value: date) -> date:
    return value - timedelta(days=value.weekday())


def _loss_fraction(reference: Decimal, current: Decimal) -> Decimal:
    if current >= reference:
        return Decimal("0")
    return (reference - current) / reference
