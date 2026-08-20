from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal
import hashlib
import json
from typing import Any, Protocol

from traderrd.domain.models import Direction, canonical_decimal
from traderrd.domain.risk import (
    ApplyEquitySnapshot,
    CancelPendingReservation,
    ConfirmPendingFill,
    ConfirmPositionClosed,
    EngineMode,
    InitializeRiskEngine,
    ManualRearm,
    PortfolioRiskEngine,
    PortfolioState,
    ProposeRiskReservation,
    ReservationStatus,
    ReversalIntent,
    RiskAction,
    RiskCommand,
    RiskDecision,
    RiskEvent,
    RiskPolicy,
    RiskReservation,
    TradeProposal,
)


class RiskStateRepository(Protocol):
    def execute(
        self,
        command: RiskCommand,
        engine: PortfolioRiskEngine,
    ) -> "RiskExecutionResult": ...

    def load_state(self) -> PortfolioState | None: ...


@dataclass(frozen=True, slots=True)
class RiskExecutionResult:
    decision: RiskDecision
    state: PortfolioState
    events: tuple[RiskEvent, ...]
    duplicate_command: bool = False


class RiskEngineService:
    def __init__(
        self,
        repository: RiskStateRepository,
        engine: PortfolioRiskEngine | None = None,
    ) -> None:
        self._repository = repository
        self._engine = engine or PortfolioRiskEngine()

    def execute(self, command: RiskCommand) -> RiskExecutionResult:
        return self._repository.execute(command, self._engine)

    def report(self) -> PortfolioState | None:
        return self._repository.load_state()


class InMemoryRiskSession:
    def __init__(self) -> None:
        self.state: PortfolioState | None = None
        self.engine = PortfolioRiskEngine()
        self.results: dict[str, tuple[str, RiskDecision]] = {}

    def execute(self, command: RiskCommand) -> RiskExecutionResult:
        digest = command_digest(command)
        existing = self.results.get(command.command_id)
        if existing is not None:
            if existing[0] != digest:
                raise ValueError("Risk command ID was reused with different input")
            if self.state is None:
                raise ValueError("Risk engine state is unavailable")
            return RiskExecutionResult(existing[1], self.state, (), True)
        transition = self.engine.process(self.state, command)
        self.state = transition.state
        self.results[command.command_id] = (digest, transition.decision)
        return RiskExecutionResult(
            transition.decision,
            transition.state,
            transition.events,
        )


def command_from_dict(payload: dict[str, Any]) -> RiskCommand:
    command_type = _required_text(payload, "type")
    common = {
        "command_id": _required_text(payload, "command_id"),
        "occurred_at": _datetime(payload, "occurred_at"),
    }
    if command_type == "initialize":
        return InitializeRiskEngine(
            **common,
            equity=_decimal(payload, "equity"),
            estimated_cost_rate=Decimal(
                str(payload.get("estimated_cost_rate", "0.001"))
            ),
        )
    if command_type == "snapshot":
        return ApplyEquitySnapshot(**common, equity=_decimal(payload, "equity"))
    if command_type == "propose":
        return ProposeRiskReservation(
            **common,
            mark_equity=_decimal(payload, "mark_equity"),
            proposal=TradeProposal(
                signal_id=_required_text(payload, "signal_id"),
                symbol=_required_text(payload, "symbol").upper(),
                direction=Direction(_required_text(payload, "direction").upper()),
                entry=_decimal(payload, "entry"),
                take_profit=_decimal(payload, "take_profit"),
                stop_loss=_decimal(payload, "stop_loss"),
                entry_expires_at=(
                    _datetime(payload, "entry_expires_at")
                    if payload.get("entry_expires_at") is not None
                    else None
                ),
            ),
        )
    if command_type == "confirm_fill":
        return ConfirmPendingFill(
            **common,
            reservation_id=_required_text(payload, "reservation_id"),
        )
    if command_type == "cancel_pending":
        return CancelPendingReservation(
            **common,
            reservation_id=_required_text(payload, "reservation_id"),
            reason=str(payload.get("reason", "operator_cancelled")),
        )
    if command_type == "confirm_close":
        return ConfirmPositionClosed(
            **common,
            reservation_id=_required_text(payload, "reservation_id"),
            mark_equity=_decimal(payload, "mark_equity"),
        )
    if command_type == "manual_rearm":
        return ManualRearm(
            **common,
            equity=_decimal(payload, "equity"),
            operator_reference=_required_text(payload, "operator_reference"),
        )
    raise ValueError(f"Unsupported risk command type: {command_type}")


def command_to_dict(command: RiskCommand) -> dict[str, Any]:
    if isinstance(command, InitializeRiskEngine):
        return {
            "type": "initialize",
            "command_id": command.command_id,
            "occurred_at": command.occurred_at.isoformat(),
            "equity": canonical_decimal(command.equity),
            "estimated_cost_rate": canonical_decimal(command.estimated_cost_rate),
        }
    if isinstance(command, ApplyEquitySnapshot):
        return _simple_command(command, "snapshot", equity=command.equity)
    if isinstance(command, ProposeRiskReservation):
        proposal = command.proposal
        return _simple_command(
            command,
            "propose",
            mark_equity=command.mark_equity,
            signal_id=proposal.signal_id,
            symbol=proposal.symbol,
            direction=proposal.direction.value,
            entry=proposal.entry,
            take_profit=proposal.take_profit,
            stop_loss=proposal.stop_loss,
            entry_expires_at=(
                proposal.entry_expires_at.isoformat()
                if proposal.entry_expires_at
                else None
            ),
        )
    if isinstance(command, ConfirmPendingFill):
        return _simple_command(
            command, "confirm_fill", reservation_id=command.reservation_id
        )
    if isinstance(command, CancelPendingReservation):
        return _simple_command(
            command,
            "cancel_pending",
            reservation_id=command.reservation_id,
            reason=command.reason,
        )
    if isinstance(command, ConfirmPositionClosed):
        return _simple_command(
            command,
            "confirm_close",
            reservation_id=command.reservation_id,
            mark_equity=command.mark_equity,
        )
    return _simple_command(
        command,
        "manual_rearm",
        equity=command.equity,
        operator_reference=command.operator_reference,
    )


def command_digest(command: RiskCommand) -> str:
    encoded = json.dumps(
        command_to_dict(command), sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def state_to_dict(state: PortfolioState) -> dict[str, Any]:
    policy = state.policy
    return {
        "policy": {
            "leverage": canonical_decimal(policy.leverage),
            "max_notional_equity_fraction": canonical_decimal(
                policy.max_notional_equity_fraction
            ),
            "target_trade_risk_fraction": canonical_decimal(
                policy.target_trade_risk_fraction
            ),
            "max_reservations": policy.max_reservations,
            "max_reserved_risk_fraction": canonical_decimal(
                policy.max_reserved_risk_fraction
            ),
            "daily_loss_fraction": canonical_decimal(policy.daily_loss_fraction),
            "weekly_loss_fraction": canonical_decimal(policy.weekly_loss_fraction),
            "max_drawdown_fraction": canonical_decimal(
                policy.max_drawdown_fraction
            ),
            "pending_entry_ttl_seconds": int(policy.pending_entry_ttl.total_seconds()),
            "estimated_cost_rate": canonical_decimal(policy.estimated_cost_rate),
        },
        "mode": state.mode.value,
        "as_of": state.as_of.isoformat(),
        "equity": canonical_decimal(state.equity),
        "high_watermark": canonical_decimal(state.high_watermark),
        "daily_anchor": state.daily_anchor.isoformat(),
        "daily_start_equity": canonical_decimal(state.daily_start_equity),
        "weekly_anchor": state.weekly_anchor.isoformat(),
        "weekly_start_equity": canonical_decimal(state.weekly_start_equity),
        "daily_halted": state.daily_halted,
        "weekly_halted": state.weekly_halted,
        "reservations": {
            key: _reservation_to_dict(value)
            for key, value in sorted(state.reservations.items())
        },
        "reversal_intents": {
            key: {
                "closing_reservation_id": value.closing_reservation_id,
                "proposal": _proposal_to_dict(value.proposal),
            }
            for key, value in sorted(state.reversal_intents.items())
        },
    }


def state_from_dict(payload: dict[str, Any]) -> PortfolioState:
    policy_data = payload["policy"]
    policy = RiskPolicy(
        leverage=Decimal(policy_data["leverage"]),
        max_notional_equity_fraction=Decimal(
            policy_data["max_notional_equity_fraction"]
        ),
        target_trade_risk_fraction=Decimal(policy_data["target_trade_risk_fraction"]),
        max_reservations=int(policy_data["max_reservations"]),
        max_reserved_risk_fraction=Decimal(policy_data["max_reserved_risk_fraction"]),
        daily_loss_fraction=Decimal(policy_data["daily_loss_fraction"]),
        weekly_loss_fraction=Decimal(policy_data["weekly_loss_fraction"]),
        max_drawdown_fraction=Decimal(policy_data["max_drawdown_fraction"]),
        pending_entry_ttl=timedelta(
            seconds=int(policy_data["pending_entry_ttl_seconds"])
        ),
        estimated_cost_rate=Decimal(policy_data["estimated_cost_rate"]),
    )
    return PortfolioState(
        policy=policy,
        mode=EngineMode(payload["mode"]),
        as_of=datetime.fromisoformat(payload["as_of"]),
        equity=Decimal(payload["equity"]),
        high_watermark=Decimal(payload["high_watermark"]),
        daily_anchor=date.fromisoformat(payload["daily_anchor"]),
        daily_start_equity=Decimal(payload["daily_start_equity"]),
        weekly_anchor=date.fromisoformat(payload["weekly_anchor"]),
        weekly_start_equity=Decimal(payload["weekly_start_equity"]),
        daily_halted=bool(payload["daily_halted"]),
        weekly_halted=bool(payload["weekly_halted"]),
        reservations={
            key: _reservation_from_dict(value)
            for key, value in payload["reservations"].items()
        },
        reversal_intents={
            key: ReversalIntent(
                value["closing_reservation_id"],
                _proposal_from_dict(value["proposal"]),
            )
            for key, value in payload["reversal_intents"].items()
        },
    )


def decision_to_dict(decision: RiskDecision) -> dict[str, Any]:
    return {
        "status": decision.status,
        "reason": decision.reason,
        "actions": [_action_to_dict(value) for value in decision.actions],
        "reservation_id": decision.reservation_id,
        "projected_risk": _optional_decimal(decision.projected_risk),
        "notional": _optional_decimal(decision.notional),
        "quantity": _optional_decimal(decision.quantity),
    }


def decision_from_dict(payload: dict[str, Any]) -> RiskDecision:
    from traderrd.domain.risk import RiskActionType

    actions = tuple(
        RiskAction(
            action_type=RiskActionType(value["action_type"]),
            reservation_id=value["reservation_id"],
            symbol=value["symbol"],
            direction=Direction(value["direction"]),
            quantity=Decimal(value["quantity"]),
            limit_price=(
                Decimal(value["limit_price"])
                if value["limit_price"]
                else None
            ),
            expires_at=(
                datetime.fromisoformat(value["expires_at"])
                if value["expires_at"]
                else None
            ),
            reason=value["reason"],
        )
        for value in payload["actions"]
    )
    return RiskDecision(
        status=payload["status"],
        reason=payload["reason"],
        actions=actions,
        reservation_id=payload["reservation_id"],
        projected_risk=_read_optional_decimal(payload["projected_risk"]),
        notional=_read_optional_decimal(payload["notional"]),
        quantity=_read_optional_decimal(payload["quantity"]),
    )


def _simple_command(command: Any, command_type: str, **values: Any) -> dict[str, Any]:
    result: dict[str, Any] = {
        "type": command_type,
        "command_id": command.command_id,
        "occurred_at": command.occurred_at.isoformat(),
    }
    for key, value in values.items():
        result[key] = canonical_decimal(value) if isinstance(value, Decimal) else value
    return result


def _reservation_to_dict(value: RiskReservation) -> dict[str, Any]:
    return {
        "reservation_id": value.reservation_id,
        "proposal": _proposal_to_dict(value.proposal),
        "status": value.status.value,
        "quantity": canonical_decimal(value.quantity),
        "notional": canonical_decimal(value.notional),
        "isolated_margin": canonical_decimal(value.isolated_margin),
        "reserved_risk": canonical_decimal(value.reserved_risk),
        "price_risk_rate": canonical_decimal(value.price_risk_rate),
        "estimated_cost": canonical_decimal(value.estimated_cost),
        "created_at": value.created_at.isoformat(),
        "expires_at": value.expires_at.isoformat(),
    }


def _reservation_from_dict(value: dict[str, Any]) -> RiskReservation:
    return RiskReservation(
        reservation_id=value["reservation_id"],
        proposal=_proposal_from_dict(value["proposal"]),
        status=ReservationStatus(value["status"]),
        quantity=Decimal(value["quantity"]),
        notional=Decimal(value["notional"]),
        isolated_margin=Decimal(value["isolated_margin"]),
        reserved_risk=Decimal(value["reserved_risk"]),
        price_risk_rate=Decimal(value["price_risk_rate"]),
        estimated_cost=Decimal(value["estimated_cost"]),
        created_at=datetime.fromisoformat(value["created_at"]),
        expires_at=datetime.fromisoformat(value["expires_at"]),
    )


def _proposal_to_dict(value: TradeProposal) -> dict[str, Any]:
    return {
        "signal_id": value.signal_id,
        "symbol": value.symbol,
        "direction": value.direction.value,
        "entry": canonical_decimal(value.entry),
        "take_profit": canonical_decimal(value.take_profit),
        "stop_loss": canonical_decimal(value.stop_loss),
        "entry_expires_at": (
            value.entry_expires_at.isoformat() if value.entry_expires_at else None
        ),
    }


def _proposal_from_dict(value: dict[str, Any]) -> TradeProposal:
    return TradeProposal(
        signal_id=value["signal_id"],
        symbol=value["symbol"],
        direction=Direction(value["direction"]),
        entry=Decimal(value["entry"]),
        take_profit=Decimal(value["take_profit"]),
        stop_loss=Decimal(value["stop_loss"]),
        entry_expires_at=(
            datetime.fromisoformat(value["entry_expires_at"])
            if value.get("entry_expires_at")
            else None
        ),
    )


def _action_to_dict(value: RiskAction) -> dict[str, Any]:
    return {
        "action_type": value.action_type.value,
        "reservation_id": value.reservation_id,
        "symbol": value.symbol,
        "direction": value.direction.value,
        "quantity": canonical_decimal(value.quantity),
        "limit_price": _optional_decimal(value.limit_price),
        "expires_at": value.expires_at.isoformat() if value.expires_at else None,
        "reason": value.reason,
    }


def _required_text(payload: dict[str, Any], key: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"Risk command field {key} must be non-empty text")
    return value.strip()


def _decimal(payload: dict[str, Any], key: str) -> Decimal:
    if key not in payload:
        raise ValueError(f"Risk command field {key} is required")
    return Decimal(str(payload[key]))


def _datetime(payload: dict[str, Any], key: str) -> datetime:
    value = datetime.fromisoformat(_required_text(payload, key))
    if value.tzinfo is None:
        raise ValueError(f"Risk command field {key} must include a timezone")
    return value


def _optional_decimal(value: Decimal | None) -> str | None:
    return canonical_decimal(value) if value is not None else None


def _read_optional_decimal(value: str | None) -> Decimal | None:
    return Decimal(value) if value is not None else None
