from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR
from enum import StrEnum

from traderrd.domain.models import Direction


class ExecutionIntentKind(StrEnum):
    ENTRY = "entry"
    CANCEL_ENTRY = "cancel_entry"
    CLOSE_POSITION = "close_position"
    SET_TRADING_STOP = "set_trading_stop"


class ExecutionIntentState(StrEnum):
    PLANNED = "planned"
    ACKNOWLEDGED = "acknowledged"
    WORKING = "working"
    FILLED = "filled"
    CANCELLED = "cancelled"
    REJECTED = "rejected"
    CLOSE_CONFIRMED = "close_confirmed"
    PROTECTION_VERIFIED = "protection_verified"
    POSITION_CLOSED_PENDING = "position_closed_pending"
    POSITION_CLOSED = "position_closed"
    RECONCILIATION_REQUIRED = "reconciliation_required"


class FeeVerificationStatus(StrEnum):
    UNAVAILABLE = "unavailable"


@dataclass(frozen=True, slots=True)
class InstrumentRules:
    symbol: str
    tick_size: Decimal
    quantity_step: Decimal
    min_quantity: Decimal
    max_quantity: Decimal
    min_notional: Decimal

    def __post_init__(self) -> None:
        values = (
            self.tick_size,
            self.quantity_step,
            self.min_quantity,
            self.max_quantity,
            self.min_notional,
        )
        if not self.symbol or self.symbol != self.symbol.upper():
            raise ValueError("Instrument symbol must be uppercase")
        if any(not value.is_finite() or value <= 0 for value in values):
            raise ValueError("Instrument constraints must be finite and positive")
        if self.max_quantity < self.min_quantity:
            raise ValueError("Instrument maximum quantity is inconsistent")

    def normalize_quantity(self, quantity: Decimal, price: Decimal) -> Decimal:
        normalized = _floor_increment(quantity, self.quantity_step)
        if normalized < self.min_quantity or normalized > self.max_quantity:
            raise ValueError("Quantity is outside instrument limits")
        if normalized * price < self.min_notional:
            raise ValueError("Order is below minimum notional")
        return normalized

    def normalize_entry_price(
        self,
        price: Decimal,
        direction: Direction,
    ) -> Decimal:
        if direction is Direction.LONG:
            return _floor_increment(price, self.tick_size)
        return _ceiling_increment(price, self.tick_size)


@dataclass(frozen=True, slots=True)
class DemoPreflightReport:
    symbol: str
    isolated_margin: bool
    one_way_mode: bool
    available_balance: Decimal
    fee_verification: FeeVerificationStatus
    instrument: InstrumentRules
    server_time_offset_ms: int
    checked_at: datetime


@dataclass(frozen=True, slots=True)
class ExecutionIntent:
    intent_id: str
    order_link_id: str
    risk_command_id: str
    risk_reservation_id: str
    kind: ExecutionIntentKind
    state: ExecutionIntentState
    symbol: str
    direction: Direction
    quantity: Decimal
    price: Decimal | None
    take_profit: Decimal | None
    stop_loss: Decimal | None
    expires_at: datetime | None
    exchange_order_id: str | None = None


def _floor_increment(value: Decimal, increment: Decimal) -> Decimal:
    return (value / increment).to_integral_value(rounding=ROUND_FLOOR) * increment


def _ceiling_increment(value: Decimal, increment: Decimal) -> Decimal:
    return (value / increment).to_integral_value(rounding=ROUND_CEILING) * increment
