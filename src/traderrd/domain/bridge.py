from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import StrEnum

from traderrd.domain.execution import ExecutionIntent
from traderrd.domain.models import Direction
from traderrd.domain.risk import RiskDecision


VALIDATED_SOURCE_CHAT_ID = 2180632014
VALIDATED_SOURCE_TOPIC_ID = 231508


class BridgeStatus(StrEnum):
    PREVIEW = "preview"
    PERSISTED = "persisted"
    DUPLICATE = "duplicate"
    REJECTED = "rejected"
    STALE = "stale"


@dataclass(frozen=True, slots=True)
class ScopedStoredSignal:
    fingerprint: str
    source_chat_id: int
    source_topic_id: int
    source_sender_id: int
    source_message_id: int
    telegram_received_at: datetime
    direction: Direction
    symbol: str
    entry: Decimal
    take_profit: Decimal
    stop_loss: Decimal

    def __post_init__(self) -> None:
        if self.telegram_received_at.tzinfo is None:
            raise ValueError("Signal receipt timestamp must be timezone-aware")


@dataclass(frozen=True, slots=True)
class DemoAccountPosition:
    symbol: str
    direction: Direction
    quantity: Decimal
    average_price: Decimal | None = None
    mark_price: Decimal | None = None
    liquidation_price: Decimal | None = None
    unrealised_pnl: Decimal | None = None
    leverage: Decimal | None = None
    position_margin: Decimal | None = None
    take_profit: Decimal | None = None
    stop_loss: Decimal | None = None

    def __post_init__(self) -> None:
        if not self.quantity.is_finite() or self.quantity <= 0:
            raise ValueError("Demo position quantity must be positive")
        unsigned = (
            self.average_price, self.mark_price, self.liquidation_price,
            self.leverage, self.position_margin, self.take_profit, self.stop_loss,
        )
        if any(value is not None and (not value.is_finite() or value <= 0) for value in unsigned):
            raise ValueError("Demo position metrics must be finite and positive")
        if self.unrealised_pnl is not None and not self.unrealised_pnl.is_finite():
            raise ValueError("Demo position unrealised P&L must be finite")


@dataclass(frozen=True, slots=True)
class DemoAccountOrder:
    order_link_id: str | None
    symbol: str
    exchange_protective_child: bool = False

    def is_known_or_exchange_protective(self, known_order_links: set[str]) -> bool:
        """Return true only for a TraderRd order or a verified Bybit TP/SL child."""
        return self.order_link_id in known_order_links or self.exchange_protective_child


@dataclass(frozen=True, slots=True)
class DemoStrategyAccountSnapshot:
    equity: Decimal
    captured_at: datetime
    positions: tuple[DemoAccountPosition, ...]
    orders: tuple[DemoAccountOrder, ...]
    wallet_balance: Decimal | None = None
    unrealised_pnl: Decimal | None = None
    available_balance: Decimal | None = None
    position_initial_margin: Decimal | None = None
    order_initial_margin: Decimal | None = None

    def __post_init__(self) -> None:
        if self.captured_at.tzinfo is None:
            raise ValueError("Account snapshot timestamp must be timezone-aware")
        if not self.equity.is_finite() or self.equity <= 0:
            raise ValueError("Strategy account equity must be positive")
        unsigned = (
            self.wallet_balance, self.available_balance,
            self.position_initial_margin, self.order_initial_margin,
        )
        if any(value is not None and (not value.is_finite() or value < 0) for value in unsigned):
            raise ValueError("Demo account metrics must be finite and non-negative")
        if self.unrealised_pnl is not None and not self.unrealised_pnl.is_finite():
            raise ValueError("Demo account unrealised P&L must be finite")


@dataclass(frozen=True, slots=True)
class DemoBridgeResult:
    status: BridgeStatus
    reason: str
    signal_fingerprint: str
    risk_command_id: str | None = None
    equity: Decimal | None = None
    decision: RiskDecision | None = None
    intents: tuple[ExecutionIntent, ...] = ()
    duplicate: bool = False
