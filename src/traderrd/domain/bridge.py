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

    def __post_init__(self) -> None:
        if self.captured_at.tzinfo is None:
            raise ValueError("Account snapshot timestamp must be timezone-aware")
        if not self.equity.is_finite() or self.equity <= 0:
            raise ValueError("Strategy account equity must be positive")


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
