from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal


class MarketDataError(RuntimeError):
    """A public market-data failure with a stable machine-readable reason."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


@dataclass(frozen=True, slots=True)
class MarketQuote:
    provider: str
    category: str
    symbol: str
    best_bid: Decimal
    best_ask: Decimal
    last_price: Decimal
    mark_price: Decimal | None
    provider_timestamp: datetime
    captured_at: datetime

    def __post_init__(self) -> None:
        if not self.provider or not self.category:
            raise ValueError("Market quote provider and category are required")
        if not self.symbol or self.symbol != self.symbol.upper():
            raise ValueError("Market quote symbol must be uppercase")
        prices = (self.best_bid, self.best_ask, self.last_price)
        if any(not value.is_finite() or value <= 0 for value in prices):
            raise ValueError("Market quote prices must be finite and positive")
        if self.mark_price is not None and (
            not self.mark_price.is_finite() or self.mark_price <= 0
        ):
            raise ValueError("Market quote mark price must be finite and positive")
        if self.best_ask < self.best_bid:
            raise ValueError("Market quote best ask cannot be below best bid")
        if self.provider_timestamp.tzinfo is None or self.captured_at.tzinfo is None:
            raise ValueError("Market quote timestamps must be timezone-aware")
