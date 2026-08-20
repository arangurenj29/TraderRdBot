from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import StrEnum
import hashlib
import json


class Direction(StrEnum):
    LONG = "LONG"
    SHORT = "SHORT"


def canonical_decimal(value: Decimal) -> str:
    """Return a stable plain-decimal representation for hashing and storage."""
    if value == 0:
        return "0"
    return format(value.normalize(), "f")


@dataclass(frozen=True, slots=True)
class SignalDraft:
    direction: Direction
    symbol: str
    timeframe_minutes: int
    entry: Decimal
    take_profit: Decimal
    stop_loss: Decimal
    signal_timestamp: datetime

    @property
    def fingerprint(self) -> str:
        payload = {
            "direction": self.direction.value,
            "entry": canonical_decimal(self.entry),
            "signal_timestamp": self.signal_timestamp.isoformat(timespec="minutes"),
            "stop_loss": canonical_decimal(self.stop_loss),
            "symbol": self.symbol,
            "take_profit": canonical_decimal(self.take_profit),
            "timeframe_minutes": self.timeframe_minutes,
        }
        serialized = json.dumps(
            payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True
        )
        return hashlib.sha256(serialized.encode("ascii")).hexdigest()


@dataclass(frozen=True, slots=True)
class ObservedSignal:
    draft: SignalDraft
    source_chat_id: int
    source_message_id: int
    telegram_received_at: datetime
    source_topic_id: int | None = None
    source_sender_id: int | None = None
    telegram_edited_at: datetime | None = None

    def __post_init__(self) -> None:
        if self.telegram_received_at.tzinfo is None:
            raise ValueError("telegram_received_at must be timezone-aware")
        if self.telegram_edited_at is not None and self.telegram_edited_at.tzinfo is None:
            raise ValueError("telegram_edited_at must be timezone-aware")
        if self.source_chat_id <= 0 or self.source_message_id <= 0:
            raise ValueError("source chat and message IDs must be positive")
        if self.source_topic_id is not None and self.source_topic_id <= 0:
            raise ValueError("source topic ID must be positive when set")
        if self.source_sender_id == 0:
            raise ValueError("source sender ID must be non-zero when set")

    @property
    def fingerprint(self) -> str:
        return self.draft.fingerprint
