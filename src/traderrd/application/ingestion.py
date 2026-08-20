from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime, timezone
from enum import StrEnum
from typing import Callable, Protocol

from traderrd.domain.market_data import MarketDataError, MarketQuote
from traderrd.domain.models import SignalDraft
from traderrd.domain.parser import SignalParseError, SignalParser


class IngestionOutcome(StrEnum):
    STORED = "stored"
    DUPLICATE_MESSAGE = "duplicate_message"
    DUPLICATE_SIGNAL = "duplicate_signal"
    INVALID = "invalid"


class QuoteCaptureOutcome(StrEnum):
    CAPTURED = "captured"
    FAILED = "failed"


@dataclass(frozen=True, slots=True)
class InboundTelegramEvent:
    source_chat_id: int
    source_message_id: int
    raw_text: str
    telegram_received_at: datetime
    source_topic_id: int | None = None
    source_sender_id: int | None = None
    is_edit: bool = False
    telegram_edited_at: datetime | None = None

    def __post_init__(self) -> None:
        if self.source_chat_id <= 0 or self.source_message_id <= 0:
            raise ValueError("source chat and message IDs must be positive")
        if self.source_topic_id is not None and self.source_topic_id <= 0:
            raise ValueError("source topic ID must be positive when set")
        if self.source_sender_id == 0:
            raise ValueError("source sender ID must be non-zero when set")
        if self.telegram_received_at.tzinfo is None:
            raise ValueError("telegram_received_at must be timezone-aware")
        if self.telegram_edited_at is not None and self.telegram_edited_at.tzinfo is None:
            raise ValueError("telegram_edited_at must be timezone-aware")


@dataclass(frozen=True, slots=True)
class IngestionResult:
    outcome: IngestionOutcome
    source_chat_id: int
    source_message_id: int
    fingerprint: str | None = None
    error_code: str | None = None
    detail: str | None = None
    quote_outcome: QuoteCaptureOutcome | None = None
    quote_error_code: str | None = None


@dataclass(frozen=True, slots=True)
class QuoteCapture:
    outcome: QuoteCaptureOutcome
    provider: str
    category: str
    symbol: str
    captured_at: datetime
    quote: MarketQuote | None = None
    error_code: str | None = None
    error_detail: str | None = None

    def __post_init__(self) -> None:
        if self.captured_at.tzinfo is None:
            raise ValueError("Quote capture timestamp must be timezone-aware")
        if self.outcome is QuoteCaptureOutcome.CAPTURED:
            if self.quote is None or self.error_code is not None:
                raise ValueError("Captured quote outcome requires quote data only")
        elif self.quote is not None or not self.error_code:
            raise ValueError("Failed quote outcome requires an error and no quote")


class MarketQuoteProvider(Protocol):
    provider_name: str
    category: str

    def fetch_quote(self, symbol: str) -> MarketQuote: ...


class SignalRepository(Protocol):
    def record_event(
        self,
        event: InboundTelegramEvent,
        draft: SignalDraft | None,
        parse_error: SignalParseError | None,
    ) -> IngestionResult: ...

    def record_quote_capture(
        self,
        event: InboundTelegramEvent,
        signal_fingerprint: str,
        capture: QuoteCapture,
    ) -> None: ...


class SignalIngestionService:
    def __init__(
        self,
        parser: SignalParser,
        repository: SignalRepository,
        quote_provider: MarketQuoteProvider | None = None,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._parser = parser
        self._repository = repository
        self._quote_provider = quote_provider
        self._clock = clock or (lambda: datetime.now(timezone.utc))

    def ingest(self, event: InboundTelegramEvent) -> IngestionResult:
        try:
            draft = self._parser.parse(event.raw_text)
            error = None
        except SignalParseError as exc:
            draft = None
            error = exc
        result = self._repository.record_event(event, draft, error)
        if (
            result.outcome is not IngestionOutcome.STORED
            or draft is None
            or result.fingerprint is None
            or self._quote_provider is None
        ):
            return result
        return self._capture_quote(event, draft, result)

    def _capture_quote(
        self,
        event: InboundTelegramEvent,
        draft: SignalDraft,
        result: IngestionResult,
    ) -> IngestionResult:
        provider = self._quote_provider
        if provider is None or result.fingerprint is None:
            return result
        try:
            quote = provider.fetch_quote(draft.symbol)
            if quote.symbol != draft.symbol:
                raise MarketDataError(
                    "quote_symbol_mismatch",
                    "Market data provider returned a different symbol",
                )
            capture = QuoteCapture(
                outcome=QuoteCaptureOutcome.CAPTURED,
                provider=provider.provider_name,
                category=provider.category,
                symbol=draft.symbol,
                captured_at=quote.captured_at,
                quote=quote,
            )
        except MarketDataError as exc:
            capture = QuoteCapture(
                outcome=QuoteCaptureOutcome.FAILED,
                provider=provider.provider_name,
                category=provider.category,
                symbol=draft.symbol,
                captured_at=self._aware_now(),
                error_code=exc.code,
                error_detail=str(exc)[:500],
            )
        except Exception:
            capture = QuoteCapture(
                outcome=QuoteCaptureOutcome.FAILED,
                provider=provider.provider_name,
                category=provider.category,
                symbol=draft.symbol,
                captured_at=self._aware_now(),
                error_code="unexpected_provider_error",
                error_detail="Unexpected public market data provider failure",
            )

        try:
            self._repository.record_quote_capture(event, result.fingerprint, capture)
        except Exception:
            return replace(
                result,
                quote_outcome=QuoteCaptureOutcome.FAILED,
                quote_error_code="quote_persistence_error",
            )
        return replace(
            result,
            quote_outcome=capture.outcome,
            quote_error_code=capture.error_code,
        )

    def _aware_now(self) -> datetime:
        try:
            value = self._clock()
            if value.tzinfo is not None:
                return value
        except Exception:
            pass
        return datetime.now(timezone.utc)
