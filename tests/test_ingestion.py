from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
import tempfile
import unittest

from traderrd.application.ingestion import (
    InboundTelegramEvent,
    IngestionOutcome,
    QuoteCaptureOutcome,
    SignalIngestionService,
)
from traderrd.domain.market_data import MarketDataError, MarketQuote
from traderrd.domain.parser import SignalParser
from traderrd.infrastructure.sqlite_repository import SQLiteSignalRepository
from tests.samples import LONG_SIGNAL, SHORT_SIGNAL


CAPTURED_AT = datetime(2026, 8, 16, 12, 0, tzinfo=timezone.utc)


class SuccessfulQuoteProvider:
    provider_name = "bybit"
    category = "linear"

    def __init__(self) -> None:
        self.calls: list[str] = []

    def fetch_quote(self, symbol: str) -> MarketQuote:
        self.calls.append(symbol)
        return MarketQuote(
            provider=self.provider_name,
            category=self.category,
            symbol=symbol,
            best_bid=Decimal("0.15687"),
            best_ask=Decimal("0.15689"),
            last_price=Decimal("0.15688"),
            mark_price=Decimal("0.156875"),
            provider_timestamp=datetime(
                2026, 8, 16, 11, 59, 59, tzinfo=timezone.utc
            ),
            captured_at=CAPTURED_AT,
        )


class FailingQuoteProvider:
    provider_name = "bybit"
    category = "linear"

    def __init__(self) -> None:
        self.calls: list[str] = []

    def fetch_quote(self, symbol: str) -> MarketQuote:
        self.calls.append(symbol)
        raise MarketDataError("unavailable", "Bybit public market data is unavailable")


class SignalIngestionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary_directory = tempfile.TemporaryDirectory()
        database = Path(self.temporary_directory.name) / "observer.sqlite3"
        self.repository = SQLiteSignalRepository(database)
        self.repository.initialize()
        self.service = SignalIngestionService(SignalParser(), self.repository)

    def tearDown(self) -> None:
        self.temporary_directory.cleanup()

    def event(
        self,
        message_id: int,
        raw_text: str = LONG_SIGNAL,
        *,
        is_edit: bool = False,
    ) -> InboundTelegramEvent:
        return InboundTelegramEvent(
            source_chat_id=2180632014,
            source_message_id=message_id,
            raw_text=raw_text,
            telegram_received_at=datetime(2026, 8, 16, 2, 15, tzinfo=timezone.utc),
            is_edit=is_edit,
            telegram_edited_at=(
                datetime(2026, 8, 16, 2, 20, tzinfo=timezone.utc) if is_edit else None
            ),
        )

    def test_stores_valid_signal_and_audit_revision(self) -> None:
        result = self.service.ingest(self.event(231508))
        self.assertEqual(result.outcome, IngestionOutcome.STORED)
        self.assertEqual(self.repository.count("inbound_messages"), 1)
        self.assertEqual(self.repository.count("inbound_revisions"), 1)
        self.assertEqual(self.repository.count("signals"), 1)

    def test_quote_schema_initialization_is_additive_and_idempotent(self) -> None:
        self.repository.initialize()
        self.assertEqual(self.repository.count("market_quote_captures"), 0)

    def test_duplicate_telegram_message_is_explicit_and_audited(self) -> None:
        self.service.ingest(self.event(231508))
        result = self.service.ingest(self.event(231508))
        self.assertEqual(result.outcome, IngestionOutcome.DUPLICATE_MESSAGE)
        self.assertEqual(self.repository.count("inbound_messages"), 1)
        self.assertEqual(self.repository.count("inbound_revisions"), 2)
        self.assertEqual(self.repository.count("signals"), 1)

    def test_duplicate_signal_under_different_message_id_is_audited(self) -> None:
        self.service.ingest(self.event(231508))
        result = self.service.ingest(self.event(231509))
        self.assertEqual(result.outcome, IngestionOutcome.DUPLICATE_SIGNAL)
        self.assertEqual(self.repository.count("inbound_messages"), 2)
        self.assertEqual(self.repository.count("inbound_revisions"), 2)
        self.assertEqual(self.repository.count("signals"), 1)
        inbound = self.repository.get_inbound(2180632014, 231509)
        self.assertEqual(inbound["outcome"], "duplicate_signal")

    def test_invalid_message_preserves_raw_audit(self) -> None:
        raw = "This is not a signal"
        result = self.service.ingest(self.event(231510, raw))
        self.assertEqual(result.outcome, IngestionOutcome.INVALID)
        self.assertEqual(result.error_code, "missing_field")
        inbound = self.repository.get_inbound(2180632014, 231510)
        self.assertEqual(inbound["raw_text"], raw)
        self.assertEqual(inbound["outcome"], "invalid")
        self.assertEqual(self.repository.count("signals"), 0)

    def test_duplicate_replay_preserves_canonical_invalid_outcome(self) -> None:
        event = self.event(231516, "This is not a signal")
        first = self.service.ingest(event)
        replay = self.service.ingest(event)

        self.assertEqual(first.outcome, IngestionOutcome.INVALID)
        self.assertEqual(replay.outcome, IngestionOutcome.DUPLICATE_MESSAGE)
        inbound = self.repository.get_inbound(2180632014, 231516)
        self.assertEqual(inbound["outcome"], "invalid")
        self.assertEqual(inbound["error_code"], "missing_field")
        self.assertEqual(self.repository.count("inbound_revisions"), 2)

    def test_edited_message_replaces_current_signal_and_preserves_revisions(self) -> None:
        first = self.service.ingest(self.event(231508, LONG_SIGNAL))
        edited_body = SHORT_SIGNAL.replace("2026-08-15 04:30", "2026-08-15 04:31")
        second = self.service.ingest(self.event(231508, edited_body, is_edit=True))
        self.assertEqual(first.outcome, IngestionOutcome.STORED)
        self.assertEqual(second.outcome, IngestionOutcome.STORED)
        self.assertNotEqual(first.fingerprint, second.fingerprint)
        self.assertEqual(self.repository.count("inbound_messages"), 1)
        self.assertEqual(self.repository.count("inbound_revisions"), 2)
        self.assertEqual(self.repository.count("signals"), 1)

    def test_repeated_unchanged_edit_is_duplicate_message(self) -> None:
        self.service.ingest(self.event(231508, LONG_SIGNAL))
        result = self.service.ingest(self.event(231508, LONG_SIGNAL, is_edit=True))
        self.assertEqual(result.outcome, IngestionOutcome.DUPLICATE_MESSAGE)
        self.assertEqual(self.repository.count("signals"), 1)

    def test_persists_successful_quote_capture(self) -> None:
        provider = SuccessfulQuoteProvider()
        service = SignalIngestionService(SignalParser(), self.repository, provider)

        result = service.ingest(self.event(231511))

        self.assertEqual(result.outcome, IngestionOutcome.STORED)
        self.assertEqual(result.quote_outcome, QuoteCaptureOutcome.CAPTURED)
        self.assertEqual(provider.calls, ["XLMUSDT"])
        captures = self.repository.get_quote_captures(result.fingerprint)
        self.assertEqual(len(captures), 1)
        self.assertEqual(captures[0]["outcome"], "captured")
        self.assertEqual(captures[0]["best_bid"], "0.15687")
        self.assertEqual(captures[0]["mark_price"], "0.156875")

    def test_quote_failure_is_audited_without_losing_signal(self) -> None:
        provider = FailingQuoteProvider()
        service = SignalIngestionService(
            SignalParser(), self.repository, provider, clock=lambda: CAPTURED_AT
        )

        result = service.ingest(self.event(231512))

        self.assertEqual(result.outcome, IngestionOutcome.STORED)
        self.assertEqual(result.quote_outcome, QuoteCaptureOutcome.FAILED)
        self.assertEqual(result.quote_error_code, "unavailable")
        self.assertEqual(self.repository.count("signals"), 1)
        captures = self.repository.get_quote_captures(result.fingerprint)
        self.assertEqual(len(captures), 1)
        self.assertEqual(captures[0]["outcome"], "failed")
        self.assertEqual(captures[0]["error_code"], "unavailable")

    def test_duplicates_and_invalid_messages_do_not_request_quotes(self) -> None:
        provider = SuccessfulQuoteProvider()
        service = SignalIngestionService(SignalParser(), self.repository, provider)

        stored = service.ingest(self.event(231513))
        duplicate_message = service.ingest(self.event(231513))
        duplicate_signal = service.ingest(self.event(231514))
        invalid = service.ingest(self.event(231515, "not a signal"))

        self.assertEqual(stored.outcome, IngestionOutcome.STORED)
        self.assertEqual(duplicate_message.outcome, IngestionOutcome.DUPLICATE_MESSAGE)
        self.assertEqual(duplicate_signal.outcome, IngestionOutcome.DUPLICATE_SIGNAL)
        self.assertEqual(invalid.outcome, IngestionOutcome.INVALID)
        self.assertEqual(provider.calls, ["XLMUSDT"])
        self.assertEqual(self.repository.count("market_quote_captures"), 1)


if __name__ == "__main__":
    unittest.main()
