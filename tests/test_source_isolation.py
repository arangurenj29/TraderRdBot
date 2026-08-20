import asyncio
from dataclasses import replace
from datetime import datetime, timezone
from io import StringIO
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from traderrd.application.ingestion import IngestionOutcome, IngestionResult
from traderrd.config import ObserverConfig
from traderrd.telegram_listener import _listen
from traderrd.telegram_preflight import (
    SourcePreflightError,
    SourcePreflightReport,
    inspect_configured_source,
    run_source_inspection,
)
from traderrd.telegram_source import (
    SourceDecision,
    SourceIsolationError,
    inspect_message_source,
)
from tests.samples import LONG_SIGNAL


class FakeEntity:
    def __init__(self, entity_id: int) -> None:
        self.id = entity_id


class FakeDialog:
    def __init__(self, entity_id: int) -> None:
        self.entity = FakeEntity(entity_id)


class FakeReplyHeader:
    def __init__(self, topic_id: int | None) -> None:
        self.forum_topic = topic_id is not None
        self.reply_to_top_id = topic_id
        self.reply_to_msg_id = topic_id


class FakeMessage:
    def __init__(
        self,
        message_id: int,
        raw_text: str,
        *,
        topic_id: int | None = 231508,
        sender_id: int | None = 9001,
        action: object | None = None,
    ) -> None:
        self.id = message_id
        self.raw_text = raw_text
        self.date = datetime(2026, 8, 16, 2, 15, tzinfo=timezone.utc)
        self.edit_date = None
        self.reply_to = FakeReplyHeader(topic_id)
        self.sender_id = sender_id
        self.action = action


class FakeEvent:
    def __init__(self, message: FakeMessage) -> None:
        self.message = message
        self.raw_text = message.raw_text


class FakeEvents:
    @staticmethod
    def NewMessage(**kwargs):
        return ("new", kwargs)

    @staticmethod
    def MessageEdited(**kwargs):
        return ("edited", kwargs)


class RecordingService:
    def __init__(self) -> None:
        self.events = []

    def ingest(self, event):
        self.events.append(event)
        return IngestionResult(
            outcome=IngestionOutcome.STORED,
            source_chat_id=event.source_chat_id,
            source_message_id=event.source_message_id,
            fingerprint="offline-fingerprint",
        )


class FakeListenerClient:
    def __init__(self, emitted_events: list[FakeEvent]) -> None:
        self.emitted_events = emitted_events
        self.handlers = []

    async def iter_dialogs(self):
        yield FakeDialog(2180632014)

    async def get_messages(self, entity, *, ids: int):
        return FakeMessage(ids, LONG_SIGNAL, sender_id=9001)

    def add_event_handler(self, handler, event_filter) -> None:
        self.handlers.append((handler, event_filter))

    async def run_until_disconnected(self) -> None:
        new_message_handler = self.handlers[0][0]
        for event in self.emitted_events:
            await new_message_handler(event)


class FakePreflightClient:
    def __init__(self, message: FakeMessage, *, authorized: bool = True) -> None:
        self.message = message
        self.authorized = authorized
        self.connected = False
        self.disconnected = False
        self.requested_message_id = None

    async def connect(self) -> None:
        self.connected = True

    async def disconnect(self) -> None:
        self.disconnected = True

    async def is_user_authorized(self) -> bool:
        return self.authorized

    async def iter_dialogs(self):
        yield FakeDialog(2180632014)

    async def get_messages(self, entity, *, ids: int):
        self.requested_message_id = ids
        return self.message


class TelegramSourceIsolationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.config = ObserverConfig(
            api_id=12345,
            api_hash="offline-test-placeholder",
            source_chat_id=2180632014,
            source_topic_id=231508,
            source_seed_message_id=231906,
            expected_sender_id=None,
            session_path=Path(self.temporary_directory.name) / "unused-session",
            database_path=Path(self.temporary_directory.name) / "unused.sqlite3",
            log_level="INFO",
            bybit_public_base_url="https://api.bybit.com",
            bybit_category="linear",
            bybit_timeout_seconds=3.0,
        )

    def tearDown(self) -> None:
        self.temporary_directory.cleanup()

    def test_topic_guard_rejects_general_group_and_other_topics(self) -> None:
        general = inspect_message_source(
            FakeMessage(1, LONG_SIGNAL, topic_id=None),
            topic_id=231508,
            expected_sender_id=None,
        )
        other = inspect_message_source(
            FakeMessage(2, LONG_SIGNAL, topic_id=987654),
            topic_id=231508,
            expected_sender_id=None,
        )

        self.assertEqual(general.decision, SourceDecision.OUTSIDE_TOPIC)
        self.assertEqual(other.decision, SourceDecision.OUTSIDE_TOPIC)

    def test_listener_ingests_only_the_configured_topic(self) -> None:
        private_text = "PRIVATE OUTSIDE TOPIC CONTENT"
        wrong_sender_text = "PRIVATE WRONG SENDER CONTENT"
        service_text = "PRIVATE SERVICE LABEL"
        client = FakeListenerClient(
            [
                FakeEvent(FakeMessage(1, private_text, topic_id=None)),
                FakeEvent(FakeMessage(3, wrong_sender_text, sender_id=7007)),
                FakeEvent(FakeMessage(4, service_text, action=object())),
                FakeEvent(FakeMessage(2, LONG_SIGNAL)),
            ]
        )
        service = RecordingService()

        with self.assertLogs("traderrd.telegram_listener") as logs:
            asyncio.run(_listen(client, FakeEvents, self.config, service))

        self.assertEqual([event.source_message_id for event in service.events], [2])
        self.assertNotIn(private_text, "\n".join(logs.output))
        self.assertNotIn(wrong_sender_text, "\n".join(logs.output))
        self.assertNotIn(service_text, "\n".join(logs.output))

    def test_listener_stops_on_parser_valid_signal_from_wrong_sender(self) -> None:
        client = FakeListenerClient(
            [FakeEvent(FakeMessage(5, LONG_SIGNAL, sender_id=7007))]
        )
        service = RecordingService()

        with self.assertRaises(SourceIsolationError):
            asyncio.run(_listen(client, FakeEvents, self.config, service))

        self.assertEqual(service.events, [])

    def test_preflight_validates_topic_parser_and_derives_numeric_sender(self) -> None:
        message = FakeMessage(231906, LONG_SIGNAL, sender_id=9001)
        client = FakePreflightClient(message)

        report = asyncio.run(
            inspect_configured_source(
                self.config, client_factory=lambda *args, **kwargs: client
            )
        )

        self.assertEqual(report.source_decision, "accepted")
        self.assertTrue(report.parser_eligible)
        self.assertEqual(report.observed_sender_id, 9001)
        self.assertEqual(client.requested_message_id, 231906)
        self.assertTrue(client.disconnected)

    def test_preflight_refuses_unauthorized_session_without_login(self) -> None:
        client = FakePreflightClient(
            FakeMessage(231906, LONG_SIGNAL), authorized=False
        )

        with self.assertRaises(SourcePreflightError):
            asyncio.run(
                inspect_configured_source(
                    self.config, client_factory=lambda *args, **kwargs: client
                )
            )

        self.assertTrue(client.disconnected)

    def test_preflight_applies_expected_sender_as_second_guard(self) -> None:
        config = replace(self.config, expected_sender_id=7007)
        client = FakePreflightClient(FakeMessage(231906, LONG_SIGNAL, sender_id=9001))

        report = asyncio.run(
            inspect_configured_source(
                config, client_factory=lambda *args, **kwargs: client
            )
        )

        self.assertEqual(report.source_decision, "unexpected_sender")
        self.assertTrue(report.expected_sender_configured)

    def test_preflight_reports_parser_error_code_without_message_text(self) -> None:
        private_text = "PRIVATE NON-SIGNAL CONTENT"
        client = FakePreflightClient(FakeMessage(231906, private_text))

        report = asyncio.run(
            inspect_configured_source(
                self.config, client_factory=lambda *args, **kwargs: client
            )
        )

        rendered = report.format()
        self.assertFalse(report.parser_eligible)
        self.assertEqual(report.parser_error_code, "missing_field")
        self.assertNotIn(private_text, rendered)

    def test_preflight_output_contains_metadata_only(self) -> None:
        private_text = "PRIVATE SOURCE CONTENT"
        report = SourcePreflightReport(
            chat_id=2180632014,
            topic_id=231508,
            seed_message_id=231906,
            source_decision="accepted",
            parser_eligible=True,
            parser_error_code=None,
            observed_sender_id=9001,
            expected_sender_configured=False,
        )
        output = StringIO()

        def return_report(coroutine):
            coroutine.close()
            return report

        with patch(
            "traderrd.telegram_preflight.asyncio.run", side_effect=return_report
        ):
            exit_code = run_source_inspection(self.config, output=output)

        self.assertEqual(exit_code, 0)
        self.assertIn("topic_id=231508", output.getvalue())
        self.assertNotIn(private_text, output.getvalue())


if __name__ == "__main__":
    unittest.main()
