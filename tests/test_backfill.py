import asyncio
from dataclasses import replace
from datetime import datetime, timezone
from io import StringIO
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from traderrd.config import ObserverConfig
from traderrd.infrastructure.sqlite_repository import SQLiteSignalRepository
from traderrd.telegram_backfill import (
    BackfillAuthorizationError,
    BackfillCounters,
    BackfillError,
    backfill_history,
    run_backfill,
)
from tests.samples import LONG_SIGNAL, SHORT_SIGNAL


class FakeEntity:
    def __init__(self, entity_id: int) -> None:
        self.id = entity_id


class FakeDialog:
    def __init__(self, entity_id: int) -> None:
        self.entity = FakeEntity(entity_id)


class FakeReplyHeader:
    def __init__(
        self, topic_id: int | None, forum_topic: bool | None = None
    ) -> None:
        self.forum_topic = topic_id is not None if forum_topic is None else forum_topic
        self.reply_to_top_id = topic_id
        self.reply_to_msg_id = topic_id


class FakeMessage:
    def __init__(
        self,
        message_id: int,
        raw_text: str | None,
        date: datetime,
        *,
        topic_id: int | None = 231508,
        sender_id: int | None = 9001,
        action: object | None = None,
        forum_topic: bool | None = None,
    ) -> None:
        self.id = message_id
        self.raw_text = raw_text
        self.date = date
        self.edit_date = None
        self.reply_to = FakeReplyHeader(topic_id, forum_topic)
        self.sender_id = sender_id
        self.action = action


class MessageService(FakeMessage):
    pass


class FakeTelegramClient:
    def __init__(
        self,
        messages_newest_first: list[FakeMessage],
        *,
        authorized: bool = True,
        iteration_error: BaseException | None = None,
    ) -> None:
        self.messages_newest_first = messages_newest_first
        self.authorized = authorized
        self.iteration_error = iteration_error
        self.connected = False
        self.disconnected = False
        self.iteration_arguments: tuple[int | None, bool, int, int] | None = None
        self.factory_kwargs: dict[str, object] = {}
        self.requested_seed_id: int | None = None

    async def connect(self) -> None:
        self.connected = True

    async def disconnect(self) -> None:
        self.disconnected = True

    async def is_user_authorized(self) -> bool:
        return self.authorized

    async def iter_dialogs(self):
        yield FakeDialog(2180632014)

    async def get_messages(self, entity: FakeEntity, *, ids: int) -> FakeMessage:
        self.requested_seed_id = ids
        return FakeMessage(ids, LONG_SIGNAL, datetime.now(timezone.utc))

    async def iter_messages(
        self,
        entity: FakeEntity,
        *,
        limit: int | None,
        reverse: bool,
        min_id: int,
        reply_to: int,
    ):
        self.iteration_arguments = (limit, reverse, min_id, reply_to)
        if self.iteration_error is not None:
            raise self.iteration_error
        messages = list(reversed(self.messages_newest_first)) if reverse else list(
            self.messages_newest_first
        )
        messages = [message for message in messages if message.id > min_id]
        for message in messages[:limit]:
            yield message


class RecordingRepository(SQLiteSignalRepository):
    def __init__(self, database_path: Path) -> None:
        super().__init__(database_path)
        self.message_order: list[int] = []

    def record_event(self, event, draft, parse_error):
        self.message_order.append(event.source_message_id)
        return super().record_event(event, draft, parse_error)

    def record_quote_capture(self, event, signal_fingerprint, capture) -> None:
        raise AssertionError("Historical backfill must never capture current quotes")


class FailingRepository(RecordingRepository):
    def __init__(self, database_path: Path, failing_message_id: int) -> None:
        super().__init__(database_path)
        self.failing_message_id = failing_message_id

    def record_event(self, event, draft, parse_error):
        if event.source_message_id == self.failing_message_id:
            raise RuntimeError("simulated local failure")
        return super().record_event(event, draft, parse_error)


class TelegramBackfillTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.repository = RecordingRepository(
            Path(self.temporary_directory.name) / "backfill.sqlite3"
        )
        self.config = ObserverConfig(
            api_id=12345,
            api_hash="offline-test-placeholder",
            source_chat_id=2180632014,
            source_topic_id=231508,
            source_seed_message_id=231906,
            expected_sender_id=None,
            session_path=Path(self.temporary_directory.name) / "unused-session",
            database_path=Path(self.temporary_directory.name) / "backfill.sqlite3",
            log_level="INFO",
            bybit_public_base_url="https://api.bybit.com",
            bybit_category="linear",
            bybit_timeout_seconds=3.0,
        )
        self.date = datetime(2026, 8, 16, 2, 15, tzinfo=timezone.utc)

    def tearDown(self) -> None:
        self.temporary_directory.cleanup()

    @staticmethod
    def factory_for(client: FakeTelegramClient):
        def factory(*args: object, **kwargs: object) -> FakeTelegramClient:
            client.factory_kwargs = kwargs
            return client

        return factory

    def run_history(
        self,
        client: FakeTelegramClient,
        *,
        limit: int | None = None,
        progress=None,
    ) -> BackfillCounters:
        return asyncio.run(
            backfill_history(
                self.config,
                limit=limit,
                repository=self.repository,
                client_factory=self.factory_for(client),
                progress=progress,
            )
        )

    def test_processes_oldest_first_and_honors_limit(self) -> None:
        client = FakeTelegramClient(
            [
                FakeMessage(2, SHORT_SIGNAL, self.date),
                FakeMessage(1, LONG_SIGNAL, self.date),
            ]
        )
        counters = self.run_history(client, limit=1)

        self.assertEqual(client.iteration_arguments, (1, True, 0, 231508))
        self.assertEqual(client.factory_kwargs["flood_sleep_threshold"], 10)
        self.assertEqual(client.factory_kwargs["request_retries"], 3)
        self.assertEqual(client.factory_kwargs["connection_retries"], 3)
        self.assertEqual(client.requested_seed_id, 231906)
        self.assertEqual(self.repository.message_order, [1])
        self.assertEqual(counters.scanned, 1)
        self.assertEqual(counters.stored, 1)
        self.assertTrue(client.disconnected)

    def test_refuses_unauthorized_session_without_starting_login(self) -> None:
        client = FakeTelegramClient([], authorized=False)
        with self.assertRaises(BackfillAuthorizationError) as raised:
            self.run_history(client)
        self.assertIn("not authorized", str(raised.exception))
        self.assertTrue(client.disconnected)
        self.assertIsNone(client.iteration_arguments)

    def test_repeated_run_resumes_after_checkpoint_without_quotes(self) -> None:
        messages = [
            FakeMessage(3, None, self.date),
            FakeMessage(2, "ordinary channel text", self.date),
            FakeMessage(1, LONG_SIGNAL, self.date),
        ]
        first = self.run_history(FakeTelegramClient(messages))
        second = self.run_history(FakeTelegramClient(messages))

        self.assertEqual(first.scanned, 3)
        self.assertEqual(first.text_messages, 2)
        self.assertEqual(first.stored, 1)
        self.assertEqual(first.invalid, 0)
        self.assertEqual(first.skipped_non_text, 1)
        self.assertEqual(first.skipped_non_signal, 1)
        self.assertEqual(second.scanned, 0)
        self.assertEqual(second.stored, 0)
        self.assertEqual(second.duplicate_message, 0)
        self.assertEqual(second.skipped_non_text, 0)
        self.assertEqual(self.repository.count("market_quote_captures"), 0)

    def test_bounded_runs_resume_from_the_next_message(self) -> None:
        messages = [
            FakeMessage(2, SHORT_SIGNAL, self.date),
            FakeMessage(1, LONG_SIGNAL, self.date),
        ]

        first = self.run_history(FakeTelegramClient(messages), limit=1)
        second_client = FakeTelegramClient(messages)
        second = self.run_history(second_client, limit=1)

        self.assertEqual(first.stored, 1)
        self.assertEqual(second.stored, 1)
        self.assertEqual(second_client.iteration_arguments, (1, True, 1, 231508))
        self.assertEqual(self.repository.message_order, [1, 2])
        self.assertEqual(
            self.repository.get_backfill_checkpoint(2180632014, 231508), 2
        )

    def test_checkpoints_are_isolated_by_topic(self) -> None:
        self.repository.initialize()
        self.repository.advance_backfill_checkpoint(2180632014, 231508, 10)
        self.repository.advance_backfill_checkpoint(2180632014, 999999, 20)

        self.assertEqual(
            self.repository.get_backfill_checkpoint(2180632014, 231508), 10
        )
        self.assertEqual(
            self.repository.get_backfill_checkpoint(2180632014, 999999), 20
        )

    def test_rejects_outside_topic_without_persisting_message(self) -> None:
        message = FakeMessage(4, LONG_SIGNAL, self.date, topic_id=999999)

        counters = BackfillCounters()
        with self.assertRaises(BackfillError):
            asyncio.run(
                backfill_history(
                    self.config,
                    counters=counters,
                    repository=self.repository,
                    client_factory=self.factory_for(FakeTelegramClient([message])),
                )
            )

        self.assertEqual(counters.scanned, 1)
        self.assertEqual(counters.source_rejected, 1)
        self.assertEqual(counters.text_messages, 0)
        self.assertEqual(self.repository.count("inbound_messages"), 0)
        self.assertEqual(
            self.repository.get_backfill_checkpoint(2180632014, 231508), 0
        )

    def test_rejects_outside_topic_before_non_signal_classification(self) -> None:
        message = FakeMessage(
            7,
            "ordinary comment",
            self.date,
            topic_id=999999,
            sender_id=7007,
        )
        counters = BackfillCounters()

        with self.assertRaises(BackfillError):
            asyncio.run(
                backfill_history(
                    self.config,
                    counters=counters,
                    repository=self.repository,
                    client_factory=self.factory_for(FakeTelegramClient([message])),
                )
            )

        self.assertEqual(counters.source_rejected, 1)
        self.assertEqual(counters.skipped_non_signal, 0)
        self.assertEqual(self.repository.count("inbound_messages"), 0)

    def test_accepts_explicit_topic_id_when_forum_flag_is_absent(self) -> None:
        message = FakeMessage(
            6,
            LONG_SIGNAL,
            self.date,
            topic_id=231508,
            forum_topic=False,
        )

        counters = self.run_history(FakeTelegramClient([message]))

        self.assertEqual(counters.stored, 1)
        self.assertEqual(counters.source_rejected, 0)
        self.assertEqual(self.repository.message_order, [6])

    def test_topic_service_metadata_does_not_block_backfill(self) -> None:
        messages = [
            MessageService(
                3,
                "synthetic service label without action metadata",
                self.date,
                topic_id=None,
                sender_id=None,
            ),
            FakeMessage(
                2,
                "synthetic service label",
                self.date,
                topic_id=None,
                sender_id=None,
                action=object(),
            ),
            FakeMessage(1, None, self.date, sender_id=None),
        ]

        counters = self.run_history(FakeTelegramClient(messages))

        self.assertEqual(counters.scanned, 3)
        self.assertEqual(counters.text_messages, 0)
        self.assertEqual(counters.skipped_non_text, 3)
        self.assertEqual(counters.source_rejected, 0)
        self.assertEqual(counters.errors, 0)
        self.assertEqual(self.repository.count("inbound_messages"), 0)
        self.assertEqual(
            self.repository.get_backfill_checkpoint(2180632014, 231508), 3
        )

    def test_rejects_unexpected_sender_without_persisting_message(self) -> None:
        self.config = replace(self.config, expected_sender_id=9001)
        message = FakeMessage(5, LONG_SIGNAL, self.date, sender_id=7007)

        counters = BackfillCounters()
        with self.assertRaises(BackfillError):
            asyncio.run(
                backfill_history(
                    self.config,
                    counters=counters,
                    repository=self.repository,
                    client_factory=self.factory_for(FakeTelegramClient([message])),
                )
            )

        self.assertEqual(counters.source_rejected, 1)
        self.assertEqual(self.repository.count("inbound_messages"), 0)

    def test_non_signal_from_unexpected_sender_advances_without_persistence(
        self,
    ) -> None:
        self.config = replace(self.config, expected_sender_id=9001)
        message = FakeMessage(
            8,
            "ordinary topic comment",
            self.date,
            sender_id=7007,
        )

        counters = self.run_history(FakeTelegramClient([message]))

        self.assertEqual(counters.scanned, 1)
        self.assertEqual(counters.text_messages, 1)
        self.assertEqual(counters.skipped_non_signal, 1)
        self.assertEqual(counters.source_rejected, 0)
        self.assertEqual(counters.errors, 0)
        self.assertEqual(self.repository.count("inbound_messages"), 0)
        self.assertEqual(
            self.repository.get_backfill_checkpoint(2180632014, 231508), 8
        )

    def test_progress_and_final_counters_do_not_include_raw_text(self) -> None:
        private_text = "PRIVATE MESSAGE CONTENT"
        messages = [FakeMessage(1, private_text, self.date)]
        progress_lines: list[str] = []
        with patch("traderrd.telegram_backfill.PROGRESS_INTERVAL", 1):
            counters = self.run_history(
                FakeTelegramClient(messages),
                progress=lambda current: progress_lines.append(current.format()),
            )

        rendered = "\n".join([*progress_lines, counters.format("Backfill final")])
        self.assertNotIn(private_text, rendered)
        self.assertIn("scanned=1", rendered)

    def test_transient_connection_error_stops_safely_with_counter(self) -> None:
        counters = BackfillCounters()
        client = FakeTelegramClient([], iteration_error=OSError("offline"))
        with self.assertRaises(BackfillError):
            asyncio.run(
                backfill_history(
                    self.config,
                    counters=counters,
                    repository=self.repository,
                    client_factory=self.factory_for(client),
                )
            )
        self.assertEqual(counters.errors, 1)
        self.assertTrue(client.disconnected)

    def test_local_failure_does_not_advance_past_failed_message(self) -> None:
        self.repository = FailingRepository(
            Path(self.temporary_directory.name) / "failing.sqlite3", 2
        )
        messages = [
            FakeMessage(2, SHORT_SIGNAL, self.date),
            FakeMessage(1, LONG_SIGNAL, self.date),
        ]

        with self.assertRaises(BackfillError):
            self.run_history(FakeTelegramClient(messages))

        self.assertEqual(
            self.repository.get_backfill_checkpoint(2180632014, 231508), 1
        )

    def test_cancellation_returns_130_without_traceback(self) -> None:
        def interrupt(coroutine: object) -> None:
            coroutine.close()
            raise KeyboardInterrupt()

        output = StringIO()
        error_output = StringIO()
        with patch("traderrd.telegram_backfill.asyncio.run", side_effect=interrupt):
            exit_code = run_backfill(
                self.config, output=output, error_output=error_output
            )

        self.assertEqual(exit_code, 130)
        self.assertIn("Backfill final:", output.getvalue())
        self.assertNotIn("Traceback", output.getvalue() + error_output.getvalue())

    def test_async_cancellation_disconnects_client(self) -> None:
        client = FakeTelegramClient([], iteration_error=asyncio.CancelledError())
        with self.assertRaises(asyncio.CancelledError):
            self.run_history(client)
        self.assertTrue(client.disconnected)


if __name__ == "__main__":
    unittest.main()
