from contextlib import closing
from datetime import datetime, timezone
from io import StringIO
from pathlib import Path
import sqlite3
import tempfile
import unittest

from traderrd.application.ingestion import InboundTelegramEvent, SignalIngestionService
from traderrd.audit_reprocess import (
    AuditReprocessingService,
    run_audit_reprocessing,
)
from traderrd.domain.parser import SignalParser
from traderrd.infrastructure.sqlite_repository import SQLiteSignalRepository
from tests.samples import LONG_SIGNAL


class AuditReprocessingTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.database_path = (
            Path(self.temporary_directory.name) / "audit-reprocessing.sqlite3"
        )
        self.repository = SQLiteSignalRepository(self.database_path)
        self.repository.initialize()
        self.ingestion = SignalIngestionService(SignalParser(), self.repository)

    def tearDown(self) -> None:
        self.temporary_directory.cleanup()

    @staticmethod
    def event(message_id: int, raw_text: str) -> InboundTelegramEvent:
        return InboundTelegramEvent(
            source_chat_id=2180632014,
            source_message_id=message_id,
            raw_text=raw_text,
            telegram_received_at=datetime(2026, 8, 16, 2, 15, tzinfo=timezone.utc),
        )

    def update_inbound(self, message_id: int, **values: str) -> None:
        assignments = ", ".join(f"{name} = ?" for name in values)
        with closing(sqlite3.connect(self.database_path)) as connection:
            connection.execute(
                f"UPDATE inbound_messages SET {assignments} "
                "WHERE source_chat_id = ? AND source_message_id = ?",
                (*values.values(), 2180632014, message_id),
            )
            connection.commit()

    def test_recovers_valid_message_without_requesting_market_quotes(self) -> None:
        self.ingestion.ingest(self.event(100, "legacy unparsed placeholder"))
        self.update_inbound(100, raw_text=LONG_SIGNAL)

        counters = AuditReprocessingService(self.repository).run()

        self.assertEqual(counters.scanned, 1)
        self.assertEqual(counters.stored, 1)
        self.assertEqual(counters.errors, 0)
        self.assertEqual(self.repository.count("signals"), 1)
        self.assertEqual(self.repository.count("market_quote_captures"), 0)
        self.assertEqual(self.repository.count("audit_reprocessing_attempts"), 1)
        self.assertEqual(
            self.repository.get_inbound(2180632014, 100)["outcome"], "stored"
        )

    def test_restores_legacy_duplicate_outcome_when_signal_already_exists(self) -> None:
        self.ingestion.ingest(self.event(101, LONG_SIGNAL))
        self.update_inbound(101, outcome="duplicate_message")

        counters = AuditReprocessingService(self.repository).run()

        self.assertEqual(counters.stored, 1)
        self.assertEqual(self.repository.count("signals"), 1)
        self.assertEqual(
            self.repository.get_inbound(2180632014, 101)["outcome"], "stored"
        )

    def test_reprocessing_is_safe_for_invalid_and_duplicate_signal_rows(self) -> None:
        self.ingestion.ingest(self.event(102, LONG_SIGNAL))
        self.ingestion.ingest(self.event(103, "ordinary historical message"))
        self.ingestion.ingest(self.event(104, "legacy unparsed placeholder"))
        self.update_inbound(104, raw_text=LONG_SIGNAL)

        counters = AuditReprocessingService(self.repository).run()

        self.assertEqual(counters.scanned, 2)
        self.assertEqual(counters.invalid, 1)
        self.assertEqual(counters.duplicate_signal, 1)
        self.assertEqual(self.repository.count("signals"), 1)
        self.assertEqual(
            self.repository.get_inbound(2180632014, 103)["outcome"], "invalid"
        )
        self.assertEqual(
            self.repository.get_inbound(2180632014, 104)["outcome"],
            "duplicate_signal",
        )

    def test_command_output_is_aggregate_only(self) -> None:
        private_text = "PRIVATE HISTORICAL MESSAGE"
        self.ingestion.ingest(self.event(105, private_text))
        output = StringIO()
        error_output = StringIO()

        exit_code = run_audit_reprocessing(
            self.database_path,
            output=output,
            error_output=error_output,
        )

        rendered = output.getvalue() + error_output.getvalue()
        self.assertEqual(exit_code, 0)
        self.assertIn("scanned=1", rendered)
        self.assertNotIn(private_text, rendered)


if __name__ == "__main__":
    unittest.main()
