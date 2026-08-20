from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import sys
from typing import TextIO

from traderrd.application.ingestion import IngestionOutcome
from traderrd.domain.parser import SignalParseError, SignalParser
from traderrd.infrastructure.sqlite_repository import SQLiteSignalRepository


@dataclass(slots=True)
class AuditReprocessingCounters:
    scanned: int = 0
    stored: int = 0
    duplicate_signal: int = 0
    invalid: int = 0
    errors: int = 0

    def format(self) -> str:
        return (
            f"Audit reprocessing: scanned={self.scanned} stored={self.stored} "
            f"duplicate_signal={self.duplicate_signal} invalid={self.invalid} "
            f"errors={self.errors}"
        )


class AuditReprocessingService:
    def __init__(
        self,
        repository: SQLiteSignalRepository,
        parser: SignalParser | None = None,
    ) -> None:
        self._repository = repository
        self._parser = parser or SignalParser()

    def run(self, limit: int | None = None) -> AuditReprocessingCounters:
        if limit is not None and limit <= 0:
            raise ValueError("Audit reprocessing limit must be a positive integer")

        counters = AuditReprocessingCounters()
        for event in self._repository.list_reprocessable_messages(limit):
            counters.scanned += 1
            try:
                draft = self._parser.parse(event.raw_text)
                parse_error = None
            except SignalParseError as exc:
                draft = None
                parse_error = exc

            try:
                result = self._repository.reprocess_event(event, draft, parse_error)
            except Exception:
                counters.errors += 1
                continue

            if result.outcome is IngestionOutcome.STORED:
                counters.stored += 1
            elif result.outcome is IngestionOutcome.DUPLICATE_SIGNAL:
                counters.duplicate_signal += 1
            else:
                counters.invalid += 1
        return counters


def run_audit_reprocessing(
    database_path: str | Path,
    *,
    limit: int | None = None,
    output: TextIO | None = None,
    error_output: TextIO | None = None,
) -> int:
    output = output or sys.stdout
    error_output = error_output or sys.stderr
    path = Path(database_path)
    if not path.is_file():
        print(
            "Audit reprocessing error: database file does not exist. "
            "Pass the correct path with --database-path.",
            file=error_output,
        )
        return 1

    repository = SQLiteSignalRepository(path)
    repository.initialize()
    counters = AuditReprocessingService(repository).run(limit)
    print(counters.format(), file=output)
    return 1 if counters.errors else 0
