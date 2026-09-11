from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
import logging
from pathlib import Path
import sys
from typing import Any, Callable, TextIO

from traderrd.application.ingestion import (
    InboundTelegramEvent,
    IngestionOutcome,
    SignalIngestionService,
)
from traderrd.config import ObserverConfig
from traderrd.domain.parser import SignalParseError, SignalParser
from traderrd.infrastructure.sqlite_repository import SQLiteSignalRepository
from traderrd.telegram_source import (
    SourceDecision,
    inspect_seed_source,
    inspect_topic_membership,
    is_non_operational_message,
    message_sender_id,
    resolve_source_entity,
)


LOGGER = logging.getLogger(__name__)
MAX_AUTOMATIC_FLOOD_WAIT_SECONDS = 10
PROGRESS_INTERVAL = 100


class BackfillError(RuntimeError):
    pass


class BackfillAuthorizationError(BackfillError):
    pass


@dataclass(slots=True)
class BackfillCounters:
    scanned: int = 0
    text_messages: int = 0
    stored: int = 0
    duplicate_message: int = 0
    duplicate_signal: int = 0
    invalid: int = 0
    skipped_non_text: int = 0
    skipped_non_signal: int = 0
    source_rejected: int = 0
    errors: int = 0

    def format(self, prefix: str = "Backfill") -> str:
        return (
            f"{prefix}: scanned={self.scanned} text_messages={self.text_messages} "
            f"stored={self.stored} duplicate_message={self.duplicate_message} "
            f"duplicate_signal={self.duplicate_signal} invalid={self.invalid} "
            f"skipped_non_text={self.skipped_non_text} "
            f"skipped_non_signal={self.skipped_non_signal} "
            f"source_rejected={self.source_rejected} errors={self.errors}"
        )


async def backfill_history(
    config: ObserverConfig,
    *,
    limit: int | None = None,
    counters: BackfillCounters | None = None,
    repository: SQLiteSignalRepository | None = None,
    client_factory: Callable[..., Any] | None = None,
    progress: Callable[[BackfillCounters], None] | None = None,
) -> BackfillCounters:
    if limit is not None and limit <= 0:
        raise ValueError("Backfill limit must be a positive integer")

    flood_wait_errors: tuple[type[BaseException], ...] = ()
    rpc_errors: tuple[type[BaseException], ...] = ()
    if client_factory is None:
        try:
            from telethon import TelegramClient, errors
        except ImportError as exc:
            raise RuntimeError(
                "Telethon is not installed. Install the project with: "
                "python -m pip install -e ."
            ) from exc
        factory = TelegramClient
        flood_wait_errors = (errors.FloodWaitError,)
        rpc_errors = (errors.RPCError,)
    else:
        factory = client_factory

    active_counters = counters or BackfillCounters()
    active_repository = repository or SQLiteSignalRepository(config.database_path)
    active_repository.initialize()
    parser = SignalParser()
    service = SignalIngestionService(parser, active_repository)
    checkpoint = active_repository.get_backfill_checkpoint(
        config.source_chat_id, config.source_topic_id
    )

    if client_factory is None and not _session_file(config.session_path).exists():
        raise BackfillAuthorizationError(
            "Authorized Telegram session file was not found. Run 'traderrd telegram-auth' "
            "interactively once before backfill."
        )
    client = factory(
        str(config.session_path),
        config.api_id,
        config.api_hash,
        flood_sleep_threshold=MAX_AUTOMATIC_FLOOD_WAIT_SECONDS,
        request_retries=3,
        connection_retries=3,
    )
    try:
        try:
            await client.connect()
            if not await client.is_user_authorized():
                raise BackfillAuthorizationError(
                    "Telegram session is not authorized. Run 'traderrd telegram-auth' "
                    "interactively once before backfill."
                )
            try:
                source_entity = await resolve_source_entity(
                    client, config.source_chat_id
                )
            except RuntimeError as exc:
                raise BackfillError(str(exc)) from exc
            try:
                _, seed_source = await inspect_seed_source(
                    client,
                    source_entity,
                    topic_id=config.source_topic_id,
                    seed_message_id=config.source_seed_message_id,
                    expected_sender_id=config.expected_sender_id,
                )
            except RuntimeError as exc:
                raise BackfillError(str(exc)) from exc
            if seed_source.decision is not SourceDecision.ACCEPTED:
                raise BackfillError(
                    "Configured seed message failed the source-isolation policy"
                )
            effective_sender_id = config.expected_sender_id or seed_source.sender_id
            await _scan_messages(
                client,
                source_entity,
                config,
                service,
                active_repository,
                active_counters,
                limit,
                checkpoint,
                effective_sender_id,
                parser,
                progress,
            )
        except BackfillError:
            raise
        except flood_wait_errors as exc:
            active_counters.errors += 1
            seconds = max(0, int(getattr(exc, "seconds", 0)))
            raise BackfillError(
                f"Telegram requested a {seconds}-second flood wait. "
                "Backfill stopped safely; re-run it later to resume."
            ) from exc
        except rpc_errors as exc:
            active_counters.errors += 1
            raise BackfillError(
                "Telegram returned an RPC error. Backfill stopped safely; "
                "re-run it later to resume."
            ) from exc
        except (OSError, asyncio.TimeoutError) as exc:
            active_counters.errors += 1
            raise BackfillError(
                "Telegram connection was interrupted. Backfill stopped safely; "
                "re-run it later to resume."
            ) from exc
    finally:
        await client.disconnect()
    return active_counters


async def _scan_messages(
    client: Any,
    source_entity: Any,
    config: ObserverConfig,
    service: SignalIngestionService,
    repository: SQLiteSignalRepository,
    counters: BackfillCounters,
    limit: int | None,
    checkpoint: int,
    effective_sender_id: int | None,
    parser: SignalParser,
    progress: Callable[[BackfillCounters], None] | None,
) -> None:
    async for message in client.iter_messages(
        source_entity,
        limit=limit,
        reverse=True,
        min_id=checkpoint,
        reply_to=config.source_topic_id,
    ):
        counters.scanned += 1
        topic_source = inspect_topic_membership(
            message,
            topic_id=config.source_topic_id,
            scoped_topic_id=config.source_topic_id,
        )
        if topic_source.decision is not SourceDecision.ACCEPTED:
            counters.source_rejected += 1
            counters.errors += 1
            _report_progress(counters, progress)
            raise BackfillError(
                "Source isolation rejected a Telegram message "
                "(reason=outside_topic). Backfill stopped without advancing "
                "its checkpoint; run source inspection before retrying."
            )

        raw_text = getattr(message, "raw_text", None)
        if is_non_operational_message(message, raw_text):
            counters.skipped_non_text += 1
            await _advance_checkpoint(
                repository,
                counters,
                config.source_chat_id,
                config.source_topic_id,
                message.id,
            )
            _report_progress(counters, progress)
            continue

        counters.text_messages += 1
        try:
            parser.parse(raw_text)
        except SignalParseError:
            counters.skipped_non_signal += 1
            await _advance_checkpoint(
                repository,
                counters,
                config.source_chat_id,
                config.source_topic_id,
                message.id,
            )
            _report_progress(counters, progress)
            continue

        if (
            effective_sender_id is not None
            and message_sender_id(message) != effective_sender_id
        ):
            counters.source_rejected += 1
            counters.errors += 1
            _report_progress(counters, progress)
            raise BackfillError(
                "Source isolation rejected a parser-valid Telegram signal "
                "(reason=unexpected_sender). Backfill stopped without advancing "
                "its checkpoint; run source inspection before retrying."
            )

        try:
            received_at = _aware_utc(message.date)
            edited_at = (
                _aware_utc(message.edit_date)
                if getattr(message, "edit_date", None) is not None
                else None
            )
            result = await asyncio.to_thread(
                service.ingest,
                InboundTelegramEvent(
                    source_chat_id=config.source_chat_id,
                    source_message_id=message.id,
                    raw_text=raw_text,
                    telegram_received_at=received_at,
                    source_topic_id=config.source_topic_id,
                    source_sender_id=message_sender_id(message),
                    is_edit=edited_at is not None,
                    telegram_edited_at=edited_at,
                ),
            )
        except Exception as exc:
            counters.errors += 1
            _report_progress(counters, progress)
            raise BackfillError(
                "Local message processing failed. Backfill stopped before advancing "
                "its checkpoint; re-run it after correcting the local error."
            ) from exc

        if result.outcome is IngestionOutcome.STORED:
            counters.stored += 1
        elif result.outcome is IngestionOutcome.DUPLICATE_MESSAGE:
            counters.duplicate_message += 1
        elif result.outcome is IngestionOutcome.DUPLICATE_SIGNAL:
            counters.duplicate_signal += 1
        else:
            counters.invalid += 1
        await _advance_checkpoint(
            repository,
            counters,
            config.source_chat_id,
            config.source_topic_id,
            message.id,
        )
        _report_progress(counters, progress)


async def _advance_checkpoint(
    repository: SQLiteSignalRepository,
    counters: BackfillCounters,
    source_chat_id: int,
    source_topic_id: int,
    source_message_id: int,
) -> None:
    try:
        await asyncio.to_thread(
            repository.advance_backfill_checkpoint,
            source_chat_id,
            source_topic_id,
            source_message_id,
        )
    except Exception as exc:
        counters.errors += 1
        raise BackfillError(
            "The local backfill checkpoint could not be saved. Backfill stopped "
            "safely and can be re-run after correcting the database error."
        ) from exc


def run_backfill(
    config: ObserverConfig,
    *,
    limit: int | None = None,
    output: TextIO = sys.stdout,
    error_output: TextIO = sys.stderr,
) -> int:
    counters = BackfillCounters()

    def progress(current: BackfillCounters) -> None:
        print(current.format("Backfill progress"), file=output)

    exit_code = 0
    try:
        asyncio.run(
            backfill_history(
                config,
                limit=limit,
                counters=counters,
                progress=progress,
            )
        )
    except (KeyboardInterrupt, asyncio.CancelledError):
        LOGGER.info("Backfill stopped by user")
        exit_code = 130
    except BackfillError as exc:
        if counters.errors == 0:
            counters.errors = 1
        print(f"Backfill error: {exc}", file=error_output)
        exit_code = 1
    print(counters.format("Backfill final"), file=output)
    return exit_code


def _report_progress(
    counters: BackfillCounters,
    progress: Callable[[BackfillCounters], None] | None,
) -> None:
    if progress is not None and counters.scanned % PROGRESS_INTERVAL == 0:
        progress(counters)


def _aware_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _session_file(session_path: Path) -> Path:
    if session_path.suffix == ".session":
        return session_path
    return Path(f"{session_path}.session")
