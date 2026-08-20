from __future__ import annotations

import asyncio
from datetime import datetime, timezone
import logging
import sys
import time
from typing import Any

from traderrd.application.ingestion import InboundTelegramEvent, SignalIngestionService
from traderrd.config import ObserverConfig
from traderrd.domain.parser import SignalParseError, SignalParser
from traderrd.infrastructure.bybit_market_data import BybitPublicMarketDataClient
from traderrd.infrastructure.heartbeat_repository import SafeHeartbeat
from traderrd.infrastructure.sqlite_repository import SQLiteSignalRepository
from traderrd.telegram_source import (
    SourceDecision,
    inspect_seed_source,
    inspect_topic_membership,
    is_non_operational_message,
    message_sender_id,
    resolve_source_entity,
    SourceIsolationError,
)


LOGGER = logging.getLogger(__name__)

_RECONNECT_MAX_DELAY_SECONDS = 60.0


def _retry_delay(attempt: int) -> float:
    """Return a bounded exponential delay for a transport retry."""
    return min(_RECONNECT_MAX_DELAY_SECONDS, float(2 ** min(attempt - 1, 6)))


def _is_retryable_transport_error(error: BaseException) -> bool:
    """Retry transient network failures, but never sandbox permission failures."""
    if isinstance(error, PermissionError):
        return False
    return isinstance(error, (ConnectionError, TimeoutError, OSError))


async def run_observer(config: ObserverConfig) -> None:
    heartbeat = SafeHeartbeat(config.database_path, "observer")
    heartbeat.ensure()
    heartbeat.started("startup")
    try:
        from telethon import TelegramClient, events
    except ImportError as exc:
        heartbeat.error("telethon_unavailable", "startup")
        raise RuntimeError(
            "Telethon is not installed. Install the project with: python -m pip install -e ."
        ) from exc

    config.session_path.parent.mkdir(parents=True, exist_ok=True)
    repository = SQLiteSignalRepository(config.database_path)
    repository.initialize()
    quote_provider = BybitPublicMarketDataClient(
        base_url=config.bybit_public_base_url,
        category=config.bybit_category,
        timeout_seconds=config.bybit_timeout_seconds,
    )
    service = SignalIngestionService(SignalParser(), repository, quote_provider)

    client = TelegramClient(str(config.session_path), config.api_id, config.api_hash)
    try:
        await client.connect()
        if not await client.is_user_authorized():
            if not sys.stdin.isatty() or not sys.stdout.isatty():
                raise RuntimeError(
                    "First-time Telegram authorization requires an interactive terminal"
                )
            await client.start()
        await _listen(client, events, config, service, heartbeat)
    except (KeyboardInterrupt, asyncio.CancelledError):
        heartbeat.stopped("user_interrupt")
        raise
    except BaseException as exc:
        heartbeat.error(_runtime_error_code(exc, "observer_error"), "runtime")
        raise
    else:
        heartbeat.stopped("disconnected")
    finally:
        await client.disconnect()


async def _listen(
    client: Any,
    events: Any,
    config: ObserverConfig,
    service: SignalIngestionService,
    heartbeat: SafeHeartbeat | None = None,
) -> None:
    source_entity = await resolve_source_entity(client, config.source_chat_id)
    _, seed_source = await inspect_seed_source(
        client,
        source_entity,
        topic_id=config.source_topic_id,
        seed_message_id=config.source_seed_message_id,
        expected_sender_id=config.expected_sender_id,
    )
    if seed_source.decision is not SourceDecision.ACCEPTED:
        raise RuntimeError(
            "Configured seed message failed the source-isolation policy"
        )
    effective_sender_id = config.expected_sender_id or seed_source.sender_id
    if heartbeat is not None:
        heartbeat.healthy("source_validated")
    parser = SignalParser()

    async def ingest_event(event: object, is_edit: bool) -> None:
        message = event.message
        raw_text = event.raw_text
        topic_source = inspect_topic_membership(
            message,
            topic_id=config.source_topic_id,
        )
        if topic_source.decision is not SourceDecision.ACCEPTED:
            LOGGER.info(
                "Message ignored chat_id=%s message_id=%s "
                "source_decision=outside_topic",
                config.source_chat_id,
                getattr(message, "id", None),
            )
            return
        if is_non_operational_message(message, raw_text):
            LOGGER.info(
                "Message ignored chat_id=%s message_id=%s "
                "source_decision=non_operational",
                config.source_chat_id,
                getattr(message, "id", None),
            )
            return

        try:
            parser.parse(raw_text)
        except SignalParseError as exc:
            LOGGER.info(
                "Message ignored chat_id=%s message_id=%s "
                "source_decision=non_signal parser_error_code=%s",
                config.source_chat_id,
                getattr(message, "id", None),
                exc.code,
            )
            return

        if (
            effective_sender_id is not None
            and message_sender_id(message) != effective_sender_id
        ):
            raise SourceIsolationError(
                "Parser-valid Telegram signal failed the numeric sender policy"
            )
        received_at = _aware_utc(message.date)
        edited_at = _aware_utc(message.edit_date) if message.edit_date else None
        result = await asyncio.to_thread(
            service.ingest,
            InboundTelegramEvent(
                source_chat_id=config.source_chat_id,
                source_message_id=message.id,
                raw_text=raw_text,
                telegram_received_at=received_at,
                source_topic_id=config.source_topic_id,
                source_sender_id=message_sender_id(message),
                is_edit=is_edit,
                telegram_edited_at=edited_at,
            )
        )
        LOGGER.info(
            "Ingestion outcome=%s chat_id=%s message_id=%s fingerprint=%s "
            "error_code=%s quote_outcome=%s quote_error_code=%s",
            result.outcome.value,
            result.source_chat_id,
            result.source_message_id,
            result.fingerprint,
            result.error_code,
            result.quote_outcome.value if result.quote_outcome else None,
            result.quote_error_code,
        )
        if heartbeat is not None:
            heartbeat.healthy(
                "ingestion",
                outcome=result.outcome.value,
            )

    async def on_new_message(event: object) -> None:
        await ingest_event(event, is_edit=False)

    async def on_edited_message(event: object) -> None:
        await ingest_event(event, is_edit=True)

    client.add_event_handler(on_new_message, events.NewMessage(chats=source_entity))
    client.add_event_handler(on_edited_message, events.MessageEdited(chats=source_entity))
    LOGGER.info(
        "Observer started chat_id=%s topic_id=%s expected_sender_configured=%s",
        config.source_chat_id,
        config.source_topic_id,
        effective_sender_id is not None,
    )
    pulse_task: asyncio.Task[None] | None = None
    if heartbeat is not None:
        pulse_task = asyncio.create_task(_heartbeat_pulse(heartbeat))
    try:
        await client.run_until_disconnected()
    finally:
        if pulse_task is not None:
            pulse_task.cancel()
            await asyncio.gather(pulse_task, return_exceptions=True)


def run(config: ObserverConfig) -> int:
    retry_attempt = 0
    while True:
        try:
            asyncio.run(run_observer(config))
        except (KeyboardInterrupt, asyncio.CancelledError):
            LOGGER.info("Observer stopped by user")
            return 130
        except SourceIsolationError as exc:
            LOGGER.error("Observer stopped: %s", exc)
            return 1
        except PermissionError:
            LOGGER.error(
                "Observer network permission denied; run it from a terminal "
                "with outbound network access"
            )
            return 1
        except (ConnectionError, TimeoutError, OSError) as exc:
            if not _is_retryable_transport_error(exc):
                LOGGER.error("Observer stopped: non-retryable transport error")
                return 1
            retry_attempt += 1
            delay = _retry_delay(retry_attempt)
            LOGGER.warning(
                "Observer transport failure (%s); retrying in %.0fs (attempt=%d)",
                type(exc).__name__,
                delay,
                retry_attempt,
            )
            time.sleep(delay)
            continue
        return 0


def _aware_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


async def _heartbeat_pulse(heartbeat: SafeHeartbeat) -> None:
    """Keep an idle observer fresh without touching Telegram or exchange state."""
    try:
        while True:
            await asyncio.sleep(30.0)
            await asyncio.to_thread(heartbeat.healthy, "observer_loop")
    except asyncio.CancelledError:
        raise


def _runtime_error_code(error: BaseException, fallback: str) -> str:
    code = getattr(error, "code", None)
    return str(code) if code else fallback
