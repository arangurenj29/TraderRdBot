from __future__ import annotations

import asyncio
from dataclasses import dataclass
from pathlib import Path
import sys
from typing import Any, Callable, TextIO

from traderrd.config import ObserverConfig
from traderrd.domain.parser import SignalParseError, SignalParser
from traderrd.telegram_source import inspect_seed_source, resolve_source_entity


class SourcePreflightError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class SourcePreflightReport:
    chat_id: int
    topic_id: int
    seed_message_id: int
    source_decision: str
    parser_eligible: bool
    parser_error_code: str | None
    observed_sender_id: int | None
    expected_sender_configured: bool

    def format(self) -> str:
        return (
            f"Source inspection: chat_id={self.chat_id} topic_id={self.topic_id} "
            f"seed_message_id={self.seed_message_id} "
            f"source_decision={self.source_decision} "
            f"parser_eligible={str(self.parser_eligible).lower()} "
            f"parser_error_code={self.parser_error_code or 'none'} "
            f"observed_sender_id={self.observed_sender_id or 'unavailable'} "
            "sender_guard_available="
            f"{str(self.observed_sender_id is not None).lower()} "
            "expected_sender_configured="
            f"{str(self.expected_sender_configured).lower()}"
        )


async def inspect_configured_source(
    config: ObserverConfig,
    *,
    client_factory: Callable[..., Any] | None = None,
) -> SourcePreflightReport:
    if client_factory is None:
        try:
            from telethon import TelegramClient
        except ImportError as exc:
            raise SourcePreflightError(
                "Telethon is not installed. Install the project with: "
                "python -m pip install -e ."
            ) from exc
        if not _session_file(config.session_path).exists():
            raise SourcePreflightError(
                "Authorized Telegram session file was not found. Run 'traderrd run' "
                "interactively once before source inspection."
            )
        factory = TelegramClient
    else:
        factory = client_factory

    client = factory(str(config.session_path), config.api_id, config.api_hash)
    try:
        await client.connect()
        if not await client.is_user_authorized():
            raise SourcePreflightError(
                "Telegram session is not authorized. Run 'traderrd run' "
                "interactively once before source inspection."
            )
        try:
            source_entity = await resolve_source_entity(client, config.source_chat_id)
        except RuntimeError as exc:
            raise SourcePreflightError(str(exc)) from exc
        try:
            message, source = await inspect_seed_source(
                client,
                source_entity,
                topic_id=config.source_topic_id,
                seed_message_id=config.source_seed_message_id,
                expected_sender_id=config.expected_sender_id,
            )
        except RuntimeError as exc:
            raise SourcePreflightError(str(exc)) from exc
        parser_eligible = False
        parser_error_code: str | None = None
        raw_text = getattr(message, "raw_text", None)
        try:
            SignalParser().parse(raw_text if isinstance(raw_text, str) else "")
            parser_eligible = True
        except SignalParseError as exc:
            parser_error_code = exc.code

        return SourcePreflightReport(
            chat_id=config.source_chat_id,
            topic_id=config.source_topic_id,
            seed_message_id=config.source_seed_message_id,
            source_decision=source.decision.value,
            parser_eligible=parser_eligible,
            parser_error_code=parser_error_code,
            observed_sender_id=source.sender_id,
            expected_sender_configured=config.expected_sender_id is not None,
        )
    finally:
        try:
            await client.disconnect()
        except Exception:
            pass


def run_source_inspection(
    config: ObserverConfig,
    *,
    output: TextIO | None = None,
    error_output: TextIO | None = None,
) -> int:
    output = output or sys.stdout
    error_output = error_output or sys.stderr
    try:
        report = asyncio.run(inspect_configured_source(config))
    except (KeyboardInterrupt, asyncio.CancelledError):
        return 130
    except SourcePreflightError as exc:
        print(f"Source inspection error: {exc}", file=error_output)
        return 1
    except Exception:
        print(
            "Source inspection error: validation could not be completed safely",
            file=error_output,
        )
        return 1
    print(report.format(), file=output)
    return int(report.source_decision != "accepted" or not report.parser_eligible)


def _session_file(session_path: Path) -> Path:
    if session_path.suffix == ".session":
        return session_path
    return Path(f"{session_path}.session")
