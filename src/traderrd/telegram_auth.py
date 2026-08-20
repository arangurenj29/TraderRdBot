from __future__ import annotations

import asyncio
from pathlib import Path
import sys
from typing import Any, Callable, TextIO

from traderrd.config import ObserverConfig


class TelegramAuthorizationError(RuntimeError):
    """A Telegram session could not be authorized safely."""


async def authorize_telegram(
    config: ObserverConfig,
    *,
    client_factory: Callable[..., Any] | None = None,
) -> None:
    """Authorize and verify the configured Telegram user session only.

    This bootstrap deliberately creates no database, observer, worker, monitor, or
    Bybit client. It only persists Telegram's own session at ``session_path``.
    """
    if client_factory is None:
        try:
            from telethon import TelegramClient
        except ImportError as exc:
            raise TelegramAuthorizationError(
                "Telethon is not installed. Install the project package first."
            ) from exc
        client_factory = TelegramClient

    config.session_path.parent.mkdir(parents=True, exist_ok=True)
    client = client_factory(str(config.session_path), config.api_id, config.api_hash)
    try:
        await client.connect()
        if not await client.is_user_authorized():
            await client.start()
        if not await client.is_user_authorized():
            raise TelegramAuthorizationError("Telegram authorization was not completed")
    finally:
        try:
            await client.disconnect()
        except Exception:
            pass


def run_telegram_auth(
    config: ObserverConfig,
    *,
    stdin: TextIO | None = None,
    stdout: TextIO | None = None,
    client_factory: Callable[..., Any] | None = None,
) -> int:
    """Run interactive Telegram bootstrap without starting trading components."""
    stdin = stdin or sys.stdin
    stdout = stdout or sys.stdout
    if not stdin.isatty() or not stdout.isatty():
        print("Telegram authorization requires an interactive terminal", file=sys.stderr)
        return 2
    try:
        asyncio.run(authorize_telegram(config, client_factory=client_factory))
    except (KeyboardInterrupt, asyncio.CancelledError):
        return 130
    except TelegramAuthorizationError as exc:
        print(f"Telegram authorization error: {exc}", file=sys.stderr)
        return 1
    except Exception:
        print("Telegram authorization error: authorization could not be completed safely", file=sys.stderr)
        return 1
    print(f"Telegram authorization verified; session stored at {Path(config.session_path)}", file=stdout)
    return 0
