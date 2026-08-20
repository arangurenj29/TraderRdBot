from __future__ import annotations

import os
from pathlib import Path
import sys

from traderrd.application.status import TraderStatusReader, render_status, status_json


def _should_color(*, as_json: bool, no_color: bool) -> bool:
    """Return whether the human terminal view may use ANSI styling."""
    return not as_json and not no_color and not os.getenv("NO_COLOR") and sys.stdout.isatty()


def run_status(
    database_path: str | Path,
    as_json: bool = False,
    no_color: bool = False,
) -> int:
    """Print the local read-only status snapshot."""
    payload = TraderStatusReader(database_path).read()
    print(
        status_json(payload)
        if as_json
        else render_status(payload, color=_should_color(as_json=as_json, no_color=no_color))
    )
    return 0
