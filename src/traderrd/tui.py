"""Read-only full-screen terminal dashboard for the Demo runtime.

The dashboard deliberately reads the local status snapshot only.  It never
loads credentials, opens Telegram/Bybit clients, or writes SQLite.
"""
from __future__ import annotations

from collections import deque
from datetime import datetime
import curses
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
import os
import sys
import time
from typing import Any, Callable, Deque

from traderrd.application.status import TraderStatusReader

_MIN_ROWS = 18
_MIN_COLS = 76

_PAIR_ACCENT = 1
_PAIR_GOOD = 2
_PAIR_WARN = 3
_PAIR_BAD = 4


@dataclass(frozen=True, slots=True)
class Projection:
    target_pnl: Decimal
    stop_pnl: Decimal
    active_count: int
    available_slots: int | None


def projection_from_status(payload: dict[str, Any]) -> Projection:
    """Calculate a hypothetical scenario only from persisted planned TP/SL."""
    target = Decimal("0")
    stop = Decimal("0")
    active = payload.get("execution", {}).get("active_intents", [])
    for intent in active:
        if intent.get("kind") != "entry":
            continue
        price = _decimal(intent.get("price"))
        quantity = _decimal(intent.get("quantity"))
        take_profit = _decimal(intent.get("take_profit"))
        stop_loss = _decimal(intent.get("stop_loss"))
        direction = intent.get("direction")
        if None in (price, quantity, take_profit, stop_loss):
            continue
        multiplier = Decimal("1") if direction == "LONG" else Decimal("-1")
        target += (take_profit - price) * quantity * multiplier
        stop += (stop_loss - price) * quantity * multiplier
    risk = payload.get("risk", {})
    policy = risk.get("policy") or {}
    max_slots = policy.get("max_reservations")
    used = risk.get("active_reservations")
    available = None
    try:
        if max_slots is not None and used is not None:
            available = max(0, int(max_slots) - int(used))
    except (TypeError, ValueError):
        pass
    return Projection(target, stop, len(active), available)




def pair_performance_lines(payload: dict[str, Any], *, limit: int) -> list[str]:
    """Return compact, safe per-pair ledger rows for the constrained TUI."""
    pairs = payload.get("performance", {}).get("pairs", [])
    if not isinstance(pairs, list) or not pairs:
        return ["Per pair: no uniquely attributed closed trades yet"]
    rows: list[str] = []
    for pair in pairs[: max(1, limit)]:
        if not isinstance(pair, dict):
            continue
        symbol = str(pair.get("symbol", "unknown"))[:12]
        rows.append(
            f"{symbol:<12} {pair.get('closed_trades', 0):>3} trades · "
            f"W/L {pair.get('wins', 0)}/{pair.get('losses', 0)} · "
            f"win {_percent(pair.get('win_rate'))} · net {_money(pair.get('net_pnl'))}"
        )
    return rows or ["Per pair: no readable attributed ledger rows"]


def _percent(value: Any) -> str:
    parsed = _decimal(value)
    return "unknown" if parsed is None else f"{parsed * Decimal('100'):.2f}%"

def _decimal(value: Any) -> Decimal | None:
    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
    return result if result.is_finite() else None


def _money(value: Decimal | Any) -> str:
    parsed = value if isinstance(value, Decimal) else _decimal(value)
    return "unknown" if parsed is None else f"{parsed:,.2f} USDT"


class TerminalDashboard:
    """Small curses view owned by the foreground supervisor."""

    def __init__(
        self,
        database_path: str | Path,
        *,
        refresh_seconds: float = 1.0,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._reader = TraderStatusReader(database_path)
        self._refresh_seconds = refresh_seconds
        self._clock = clock
        self._screen: Any | None = None
        self._logs: Deque[str] = deque(maxlen=200)
        self._show_logs = True
        self._last_refresh = 0.0
        self._payload: dict[str, Any] | None = None
        self._quit = False
        self._show_help = False
        self._colors_enabled = False
        self._pulse = False
        self._refreshed_at: datetime | None = None

    @staticmethod
    def supported(
        input_stream: Any = sys.stdin,
        output_stream: Any = sys.stdout,
        *,
        term: str | None = None,
    ) -> bool:
        terminal = (term if term is not None else os.getenv("TERM", "")).strip().lower()
        return bool(
            getattr(input_stream, "isatty", lambda: False)()
            and getattr(output_stream, "isatty", lambda: False)()
            and terminal not in {"", "dumb", "unknown"}
        )

    @staticmethod
    def unavailable_reason() -> str:
        terminal = os.getenv("TERM", "").strip() or "unset"
        if not (getattr(sys.stdin, "isatty", lambda: False)() and getattr(sys.stdout, "isatty", lambda: False)()):
            return "stdin/stdout are not interactive TTY streams"
        if terminal.lower() in {"dumb", "unknown"}:
            return f"TERM={terminal!r} has no supported full-screen capability"
        return "terminal does not support the full-screen dashboard"

    def start(self) -> None:
        self._screen = curses.initscr()
        curses.noecho()
        curses.cbreak()
        self._initialize_colors()
        self._screen.keypad(True)
        self._screen.nodelay(True)
        try:
            curses.curs_set(0)
        except curses.error:
            pass

    def close(self) -> None:
        if self._screen is None:
            return
        try:
            self._screen.keypad(False)
            curses.nocbreak()
            curses.echo()
            curses.endwin()
        finally:
            self._screen = None

    def log(self, line: str) -> None:
        self._logs.append(line)

    def step(self, *, force: bool = False) -> bool:
        if self._screen is None:
            return True
        now = self._clock()
        if force or self._payload is None or now - self._last_refresh >= self._refresh_seconds:
            self._payload = self._reader.read()
            self._last_refresh = now
            self._pulse = not self._pulse
            self._refreshed_at = datetime.now()
        key = self._screen.getch()
        if key in (ord("q"), ord("Q")):
            self._quit = True
        elif key in (ord("l"), ord("L")):
            self._show_logs = not self._show_logs
        elif key in (ord("r"), ord("R")):
            self._payload = self._reader.read()
            self._last_refresh = now
            self._pulse = not self._pulse
            self._refreshed_at = datetime.now()
        elif key == ord("?"):
            self._show_help = not self._show_help
        self._render()
        return not self._quit

    def _render(self) -> None:
        assert self._screen is not None
        screen = self._screen
        height, width = screen.getmaxyx()
        screen.erase()
        if height < _MIN_ROWS or width < _MIN_COLS:
            self._add(0, 0, f"TraderRd dashboard needs at least {_MIN_COLS}x{_MIN_ROWS}; current {width}x{height}.", curses.A_BOLD)
            self._add(2, 0, "Resize this terminal, or press q to stop all Demo components.")
            self._footer(height, width)
            screen.refresh()
            return
        payload = self._payload or self._reader.read()
        projection = projection_from_status(payload)
        overall = str(payload.get("overall_status", "unknown"))
        title_attr = curses.A_BOLD | self._status_attr(overall)
        self._add(0, 0, f" TRADERRD  ·  {overall.upper()} ", curses.A_REVERSE | title_attr)
        self._add(0, max(0, width - 26), "[ DEMO ONLY · NO MAINNET ]", self._accent_attr() | curses.A_BOLD)
        refreshed = self._refreshed_at.strftime("%H:%M:%S") if self._refreshed_at else "starting"
        pulse = "●" if self._pulse else "○"
        self._add(1, 1, f"{pulse} LIVE · refreshed {refreshed} · local SQLite read-only", curses.A_DIM | self._accent_attr())
        self._divider(2, width)
        if self._show_help:
            self._add(3, 1, "HELP  q stop all Demo components · l toggle logs · r refresh · ? close help", self._accent_attr() | curses.A_BOLD)
        components = payload.get("components", {})
        component_text = "  ".join(
            f"{name[:3].upper()} {str(components.get(name, {}).get('status', 'unknown')).upper()} {self._age(components.get(name, {}).get('age_seconds'))}"
            for name in ("observer", "worker", "monitor")
        )
        risk = payload.get("risk", {})
        drawdown = risk.get("drawdown", {}).get("current_loss_fraction")
        health_row = 4 if self._show_help else 3
        self._add(health_row, 1, "HEALTH / RISK", self._accent_attr() | curses.A_BOLD)
        self._add(health_row, 17, component_text, self._status_attr(overall))
        self._add(health_row + 1, 3, f"Equity {_money(risk.get('equity'))}  ·  Reserved {_money(risk.get('reserved_risk'))}  ·  Slots {risk.get('active_reservations', 0)}  ·  Drawdown {_percent(drawdown)}", self._risk_attr(drawdown))
        source = payload.get("source", {}).get("latest_signal")
        signal = "none yet" if not source else f"#{source['message_id']} {source['symbol']} {source['direction']}"
        worker = payload.get("worker", {})
        signal_row = health_row + 3
        self._divider(signal_row - 1, width)
        self._add(signal_row, 1, "SIGNAL FLOW", self._accent_attr() | curses.A_BOLD)
        self._add(signal_row, 15, f"{signal}  ·  lag {worker.get('cursor_lag_messages', '?')}  ·  queued {worker.get('unprocessed_signal_count', '?')}")
        execution = payload.get("execution", {})
        expiry = execution.get("expiry", {})
        intents = execution.get("active_intents", [])
        execution_row = signal_row + 2
        self._add(execution_row, 1, "EXECUTION", self._accent_attr() | curses.A_BOLD)
        self._add(execution_row, 15, f"active {len(intents)}  ·  pending {expiry.get('active_entry_count', 0)}  ·  expiry {expiry.get('next_entry_expiry_at') or 'none'}")
        row = execution_row + 1
        max_intent_rows = 2 if height < 24 else 3
        for intent in intents[:max_intent_rows]:
            self._add(row, 3, f"{intent.get('symbol')} {intent.get('direction')} · {intent.get('state')} · entry {intent.get('price') or '-'} · TP {intent.get('take_profit') or '-'} · SL {intent.get('stop_loss') or '-'}")
            row += 1
        performance_row = max(row + 1, 12)
        self._divider(performance_row - 1, width)
        performance = payload.get("performance", {}).get("overall", {})
        self._add(performance_row, 1, "PERFORMANCE", self._accent_attr() | curses.A_BOLD)
        self._add(performance_row, 16, f"net {_money(performance.get('net_pnl'))} · trades {performance.get('closed_trades', 0)} · W/L {performance.get('wins', 0)}/{performance.get('losses', 0)} · win {_percent(performance.get('win_rate'))}", self._pnl_attr(performance.get('net_pnl')))
        footer_row = height - 1
        pair_header = performance_row + 1
        self._add(pair_header, 3, "BY PAIR", curses.A_BOLD)
        pair_limit = max(1, min(4, footer_row - pair_header - 4))
        pair_rows = pair_performance_lines(payload, limit=pair_limit)
        for offset, pair_line in enumerate(pair_rows):
            self._add(pair_header + 1 + offset, 5, pair_line, self._pnl_attr_from_line(pair_line))
        projection_row = pair_header + 1 + len(pair_rows)
        projection_text = f"PROJECTION · HYPOTHETICAL planned TP/SL only: target {_money(projection.target_pnl)} · stop {_money(projection.stop_pnl)} · capacity {projection.available_slots if projection.available_slots is not None else '?'} slots"
        self._add(projection_row, 1, projection_text, curses.A_DIM | self._accent_attr())
        logs_start = projection_row + 2
        if self._show_logs and logs_start < footer_row:
            self._logs_view(logs_start, footer_row, width)
        self._footer(height, width)
        screen.refresh()

    def _initialize_colors(self) -> None:
        try:
            # curses publishes COLORS only after start_color() on several
            # terminals (including macOS Terminal). Assess capability after it.
            curses.start_color()
            if not curses.has_colors() or getattr(curses, "COLORS", 0) < 8:
                return
            try:
                curses.use_default_colors()
            except curses.error:
                pass
            curses.init_pair(_PAIR_ACCENT, curses.COLOR_CYAN, -1)
            curses.init_pair(_PAIR_GOOD, curses.COLOR_GREEN, -1)
            curses.init_pair(_PAIR_WARN, curses.COLOR_YELLOW, -1)
            curses.init_pair(_PAIR_BAD, curses.COLOR_RED, -1)
            self._colors_enabled = True
        except curses.error:
            self._colors_enabled = False

    def _color(self, pair: int) -> int:
        return curses.color_pair(pair) if self._colors_enabled else 0

    def _accent_attr(self) -> int:
        return self._color(_PAIR_ACCENT)

    def _status_attr(self, status: Any) -> int:
        if status == "healthy":
            return self._color(_PAIR_GOOD)
        if status in {"degraded", "not_started"}:
            return self._color(_PAIR_WARN)
        return self._color(_PAIR_BAD)

    def _pnl_attr(self, value: Any) -> int:
        parsed = _decimal(value)
        if parsed is None or parsed == 0:
            return 0
        return self._color(_PAIR_GOOD if parsed > 0 else _PAIR_BAD)

    def _pnl_attr_from_line(self, line: str) -> int:
        # The display row is derived from sanitized local status. A leading
        # minus in its net field is sufficient for terminal-only emphasis.
        return self._color(_PAIR_BAD) if "net -" in line else self._color(_PAIR_GOOD) if "net " in line and "unknown" not in line else 0

    def _risk_attr(self, drawdown: Any) -> int:
        value = _decimal(drawdown)
        if value is None:
            return self._color(_PAIR_WARN)
        if value >= Decimal("0.15"):
            return self._color(_PAIR_BAD) | curses.A_BOLD
        if value >= Decimal("0.08"):
            return self._color(_PAIR_WARN) | curses.A_BOLD
        return self._color(_PAIR_GOOD)

    def _divider(self, row: int, width: int) -> None:
        self._add(row, 1, "─" * max(1, width - 2), self._accent_attr() | curses.A_DIM)

    def _logs_view(self, start: int, end: int, width: int) -> None:
        self._add(start, 1, "LIVE LOGS", curses.A_BOLD)
        lines = list(self._logs)[-(max(0, end - start - 1)):]
        for offset, line in enumerate(lines, start=1):
            self._add(start + offset, 2, line[: max(1, width - 4)])

    def _footer(self, height: int, width: int) -> None:
        suffix = "logs on" if self._show_logs else "logs hidden"
        self._add(height - 1, 1, f"q quit all components · l toggle logs · r refresh · ? help · {suffix}"[: max(1, width - 2)], curses.A_REVERSE)

    def _add(self, row: int, column: int, text: str, attr: int = 0) -> None:
        assert self._screen is not None
        try:
            self._screen.addnstr(row, column, text, max(1, self._screen.getmaxyx()[1] - column - 1), attr)
        except curses.error:
            pass

    @staticmethod
    def _age(value: Any) -> str:
        try:
            return f"{int(float(value))}s"
        except (TypeError, ValueError):
            return "?"

    @staticmethod
    def _health_attr(status: Any) -> int:
        return curses.A_BOLD if status == "healthy" else curses.A_REVERSE


def run_tui(database_path: str | Path) -> int:
    """Run the read-only dashboard independently, when useful for observation."""
    if not TerminalDashboard.supported():
        print(
            "TraderRd TUI unavailable: " + TerminalDashboard.unavailable_reason()
            + "; use 'traderrd status' instead."
        )
        return 1
    dashboard = TerminalDashboard(database_path)
    try:
        dashboard.start()
        while dashboard.step():
            time.sleep(0.1)
        return 0
    except KeyboardInterrupt:
        return 130
    finally:
        dashboard.close()
