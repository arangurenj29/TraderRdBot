from __future__ import annotations

from decimal import Decimal
from unittest.mock import patch
import unittest

from traderrd.tui import TerminalDashboard, pair_performance_lines, projection_from_status, run_tui


class TuiTests(unittest.TestCase):
    def test_projection_uses_only_active_entry_plans(self) -> None:
        payload = {
            "execution": {"active_intents": [
                {"kind": "entry", "direction": "LONG", "quantity": "2", "price": "100", "take_profit": "105", "stop_loss": "97"},
                {"kind": "entry", "direction": "SHORT", "quantity": "3", "price": "20", "take_profit": "18", "stop_loss": "21"},
                {"kind": "close_position", "direction": "LONG", "quantity": "99", "price": "1", "take_profit": "2", "stop_loss": "0"},
            ]},
            "risk": {"active_reservations": 2, "policy": {"max_reservations": 3}},
        }
        projection = projection_from_status(payload)
        self.assertEqual(projection.target_pnl, Decimal("16"))
        self.assertEqual(projection.stop_pnl, Decimal("-9"))
        self.assertEqual(projection.active_count, 3)
        self.assertEqual(projection.available_slots, 1)

    def test_projection_fails_closed_on_missing_prices(self) -> None:
        result = projection_from_status({"execution": {"active_intents": [{"kind": "entry", "direction": "LONG"}]}, "risk": {}})
        self.assertEqual(result.target_pnl, Decimal("0"))
        self.assertEqual(result.stop_pnl, Decimal("0"))
        self.assertIsNone(result.available_slots)

    def test_pair_performance_rows_include_win_rate_and_net_pnl_with_empty_state(self) -> None:
        payload = {"performance": {"pairs": [
            {"symbol": "BTCUSDT", "closed_trades": 4, "wins": 3, "losses": 1, "win_rate": "0.75", "net_pnl": "12.5"},
            {"symbol": "ETHUSDT", "closed_trades": 2, "wins": 1, "losses": 1, "win_rate": "0.5", "net_pnl": "-2.25"},
        ]}}
        rows = pair_performance_lines(payload, limit=1)
        self.assertEqual(len(rows), 1)
        self.assertIn("BTCUSDT", rows[0])
        self.assertIn("W/L 3/1", rows[0])
        self.assertIn("75.00%", rows[0])
        self.assertIn("12.50 USDT", rows[0])
        self.assertIn("no uniquely attributed", pair_performance_lines({"performance": {"pairs": []}}, limit=3)[0])

    def test_noninteractive_standalone_tui_falls_back_without_starting_curses(self) -> None:
        with patch.object(TerminalDashboard, "supported", return_value=False):
            self.assertEqual(run_tui("missing.sqlite3"), 1)

    def test_terminal_support_requires_both_streams_tty(self) -> None:
        class Tty:
            def isatty(self) -> bool:
                return True
        class Pipe:
            def isatty(self) -> bool:
                return False
        self.assertTrue(TerminalDashboard.supported(Tty(), Tty(), term="xterm-256color"))
        self.assertFalse(TerminalDashboard.supported(Tty(), Tty(), term="dumb"))
        self.assertFalse(TerminalDashboard.supported(Tty(), Pipe(), term="xterm-256color"))


if __name__ == "__main__":
    unittest.main()

class TuiRenderTests(unittest.TestCase):
    class _Screen:
        def __init__(self, *, key: int = -1, height: int = 24, width: int = 100) -> None:
            self.key = key
            self.height = height
            self.width = width
            self.rendered: list[str] = []
            self.refreshed = False
        def getmaxyx(self) -> tuple[int, int]: return self.height, self.width
        def erase(self) -> None: pass
        def addnstr(self, row: int, column: int, text: str, width: int, attr: int = 0) -> None: self.rendered.append(text)
        def refresh(self) -> None: self.refreshed = True
        def getch(self) -> int: return self.key
        def keypad(self, _: bool) -> None: pass
        def nodelay(self, _: bool) -> None: pass

    def _payload(self) -> dict:
        return {
            "overall_status": "healthy",
            "components": {name: {"status": "healthy", "age_seconds": 1} for name in ("observer", "worker", "monitor")},
            "source": {"latest_signal": None}, "worker": {"cursor_lag_messages": 0, "unprocessed_signal_count": 0},
            "execution": {"active_intents": [], "expiry": {"active_entry_count": 0, "next_entry_expiry_at": None}},
            "risk": {"equity": "100", "reserved_risk": "0", "active_reservations": 0, "drawdown": {"current_loss_fraction": "0"}},
            "performance": {"overall": {"net_pnl": "0", "closed_trades": 0, "wins": 0, "losses": 0, "win_rate": None}, "pairs": []},
        }

    def test_render_step_draws_risk_and_dashboard_without_attribute_error(self) -> None:
        screen = self._Screen()
        dashboard = TerminalDashboard("missing.sqlite3")
        dashboard._screen = screen
        dashboard._reader = type("Reader", (), {"read": lambda _: self._payload()})()
        self.assertTrue(dashboard.step(force=True))
        self.assertTrue(screen.refreshed)
        self.assertTrue(any("HEALTH / RISK" in line for line in screen.rendered))
        self.assertTrue(any("BY PAIR" in line for line in screen.rendered))
        self.assertTrue(any("DEMO ONLY" in line for line in screen.rendered))

    def test_question_mark_toggles_help_and_small_screen_renders_safely(self) -> None:
        screen = self._Screen(key=ord("?"), height=18, width=76)
        dashboard = TerminalDashboard("missing.sqlite3")
        dashboard._screen = screen
        dashboard._reader = type("Reader", (), {"read": lambda _: self._payload()})()
        self.assertTrue(dashboard.step(force=True))
        self.assertTrue(dashboard._show_help)
        self.assertTrue(any("HELP" in line for line in screen.rendered))


class TuiColorTests(unittest.TestCase):
    def test_initializes_colors_before_assessing_capabilities(self) -> None:
        dashboard = TerminalDashboard("missing.sqlite3")
        events: list[str] = []
        def start_color() -> None:
            events.append("start")
        def has_colors() -> bool:
            events.append("has")
            return True
        with patch("traderrd.tui.curses.start_color", side_effect=start_color), patch(
            "traderrd.tui.curses.has_colors", side_effect=has_colors
        ), patch("traderrd.tui.curses.COLORS", 8, create=True), patch(
            "traderrd.tui.curses.use_default_colors"
        ), patch("traderrd.tui.curses.init_pair") as init_pair, patch(
            "traderrd.tui.curses.color_pair", side_effect=lambda pair: pair * 10
        ):
            dashboard._initialize_colors()
            self.assertTrue(dashboard._colors_enabled)
            self.assertEqual(events[:2], ["start", "has"])
            self.assertEqual(dashboard._status_attr("healthy"), 20)
            self.assertEqual(dashboard._status_attr("stopped"), 40)
            self.assertEqual(dashboard._pnl_attr("-1"), 40)
        self.assertEqual(init_pair.call_count, 4)

    def test_color_fallback_has_no_terminal_escape_output(self) -> None:
        dashboard = TerminalDashboard("missing.sqlite3")
        with patch("traderrd.tui.curses.has_colors", return_value=False):
            dashboard._initialize_colors()
        self.assertFalse(dashboard._colors_enabled)
        self.assertEqual(dashboard._status_attr("healthy"), 0)
        self.assertEqual(dashboard._risk_attr("0.15"), __import__("curses").A_BOLD)
