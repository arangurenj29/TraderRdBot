from datetime import datetime, timedelta, timezone
from io import StringIO
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from traderrd.application.ingestion import InboundTelegramEvent, SignalIngestionService
from traderrd.application.status import TraderStatusReader, render_status, _component_snapshot, _recent_errors
from traderrd.status_cli import _should_color
from traderrd.cli import main
from traderrd.domain.parser import SignalParser
from traderrd.infrastructure.bridge_repository import SQLiteDemoBridgeRepository
from traderrd.infrastructure.heartbeat_repository import SQLiteHeartbeatRepository
from traderrd.infrastructure.sqlite_repository import SQLiteSignalRepository
from tests.samples import LONG_SIGNAL


UTC = timezone.utc
NOW = datetime(2026, 8, 19, 5, 0, tzinfo=UTC)


class TraderStatusTests(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.database = Path(self.directory.name) / "status.sqlite3"

    def test_heartbeat_projection_is_bounded_with_large_history_and_preserves_summaries(self) -> None:
        repository = SQLiteHeartbeatRepository(self.database)
        repository.initialize()
        with sqlite3.connect(self.database) as connection:
            connection.row_factory = sqlite3.Row
            for component in ("observer", "worker", "monitor"):
                for state in ("starting", "healthy", "error"):
                    connection.execute(
                        "INSERT INTO component_heartbeats(component,state,observed_at,error_code) VALUES(?,?,?,?)",
                        (component, state, NOW.isoformat(), "old_error" if state == "error" else None),
                    )
            connection.execute(
                """WITH RECURSIVE history(n) AS (
                    VALUES(1) UNION ALL SELECT n+1 FROM history WHERE n < 550000
                ) INSERT INTO component_heartbeats(component,state,observed_at)
                SELECT 'worker','healthy',? FROM history""", (NOW.isoformat(),),
            )
            history_end = connection.execute("SELECT MAX(id) FROM component_heartbeats").fetchone()[0]
            for index in range(40):
                # Insertion order, not timestamp order, defines latest/error
                # history. Preserve it even when wall-clock timestamps regress.
                connection.execute(
                    "INSERT INTO component_heartbeats(component,state,observed_at,error_code) VALUES('monitor','error',?,?)",
                    ((NOW - timedelta(seconds=index)).isoformat(), f"error_{index}"),
                )
            connection.execute("INSERT INTO component_heartbeats(component,state,observed_at) VALUES('observer','starting',?)", (NOW.isoformat(),))
            reference = connection.execute(
                "SELECT component,state,observed_at,operation,error_code FROM component_heartbeats WHERE id<=9 OR id>=? ORDER BY id", (history_end,),
            ).fetchall()
            connection.commit()
            connection.execute("PRAGMA query_only=ON")
            vm_steps = 0
            def bounded_work():
                nonlocal vm_steps
                vm_steps += 1000
                return int(vm_steps > 10000)
            connection.set_progress_handler(bounded_work, 1000)
            rows = TraderStatusReader._heartbeat_events(connection)
            connection.set_progress_handler(None, 0)
        self.assertLessEqual(len(rows), 3 * 3 + 20)
        self.assertEqual(_component_snapshot(rows, NOW), _component_snapshot(reference, NOW))
        self.assertEqual(_recent_errors(rows), _recent_errors(reference))
        self.assertEqual(len(_recent_errors(rows)), 20)
        self.assertEqual(_recent_errors(rows)[0]["code"], "error_39")

    def test_heartbeat_indexes_upgrade_existing_history_without_reader_writes(self) -> None:
        repository = SQLiteHeartbeatRepository(self.database)
        repository.initialize()
        repository.record("worker", "error", observed_at=NOW, error_code="only_error")
        indexes = ("idx_component_heartbeats_component_state_id", "idx_component_heartbeats_state_id")
        with sqlite3.connect(self.database) as connection:
            connection.row_factory = sqlite3.Row
            for index in indexes:
                connection.execute(f"DROP INDEX {index}")
            connection.execute("PRAGMA query_only=ON")
            rows = TraderStatusReader._heartbeat_events(connection)
            self.assertEqual(len(rows), 1)
            self.assertIsNone(_component_snapshot(rows, NOW)["worker"]["last_success_at"])
        repository.initialize()
        repository.initialize()
        with sqlite3.connect(self.database) as connection:
            names = {row[1] for row in connection.execute("PRAGMA index_list(component_heartbeats)")}
            self.assertTrue(set(indexes).issubset(names))
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM component_heartbeats").fetchone()[0], 1)

    def tearDown(self) -> None:
        self.directory.cleanup()

    def test_missing_database_is_not_started_and_status_does_not_create_it(self) -> None:
        payload = TraderStatusReader(
            self.database, clock=lambda: NOW
        ).read()

        self.assertEqual(payload["schema_version"], "1")
        self.assertEqual(payload["overall_status"], "not_started")
        self.assertIsNone(payload["risk"]["reserved_risk"])
        self.assertIsNone(payload["risk"]["drawdown"]["current_loss_fraction"])
        self.assertIsNone(payload["risk"]["drawdown"]["circuit_breaker_latched"])
        self.assertEqual(
            payload["components"]["observer"]["activation_command"],
            "./.venv/bin/traderrd demo-run",
        )
        self.assertEqual(
            payload["components"]["worker"]["activation_command"],
            "./.venv/bin/traderrd demo-run",
        )
        self.assertEqual(
            payload["components"]["monitor"]["activation_command"],
            "./.venv/bin/traderrd demo-run",
        )
        rendered = render_status(payload)
        self.assertIn("./.venv/bin/traderrd demo-run", rendered)
        self.assertTrue(
            all(
                item["status"] == "not_started"
                for item in payload["components"].values()
            )
        )
        self.assertFalse(self.database.exists())

    def test_component_health_boundaries_and_newer_error(self) -> None:
        repository = SQLiteHeartbeatRepository(self.database)
        repository.initialize()
        repository.record("observer", "healthy", observed_at=NOW - timedelta(seconds=30))
        repository.record("worker", "healthy", observed_at=NOW - timedelta(seconds=120))
        repository.record("monitor", "healthy", observed_at=NOW - timedelta(seconds=301))
        repository.record(
            "observer",
            "error",
            operation="cycle",
            error_code="network failure",
            observed_at=NOW - timedelta(seconds=5),
        )

        payload = TraderStatusReader(self.database, clock=lambda: NOW).read()

        self.assertEqual(payload["components"]["observer"]["status"], "degraded")
        self.assertEqual(payload["components"]["worker"]["status"], "degraded")
        self.assertEqual(payload["components"]["monitor"]["status"], "stopped")
        self.assertEqual(payload["overall_status"], "stopped")
        self.assertEqual(payload["recent_errors"][0]["code"], "network_failure")
        self.assertIsNone(payload["components"]["observer"]["activation_command"])
        self.assertIsNone(payload["components"]["worker"]["activation_command"])
        self.assertEqual(
            payload["components"]["monitor"]["activation_command"],
            "./.venv/bin/traderrd demo-run",
        )
        rendered = render_status(payload)
        self.assertIn("investigate before restarting", rendered)
        self.assertIn("./.venv/bin/traderrd demo-run", rendered)

    def test_human_output_separates_resolved_errors_from_current_health(self) -> None:
        repository = SQLiteHeartbeatRepository(self.database)
        repository.initialize()
        repository.record(
            "worker",
            "error",
            operation="cycle",
            error_code="malformed response",
            observed_at=NOW - timedelta(minutes=2),
        )
        repository.record(
            "worker",
            "healthy",
            operation="cycle",
            observed_at=NOW - timedelta(seconds=10),
        )

        payload = TraderStatusReader(self.database, clock=lambda: NOW).read()
        rendered = render_status(payload)

        self.assertEqual(payload["components"]["worker"]["status"], "healthy")
        self.assertIn("[OK] Worker", rendered)
        self.assertIn("RECENT HISTORY", rendered)
        self.assertIn("Resolved incidents: 1", rendered)
        self.assertNotIn("CURRENT INCIDENTS", rendered)
        self.assertIn("2026-08-18 23:58:00 PET", rendered)
        self.assertNotIn("2026-08-19T04:58:00+00:00", rendered)

    def test_exact_source_snapshot_worker_lag_and_execution_summary(self) -> None:
        signals = SQLiteSignalRepository(self.database)
        signals.initialize()
        ingestion = SignalIngestionService(SignalParser(), signals)
        for message_id, topic_id, sender_id, body in (
            (10, 231508, 8003985182, LONG_SIGNAL),
            (
                11,
                999999,
                8003985182,
                LONG_SIGNAL.replace("XLMUSDT", "ADAUSDT")
                .replace("0.15688", "0.4")
                .replace("0.15813504", "0.4032")
                .replace("0.1521736", "0.388"),
            ),
            (
                12,
                231508,
                999999,
                LONG_SIGNAL.replace("XLMUSDT", "SOLUSDT")
                .replace("0.15688", "100")
                .replace("0.15813504", "100.8")
                .replace("0.1521736", "97"),
            ),
        ):
            result = ingestion.ingest(
                InboundTelegramEvent(
                    source_chat_id=2180632014,
                    source_topic_id=topic_id,
                    source_sender_id=sender_id,
                    source_message_id=message_id,
                    raw_text=body,
                    telegram_received_at=NOW - timedelta(minutes=1),
                )
            )
            self.assertEqual(result.outcome.value, "stored")

        bridge = SQLiteDemoBridgeRepository(self.database)
        bridge.initialize()
        bridge.initialize_worker_cursor(2180632014, 231508, 8003985182, 9)
        execution_sql = (
            "INSERT INTO demo_execution_intents ("
            "intent_id, order_link_id, risk_command_id, risk_reservation_id, "
            "kind, state, symbol, direction, quantity, price, take_profit, "
            "stop_loss, expires_at, exchange_order_id, created_at, updated_at"
            ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
        )
        SQLiteExecutionForTest(self.database).initialize()
        with sqlite3.connect(self.database) as connection:
            connection.execute(
                execution_sql,
                (
                    "planned-1", "link-1", "risk-1", "reservation-1", "entry",
                    "planned", "BTCUSDT", "LONG", "1", "100", "101", "97",
                    (NOW + timedelta(hours=1)).isoformat(), None, NOW.isoformat(), NOW.isoformat(),
                ),
            )
            connection.execute(
                execution_sql,
                (
                    "working-1", "link-2", "risk-2", "reservation-2", "entry",
                    "working", "ETHUSDT", "SHORT", "1", "100", "99", "103",
                    (NOW - timedelta(hours=1)).isoformat(), None, NOW.isoformat(), NOW.isoformat(),
                ),
            )
            connection.execute(
                execution_sql,
                (
                    "reconcile-1", "link-3", "risk-3", "reservation-3", "close_position",
                    "reconciliation_required", "SOLUSDT", "SHORT", "1", None, None, None,
                    None, None, NOW.isoformat(), NOW.isoformat(),
                ),
            )
            connection.commit()

        payload = TraderStatusReader(self.database, clock=lambda: NOW).read()

        latest = payload["source"]["latest_signal"]
        self.assertEqual(latest["message_id"], 10)
        self.assertEqual(payload["worker"]["cursor_message_id"], 9)
        self.assertEqual(payload["worker"]["cursor_lag_messages"], 1)
        self.assertEqual(payload["worker"]["unprocessed_signal_count"], 1)
        self.assertEqual(payload["execution"]["intent_counts"]["planned"], 1)
        self.assertEqual(payload["execution"]["intent_counts"]["working"], 1)
        self.assertEqual(payload["execution"]["expiry"]["active_entry_count"], 2)
        self.assertEqual(payload["execution"]["expiry"]["expired_entry_count"], 1)
        self.assertEqual(len(payload["execution"]["reconciliation_required"]), 1)
        active = payload["execution"]["active_intents"]
        self.assertEqual({item["symbol"] for item in active}, {"BTCUSDT", "ETHUSDT"})
        self.assertTrue(all("take_profit" in item for item in active))

    def test_risk_reserved_risk_drawdown_and_killed_latch(self) -> None:
        state = {
            "mode": "killed",
            "as_of": NOW.isoformat(),
            "equity": "850",
            "high_watermark": "1000",
            "daily_halted": True,
            "weekly_halted": False,
            "reservations": {
                "pending-1": {"status": "pending", "reserved_risk": "10.5"},
                "filled-1": {"status": "filled", "reserved_risk": "4.5"},
                "closed-1": {"status": "closed", "reserved_risk": "99"},
            },
        }
        with sqlite3.connect(self.database) as connection:
            connection.execute(
                "CREATE TABLE risk_engine_state ("
                "singleton_id INTEGER PRIMARY KEY, state_json TEXT NOT NULL)"
            )
            connection.execute(
                "INSERT INTO risk_engine_state(singleton_id, state_json) VALUES (1, ?)",
                (json.dumps(state),),
            )
            connection.commit()

        payload = TraderStatusReader(self.database, clock=lambda: NOW).read()
        risk = payload["risk"]

        self.assertEqual(risk["reserved_risk"], "15")
        self.assertEqual(risk["active_reservations"], 2)
        self.assertEqual(risk["drawdown"]["current_loss_fraction"], "0.15")
        self.assertTrue(risk["drawdown"]["circuit_breaker_latched"])
        rendered = render_status(payload)
        self.assertIn("reserved risk: 15", rendered)
        self.assertIn("drawdown 15.00%", rendered)
        self.assertIn("circuit breaker YES", rendered)

    def test_performance_is_ledger_only_net_of_fee_without_double_counting(self) -> None:
        SQLiteExecutionForTest(self.database).initialize()
        with sqlite3.connect(self.database) as connection:
            connection.executemany(
                """
                INSERT INTO demo_performance_outcomes (
                    risk_reservation_id, entry_intent_id, status, unresolved_reason,
                    symbol, direction, quantity, exchange_order_id, closed_at,
                    closed_pnl, open_fee, close_fee, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    ("r1", "i1", "attributed", None, "BTCUSDT", "LONG", "1", "o1",
                     NOW.isoformat(), "10", "1", "2", NOW.isoformat(), NOW.isoformat()),
                    ("r2", "i2", "attributed", None, "BTCUSDT", "SHORT", "1", "o2",
                     NOW.isoformat(), "-4", "0.5", "0.5", NOW.isoformat(), NOW.isoformat()),
                    ("r3", "i3", "attributed", None, "ETHUSDT", "LONG", "1", "o3",
                     NOW.isoformat(), "0", "0.1", "0.1", NOW.isoformat(), NOW.isoformat()),
                    ("r4", "i4", "unresolved", "missing_closed_pnl", "SOLUSDT", "LONG", "1", None,
                     None, None, None, None, NOW.isoformat(), NOW.isoformat()),
                ],
            )
            connection.commit()

        payload = TraderStatusReader(self.database, clock=lambda: NOW).read()
        performance = payload["performance"]

        self.assertTrue(performance["exchange_closed_pnl_includes_trading_fees"])
        self.assertEqual(performance["unresolved_exit_count"], 1)
        self.assertEqual(performance["overall"], {
            "closed_trades": 3, "wins": 1, "losses": 1, "breakeven": 1,
            "win_rate": "0.5", "exchange_reported_realized_pnl": "6",
            "trading_fees": "4.2", "net_pnl": "6",
        })
        self.assertEqual(performance["pairs"][0]["symbol"], "BTCUSDT")
        rendered = render_status(payload)
        self.assertIn("PERFORMANCE · Demo · Ledger rollout onward", rendered)
        self.assertIn("Net P&L: 6 USDT", rendered)
        self.assertIn("Per-pair results:", rendered)
        self.assertIn("BTCUSDT", rendered)
        self.assertIn("win rate  50.00%", rendered)
        self.assertIn("ACTION REQUIRED: 1 closed exit(s)", rendered)

    def test_performance_empty_state_explains_why_no_pair_rows_exist(self) -> None:
        rendered = render_status(TraderStatusReader(self.database, clock=lambda: NOW).read())

        self.assertIn("Per pair: no uniquely attributed closed trades yet", rendered)

    def test_color_renderer_is_opt_in_and_semantic(self) -> None:
        payload = TraderStatusReader(self.database, clock=lambda: NOW).read()
        payload["overall_status"] = "healthy"
        payload["components"]["observer"]["status"] = "healthy"
        payload["performance"]["overall"]["net_pnl"] = "12.5"
        colored = render_status(payload, color=True)
        plain = render_status(payload, color=False)

        self.assertIn("\033[32mHEALTHY\033[0m", colored)
        self.assertIn("\033[32m12.5\033[0m", colored)
        self.assertNotIn("\033[", plain)

    def test_color_is_disabled_for_json_no_color_and_non_tty(self) -> None:
        with patch.dict("os.environ", {}, clear=True), patch(
            "traderrd.status_cli.sys.stdout.isatty", return_value=True
        ):
            self.assertTrue(_should_color(as_json=False, no_color=False))
            self.assertFalse(_should_color(as_json=True, no_color=False))
            self.assertFalse(_should_color(as_json=False, no_color=True))
        with patch("traderrd.status_cli.sys.stdout.isatty", return_value=False):
            self.assertFalse(_should_color(as_json=False, no_color=False))
        with patch.dict("os.environ", {"NO_COLOR": "1"}, clear=False), patch(
            "traderrd.status_cli.sys.stdout.isatty", return_value=True
        ):
            self.assertFalse(_should_color(as_json=False, no_color=False))

    def test_malformed_risk_state_does_not_invent_risk_or_drawdown(self) -> None:
        state = {
            "mode": "unknown-mode",
            "equity": "not-a-number",
            "high_watermark": "1000",
            "reservations": {
                "pending-1": {"status": "pending", "reserved_risk": "bad"},
            },
        }
        with sqlite3.connect(self.database) as connection:
            connection.execute(
                "CREATE TABLE risk_engine_state ("
                "singleton_id INTEGER PRIMARY KEY, state_json TEXT NOT NULL)"
            )
            connection.execute(
                "INSERT INTO risk_engine_state(singleton_id, state_json) VALUES (1, ?)",
                (json.dumps(state),),
            )
            connection.commit()

        risk = TraderStatusReader(self.database, clock=lambda: NOW).read()["risk"]

        self.assertIsNone(risk["reserved_risk"])
        self.assertIsNone(risk["drawdown"]["current_loss_fraction"])
        self.assertIsNone(risk["drawdown"]["circuit_breaker_latched"])

    def test_cli_status_is_read_only_and_json_has_no_raw_payload(self) -> None:
        arguments = [
            "traderrd",
            "status",
            "--json",
            "--database-path",
            str(self.database),
        ]
        with (
            patch("sys.argv", arguments),
            patch(
                "traderrd.cli.load_config",
                side_effect=AssertionError("status must not load configuration"),
            ),
            patch(
                "traderrd.cli.load_dotenv",
                side_effect=AssertionError("status must not load environment"),
            ),
            patch("sys.stdout", new_callable=StringIO) as output,
        ):
            self.assertEqual(main(), 0)

        rendered = output.getvalue()
        parsed = json.loads(rendered)
        self.assertEqual(parsed["schema_version"], "1")
        self.assertNotIn("raw_text", rendered)
        self.assertFalse(self.database.exists())


class SQLiteExecutionForTest:
    """Small schema setup helper to keep the status fixture write-only."""

    def __init__(self, database: Path) -> None:
        self.database = database

    def initialize(self) -> None:
        from traderrd.infrastructure.execution_repository import SQLiteDemoExecutionRepository

        SQLiteDemoExecutionRepository(self.database).initialize()


if __name__ == "__main__":
    unittest.main()
