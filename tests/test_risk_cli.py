from contextlib import closing
from io import StringIO
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from traderrd.risk_cli import run_risk_replay, run_risk_report
from traderrd.demo_runtime import _demo_operation_lock


COMMANDS = [
    {
        "type": "initialize",
        "command_id": "init-1",
        "occurred_at": "2026-08-17T09:00:00-05:00",
        "equity": "1000.00",
        "estimated_cost_rate": "0.001",
    },
    {
        "type": "propose",
        "command_id": "propose-1",
        "occurred_at": "2026-08-17T09:01:00-05:00",
        "mark_equity": "1000.00",
        "signal_id": "signal-1",
        "symbol": "BTCUSDT",
        "direction": "LONG",
        "entry": "100",
        "take_profit": "100.8",
        "stop_loss": "97",
    },
]


class RiskCliTests(unittest.TestCase):
    def test_live_epoch_replay_is_refused_before_initializing_database(self) -> None:
        from datetime import datetime, timezone
        now = datetime.now(timezone.utc).isoformat()
        epoch = dict(type="start_demo_risk_epoch", command_id="demo-risk-epoch:epoch",
                     occurred_at=now, captured_at=now, equity="860", wallet_balance="860",
                     unrealised_pnl="0", operator_reference="operator",
                     previous_epoch_id="initial", evidence_symbol="BTCUSDT")
        self.commands_path.write_text(json.dumps(epoch) + "\n", encoding="utf-8")
        with patch("sys.stdout", new_callable=StringIO) as output:
            self.assertEqual(run_risk_replay(self.commands_path, self.database_path, True), 1)
        self.assertFalse(self.database_path.exists())
        self.assertIn("never risk-replay", output.getvalue())

    def test_applied_replay_refuses_exclusive_operation_without_database_writes(self) -> None:
        with (
            _demo_operation_lock(self.database_path, exclusive=True),
            patch("sys.stdout", new_callable=StringIO),
        ):
            self.assertEqual(run_risk_replay(self.commands_path, self.database_path, True), 1)
            self.assertFalse(self.database_path.exists())
            self.assertEqual(run_risk_replay(self.commands_path, self.database_path, False), 0)

    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        root = Path(self.directory.name)
        self.commands_path = root / "commands.jsonl"
        self.database_path = root / "risk.sqlite3"
        self.commands_path.write_text(
            "\n".join(json.dumps(value) for value in COMMANDS) + "\n",
            encoding="utf-8",
        )

    def tearDown(self) -> None:
        self.directory.cleanup()

    def test_replay_is_dry_run_by_default_and_prints_aggregate_only(self) -> None:
        with patch("sys.stdout", new_callable=StringIO) as output:
            exit_code = run_risk_replay(
                self.commands_path,
                self.database_path,
                apply_state=False,
            )

        self.assertEqual(exit_code, 0)
        self.assertFalse(self.database_path.exists())
        self.assertIn("commands=2", output.getvalue())
        self.assertIn("state_applied=false", output.getvalue())
        self.assertNotIn("BTCUSDT", output.getvalue())

    def test_explicit_apply_persists_decisions_but_executes_nothing(self) -> None:
        with patch("sys.stdout", new_callable=StringIO) as output:
            exit_code = run_risk_replay(
                self.commands_path,
                self.database_path,
                apply_state=True,
            )

        self.assertEqual(exit_code, 0)
        self.assertIn("exchange_execution=false", output.getvalue())
        with closing(sqlite3.connect(self.database_path)) as connection:
            commands = connection.execute(
                "SELECT COUNT(*) FROM risk_engine_commands"
            ).fetchone()[0]
        self.assertEqual(commands, 2)

    def test_report_is_read_only_and_contains_state_counters(self) -> None:
        with patch("sys.stdout", new_callable=StringIO):
            run_risk_replay(
                self.commands_path,
                self.database_path,
                apply_state=True,
            )

        with patch("sys.stdout", new_callable=StringIO) as output:
            exit_code = run_risk_report(self.database_path)

        self.assertEqual(exit_code, 0)
        self.assertIn("mode=active", output.getvalue())
        self.assertIn("active_reservations=1", output.getvalue())
        self.assertIn("exchange_execution=false", output.getvalue())
        self.assertNotIn("BTCUSDT", output.getvalue())


if __name__ == "__main__":
    unittest.main()
