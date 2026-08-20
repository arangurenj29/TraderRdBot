import asyncio
from io import StringIO
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from traderrd.cli import build_parser, main
from traderrd.infrastructure.sqlite_repository import SQLiteSignalRepository
from traderrd.telegram_listener import run
from traderrd.telegram_source import SourceIsolationError


class ObserverRuntimeTests(unittest.TestCase):
    @staticmethod
    def _raise_after_closing(coroutine: object, error: BaseException) -> None:
        coroutine.close()
        raise error

    def test_keyboard_interrupt_returns_shell_interrupt_code(self) -> None:
        def interrupt(coroutine: object) -> None:
            self._raise_after_closing(coroutine, KeyboardInterrupt())

        with patch("traderrd.telegram_listener.asyncio.run", side_effect=interrupt):
            result = run(object())

        self.assertEqual(result, 130)

    def test_cancelled_error_returns_shell_interrupt_code(self) -> None:
        def cancel(coroutine: object) -> None:
            self._raise_after_closing(coroutine, asyncio.CancelledError())

        with patch("traderrd.telegram_listener.asyncio.run", side_effect=cancel):
            result = run(object())

        self.assertEqual(result, 130)

    def test_source_isolation_failure_stops_observer_cleanly(self) -> None:
        def reject(coroutine: object) -> None:
            self._raise_after_closing(
                coroutine,
                SourceIsolationError("safe source isolation failure"),
            )

        with patch("traderrd.telegram_listener.asyncio.run", side_effect=reject):
            result = run(object())

        self.assertEqual(result, 1)

    def test_transport_failure_retries_with_backoff(self) -> None:
        calls = 0

        def fail_once_then_stop(coroutine: object) -> None:
            nonlocal calls
            coroutine.close()
            calls += 1
            if calls == 1:
                raise ConnectionError("temporary Telegram outage")

        with (
            patch("traderrd.telegram_listener.asyncio.run", side_effect=fail_once_then_stop),
            patch("traderrd.telegram_listener.time.sleep") as sleep,
        ):
            result = run(object())

        self.assertEqual(result, 0)
        self.assertEqual(calls, 2)
        sleep.assert_called_once_with(1.0)

    def test_network_permission_failure_does_not_retry(self) -> None:
        def deny_network(coroutine: object) -> None:
            coroutine.close()
            raise PermissionError("network blocked")

        with (
            patch(
                "traderrd.telegram_listener.asyncio.run",
                side_effect=deny_network,
            ) as run_async,
            patch("traderrd.telegram_listener.time.sleep") as sleep,
        ):
            result = run(object())

        self.assertEqual(result, 1)
        run_async.assert_called_once()
        sleep.assert_not_called()

    def test_backfill_limit_must_be_positive(self) -> None:
        error_output = StringIO()
        with patch("sys.stderr", error_output), self.assertRaises(SystemExit):
            build_parser().parse_args(["backfill", "--limit", "0"])
        self.assertIn("must be a positive integer", error_output.getvalue())

    def test_audit_reprocessing_limit_must_be_positive(self) -> None:
        error_output = StringIO()
        with patch("sys.stderr", error_output), self.assertRaises(SystemExit):
            build_parser().parse_args(["reprocess-audit", "--limit", "0"])
        self.assertIn("must be a positive integer", error_output.getvalue())

    def test_audit_reprocessing_cli_does_not_load_telegram_configuration(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database_path = Path(directory) / "audit.sqlite3"
            SQLiteSignalRepository(database_path).initialize()
            arguments = [
                "traderrd",
                "reprocess-audit",
                "--database-path",
                str(database_path),
            ]
            with (
                patch("sys.argv", arguments),
                patch(
                    "traderrd.cli.load_config",
                    side_effect=AssertionError("configuration must not be loaded"),
                ),
                patch(
                    "traderrd.cli.load_dotenv",
                    side_effect=AssertionError("environment file must not be loaded"),
                ),
                patch("sys.stdout", StringIO()),
            ):
                exit_code = main()

        self.assertEqual(exit_code, 0)

    def test_historical_simulation_limit_must_be_positive(self) -> None:
        error_output = StringIO()
        with patch("sys.stderr", error_output), self.assertRaises(SystemExit):
            build_parser().parse_args(["simulate-history", "--limit", "0"])
        self.assertIn("must be a positive integer", error_output.getvalue())

    def test_historical_simulation_rejects_an_unvalidated_source(self) -> None:
        arguments = [
            "traderrd",
            "simulate-history",
            "--source-topic-id",
            "999999",
        ]
        with (
            patch("sys.argv", arguments),
            patch("sys.stderr", StringIO()) as error_output,
            self.assertRaises(SystemExit),
        ):
            main()
        self.assertIn("restricted to validated chat", error_output.getvalue())

    def test_historical_simulation_dry_run_does_not_load_configuration(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database_path = Path(directory) / "simulation.sqlite3"
            repository = SQLiteSignalRepository(database_path)
            repository.initialize()
            arguments = [
                "traderrd",
                "simulate-history",
                "--database-path",
                str(database_path),
            ]
            with (
                patch("sys.argv", arguments),
                patch(
                    "traderrd.cli.load_config",
                    side_effect=AssertionError("configuration must not be loaded"),
                ),
                patch(
                    "traderrd.cli.load_dotenv",
                    side_effect=AssertionError("environment file must not be loaded"),
                ),
                patch("sys.stdout", StringIO()) as output,
            ):
                exit_code = main()

        self.assertEqual(exit_code, 0)
        self.assertIn("public_data_requested=false", output.getvalue())

    def test_historical_simulation_interrupt_returns_130(self) -> None:
        arguments = ["traderrd", "simulate-history"]
        with (
            patch("sys.argv", arguments),
            patch(
                "traderrd.historical_simulation_cli.run_historical_simulation",
                side_effect=KeyboardInterrupt,
            ),
            patch("sys.stdout", StringIO()) as output,
        ):
            exit_code = main()

        self.assertEqual(exit_code, 130)
        self.assertIn("interrupted", output.getvalue())

    def test_risk_replay_requires_a_command_file(self) -> None:
        arguments = ["traderrd", "risk-replay"]
        with (
            patch("sys.argv", arguments),
            patch("sys.stderr", StringIO()) as error_output,
            self.assertRaises(SystemExit),
        ):
            main()
        self.assertIn("requires --commands-file", error_output.getvalue())

    def test_risk_replay_dry_run_does_not_load_configuration_or_write_db(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            commands_path = root / "commands.jsonl"
            database_path = root / "risk.sqlite3"
            commands_path.write_text(
                json.dumps(
                    {
                        "type": "initialize",
                        "command_id": "init",
                        "occurred_at": "2026-08-17T09:00:00-05:00",
                        "equity": "1000",
                    }
                )
                + "\n",
                encoding="utf-8",
            )
            arguments = [
                "traderrd",
                "risk-replay",
                "--commands-file",
                str(commands_path),
                "--database-path",
                str(database_path),
            ]
            with (
                patch("sys.argv", arguments),
                patch(
                    "traderrd.cli.load_config",
                    side_effect=AssertionError("configuration must not be loaded"),
                ),
                patch(
                    "traderrd.cli.load_dotenv",
                    side_effect=AssertionError("environment file must not be loaded"),
                ),
                patch("sys.stdout", StringIO()) as output,
            ):
                exit_code = main()

            self.assertEqual(exit_code, 0)
            self.assertFalse(database_path.exists())
            self.assertIn("exchange_execution=false", output.getvalue())


if __name__ == "__main__":
    unittest.main()
