from contextlib import chdir, closing
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from io import StringIO
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from traderrd.cli import main
from traderrd.application.demo_bridge import DemoBridgeError
from traderrd.application.risk_engine import RiskEngineService
from traderrd.domain.execution import (
    DemoPreflightReport,
    FeeVerificationStatus,
    InstrumentRules,
)
from traderrd.domain.models import Direction
from traderrd.domain.risk import (
    InitializeRiskEngine,
    ProposeRiskReservation,
    TradeProposal,
)
from traderrd.infrastructure.risk_repository import SQLiteRiskStateRepository
from traderrd.infrastructure.execution_repository import (
    SQLiteDemoExecutionRepository,
)
from traderrd.demo_cli import (
    load_demo_client,
    run_demo_execute,
    run_demo_preflight,
)
from traderrd.demo_bridge_cli import _configured_sender_id


UTC = timezone.utc


class DemoCliSafetyTests(unittest.TestCase):
    def test_bridge_sender_guard_requires_confirmed_numeric_configuration(self) -> None:
        with patch.dict(
            "os.environ", {"TELEGRAM_EXPECTED_SENDER_ID": "778899"}, clear=True
        ):
            self.assertEqual(_configured_sender_id(), 778899)
        with patch.dict("os.environ", {}, clear=True), self.assertRaises(
            DemoBridgeError
        ):
            _configured_sender_id()

    def test_preflight_labels_exchange_fees_unavailable(self) -> None:
        rules = InstrumentRules(
            symbol="BTCUSDT",
            tick_size=Decimal("0.1"),
            quantity_step=Decimal("0.001"),
            min_quantity=Decimal("0.001"),
            max_quantity=Decimal("100"),
            min_notional=Decimal("5"),
        )
        report = DemoPreflightReport(
            symbol="BTCUSDT",
            isolated_margin=True,
            one_way_mode=True,
            available_balance=Decimal("850"),
            fee_verification=FeeVerificationStatus.UNAVAILABLE,
            instrument=rules,
            server_time_offset_ms=0,
            checked_at=datetime(2026, 8, 17, tzinfo=timezone.utc),
        )
        with (
            patch("traderrd.demo_cli.load_demo_client", return_value=object()),
            patch("traderrd.demo_cli.BybitDemoPreflight") as preflight,
            patch("sys.stdout", new_callable=StringIO) as output,
        ):
            preflight.return_value.run.return_value = report
            exit_code = run_demo_preflight("BTCUSDT", "/isolated/demo.env")

        self.assertEqual(exit_code, 0)
        rendered = output.getvalue()
        self.assertIn("exchange_fee_verification=unavailable", rendered)
        self.assertIn("risk_cost_source=configured_estimate", rendered)
        self.assertNotIn("maker_fee_rate", rendered)
        self.assertNotIn("taker_fee_rate", rendered)

    def test_dry_execution_needs_no_credentials_or_writes(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database_path = Path(directory) / "risk.sqlite3"
            repository = SQLiteRiskStateRepository(database_path)
            repository.initialize()
            service = RiskEngineService(repository)
            start = datetime(2026, 8, 17, 14, 0, tzinfo=UTC)
            service.execute(InitializeRiskEngine("init", start, Decimal("1000")))
            service.execute(
                ProposeRiskReservation(
                    "proposal",
                    start + timedelta(minutes=1),
                    Decimal("1000"),
                    TradeProposal(
                        "signal",
                        "BTCUSDT",
                        Direction.LONG,
                        Decimal("100"),
                        Decimal("100.8"),
                        Decimal("97"),
                    ),
                )
            )

            with (
                patch.dict("os.environ", {}, clear=True),
                patch("sys.stdout", new_callable=StringIO) as output,
            ):
                exit_code = run_demo_execute(
                    database_path,
                    "BTCUSDT",
                    "proposal",
                    submit_demo=False,
                )

            self.assertEqual(exit_code, 0)
            self.assertIn("risk_actions=1", output.getvalue())
            self.assertIn("private_requests=0", output.getvalue())
            with closing(sqlite3.connect(database_path)) as connection:
                table = connection.execute(
                    "SELECT 1 FROM sqlite_master WHERE type='table' "
                    "AND name='demo_execution_intents'"
                ).fetchone()
            self.assertIsNone(table)

    def test_missing_credentials_fail_without_printing_values(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            missing = Path(directory) / "missing.env"
            with patch.dict("os.environ", {}, clear=True):
                with self.assertRaises(ValueError) as raised:
                    load_demo_client(missing)

        self.assertNotIn("API_KEY", str(raised.exception))
        self.assertNotIn("API_SECRET", str(raised.exception))

    def test_mainnet_environment_is_rejected(self) -> None:
        values = {
            "BYBIT_DEMO_API_KEY": "key",
            "BYBIT_DEMO_API_SECRET": "secret",
            "BYBIT_DEMO_BASE_URL": "https://api.bybit.com",
        }
        with tempfile.TemporaryDirectory() as directory:
            missing = Path(directory) / "missing.env"
            with patch.dict("os.environ", values, clear=True):
                with self.assertRaises(ValueError) as raised:
                    load_demo_client(missing)

        self.assertIn("restricted to Bybit Demo Trading", str(raised.exception))
        self.assertNotIn("secret", str(raised.exception))

    def test_testnet_credentials_are_not_used_for_demo(self) -> None:
        values = {
            "BYBIT_TESTNET_API_KEY": "old-key",
            "BYBIT_TESTNET_API_SECRET": "old-secret",
            "BYBIT_TESTNET_BASE_URL": "https://api-testnet.bybit.com",
        }
        with tempfile.TemporaryDirectory() as directory:
            missing = Path(directory) / "missing.env"
            with patch.dict("os.environ", values, clear=True):
                with self.assertRaises(ValueError) as raised:
                    load_demo_client(missing)

        rendered = str(raised.exception)
        self.assertIn("Demo Trading API credentials are required", rendered)
        self.assertNotIn("old-key", rendered)
        self.assertNotIn("old-secret", rendered)

    def test_default_env_file_loads_demo_credentials(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            env_file = Path(directory) / ".env"
            env_file.write_text(
                "BYBIT_DEMO_API_KEY=file-key\n"
                "BYBIT_DEMO_API_SECRET=file-secret\n"
                "BYBIT_DEMO_BASE_URL=https://api-demo.bybit.com\n",
                encoding="utf-8",
            )
            with patch.dict("os.environ", {}, clear=True), chdir(directory):
                client = load_demo_client()

        self.assertEqual(client.config.credentials.api_key, "file-key")
        self.assertEqual(client.config.credentials.api_secret, "file-secret")

    def test_custom_env_file_loads_demo_credentials(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            env_file = Path(directory) / "demo-settings.env"
            env_file.write_text(
                "BYBIT_DEMO_API_KEY=custom-key\n"
                "BYBIT_DEMO_API_SECRET=custom-secret\n",
                encoding="utf-8",
            )
            with patch.dict("os.environ", {}, clear=True):
                client = load_demo_client(env_file)

        self.assertEqual(client.config.credentials.api_key, "custom-key")
        self.assertEqual(client.config.credentials.api_secret, "custom-secret")

    def test_process_environment_overrides_env_file(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            env_file = Path(directory) / "demo-settings.env"
            env_file.write_text(
                "BYBIT_DEMO_API_KEY=file-key\n"
                "BYBIT_DEMO_API_SECRET=file-secret\n",
                encoding="utf-8",
            )
            values = {
                "BYBIT_DEMO_API_KEY": "environment-key",
                "BYBIT_DEMO_API_SECRET": "environment-secret",
            }
            with patch.dict("os.environ", values, clear=True):
                client = load_demo_client(env_file)

        self.assertEqual(client.config.credentials.api_key, "environment-key")
        self.assertEqual(
            client.config.credentials.api_secret,
            "environment-secret",
        )

    def test_cli_passes_custom_env_file_to_all_demo_commands(self) -> None:
        env_path = "/isolated/demo.env"
        cases = (
            (
                [
                    "traderrd",
                    "demo-bridge",
                    "--message-id",
                    "231906",
                    "--source-sender-id",
                    "778899",
                    "--env-file",
                    env_path,
                ],
                "traderrd.demo_bridge_cli.run_demo_bridge",
                (
                    "data/traderrd.sqlite3",
                    231906,
                    778899,
                    False,
                    Decimal("0.001"),
                    env_path,
                ),
            ),
            (
                [
                    "traderrd",
                    "demo-preflight",
                    "--symbol",
                    "BTCUSDT",
                    "--env-file",
                    env_path,
                ],
                "traderrd.demo_cli.run_demo_preflight",
                ("BTCUSDT", env_path),
            ),
            (
                [
                    "traderrd",
                    "demo-execute",
                    "--symbol",
                    "BTCUSDT",
                    "--env-file",
                    env_path,
                ],
                "traderrd.demo_cli.run_demo_execute",
                ("data/traderrd.sqlite3", "BTCUSDT", None, False, env_path),
            ),
            (
                [
                    "traderrd",
                    "demo-reconcile",
                    "--intent-id",
                    "intent-1",
                    "--env-file",
                    env_path,
                ],
                "traderrd.demo_cli.run_demo_reconcile",
                ("data/traderrd.sqlite3", "intent-1", False, env_path),
            ),
        )
        for argv, target, expected in cases:
            with self.subTest(command=argv[1]):
                with patch("sys.argv", argv), patch(target, return_value=0) as run:
                    self.assertEqual(main(), 0)
                run.assert_called_once_with(*expected)

    def test_demo_repository_does_not_create_or_reuse_testnet_tables(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database_path = Path(directory) / "execution.sqlite3"
            SQLiteDemoExecutionRepository(database_path).initialize()
            with closing(sqlite3.connect(database_path)) as connection:
                tables = {
                    row[0]
                    for row in connection.execute(
                        "SELECT name FROM sqlite_master WHERE type='table'"
                    )
                }

        self.assertIn("demo_execution_intents", tables)
        self.assertIn("demo_execution_events", tables)
        self.assertNotIn("testnet_execution_intents", tables)
        self.assertNotIn("testnet_execution_events", tables)

    def test_retired_testnet_command_fails_with_migration_error(self) -> None:
        with (
            patch("sys.argv", ["traderrd", "testnet-preflight"]),
            patch("sys.stderr", new_callable=StringIO) as error,
            self.assertRaises(SystemExit) as raised,
        ):
            main()

        self.assertEqual(raised.exception.code, 2)
        rendered = error.getvalue()
        self.assertIn("testnet commands are retired", rendered)
        self.assertIn("demo-preflight", rendered)
        self.assertIn("BYBIT_DEMO_*", rendered)

    def test_retired_testnet_flag_fails_before_demo_execution(self) -> None:
        with (
            patch(
                "sys.argv",
                [
                    "traderrd",
                    "demo-execute",
                    "--symbol",
                    "BTCUSDT",
                    "--submit-testnet",
                ],
            ),
            patch("sys.stderr", new_callable=StringIO) as error,
            self.assertRaises(SystemExit) as raised,
        ):
            main()

        self.assertEqual(raised.exception.code, 2)
        self.assertIn("testnet flags are retired", error.getvalue())


if __name__ == "__main__":
    unittest.main()
