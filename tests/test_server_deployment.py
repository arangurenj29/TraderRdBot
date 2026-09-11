from __future__ import annotations

from datetime import datetime, timezone
import sqlite3
import subprocess
import sys
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from traderrd.config import ObserverConfig
from traderrd.health_cli import run_healthcheck
from traderrd.infrastructure.heartbeat_repository import SQLiteHeartbeatRepository
from traderrd.telegram_auth import authorize_telegram, run_telegram_auth


class _Tty:
    def isatty(self) -> bool:
        return True

    def write(self, _value: str) -> int:
        return 0

    def flush(self) -> None:
        return None


class _Client:
    def __init__(self, *_args: object) -> None:
        self.authorized = False
        self.disconnected = False

    async def connect(self) -> None:
        return None

    async def is_user_authorized(self) -> bool:
        return self.authorized

    async def start(self) -> None:
        self.authorized = True

    async def disconnect(self) -> None:
        self.disconnected = True


class ServerDeploymentTests(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.database = Path(self.directory.name) / "state.sqlite3"

    def tearDown(self) -> None:
        self.directory.cleanup()

    def test_healthcheck_timeout_allows_swap_limited_startup_without_relaxing_checks(self) -> None:
        compose = Path("compose.yaml").read_text()
        self.assertIn("timeout: 20s", compose)
        self.assertIn("interval: 30s", compose)
        self.assertIn("retries: 3", compose)
        self.assertIn("start_period: 2m", compose)

    def test_healthcheck_is_strict_and_read_only(self) -> None:
        self.assertEqual(run_healthcheck(self.database), 1)
        self.assertFalse(self.database.exists())

        heartbeats = SQLiteHeartbeatRepository(self.database)
        heartbeats.initialize()
        now = datetime.now(timezone.utc)
        for component in ("observer", "worker", "monitor"):
            heartbeats.record(component, "healthy", observed_at=now)
        self.assertEqual(run_healthcheck(self.database), 0)

        heartbeats.record("worker", "error", error_code="network", observed_at=now)
        self.assertEqual(run_healthcheck(self.database), 1)

    def test_telegram_auth_only_authorizes_session(self) -> None:
        config = ObserverConfig(
            api_id=1,
            api_hash="test",
            source_chat_id=2180632014,
            source_topic_id=231508,
            source_seed_message_id=231906,
            expected_sender_id=8003985182,
            session_path=Path(self.directory.name) / "session" / "traderrd",
            database_path=self.database,
            log_level="INFO",
            bybit_public_base_url="https://api.bybit.com",
            bybit_category="linear",
            bybit_timeout_seconds=3.0,
        )
        import asyncio

        asyncio.run(authorize_telegram(config, client_factory=_Client))
        self.assertTrue(config.session_path.parent.exists())
        self.assertFalse(self.database.exists())

    def test_telegram_auth_requires_tty_before_contacting_telegram(self) -> None:
        config = ObserverConfig(
            api_id=1, api_hash="test", source_chat_id=2180632014,
            source_topic_id=231508, source_seed_message_id=231906,
            expected_sender_id=8003985182, session_path=Path(self.directory.name) / "s",
            database_path=self.database, log_level="INFO", bybit_public_base_url="https://api.bybit.com",
            bybit_category="linear", bybit_timeout_seconds=3.0,
        )
        class NonTty:
            def isatty(self) -> bool:
                return False
        self.assertEqual(run_telegram_auth(config, stdin=NonTty(), stdout=_Tty()), 2)

    def test_deployment_artifacts_enforce_demo_only_runtime(self) -> None:
        root = Path(__file__).parents[1]
        dockerfile = (root / "Dockerfile").read_text(encoding="utf-8")
        compose = (root / "compose.yaml").read_text(encoding="utf-8")
        env_example = (root / ".env.server.example").read_text(encoding="utf-8")

        self.assertIn("FROM python:3.11-slim", dockerfile)
        self.assertIn("USER traderrd", dockerfile)
        self.assertIn("demo-run", compose)
        self.assertIn("--no-tui", compose)
        self.assertIn("restart: unless-stopped", compose)
        self.assertIn("telegram-auth", compose)
        self.assertIn("profiles: [\"bootstrap\"]", compose)
        self.assertIn("healthcheck", compose)
        self.assertIn("/var/lib/traderrd", compose)
        self.assertIn("BYBIT_DEMO_BASE_URL=https://api-demo.bybit.com", env_example)
        self.assertNotIn("BYBIT_API_SECRET=", env_example)

    def test_dockerignore_excludes_runtime_secrets_and_preserves_templates(self) -> None:
        patterns = (Path(__file__).parents[1] / ".dockerignore").read_text(encoding="utf-8")
        for required in (
            ".env.*", "!.env.example", "!.env.server.example", ".venv/", "data/",
            ".local/", "logs/", "*.sqlite3", "*.sqlite3-wal", "*.sqlite3-shm",
            "*.session", ".git/", ".atl/", ".codex/", ".agents/", "__pycache__/",
        ):
            self.assertIn(required, patterns)

    def test_sqlite_snapshot_includes_wal_and_restore_clears_sidecars(self) -> None:
        root = Path(__file__).parents[1]
        source = Path(self.directory.name) / "live.sqlite3"
        backup = Path(self.directory.name) / "backup.sqlite3"
        restored = Path(self.directory.name) / "restored.sqlite3"
        with sqlite3.connect(source) as connection:
            connection.execute("PRAGMA journal_mode=WAL")
            connection.execute("CREATE TABLE events(value TEXT)")
            connection.execute("INSERT INTO events VALUES ('committed-in-wal')")
            connection.commit()
        subprocess.run(
            [sys.executable, str(root / "scripts/sqlite_snapshot.py"), "backup", str(source), str(backup)],
            check=True, capture_output=True, text=True,
        )
        with sqlite3.connect(backup) as connection:
            self.assertEqual(connection.execute("SELECT value FROM events").fetchone()[0], "committed-in-wal")
        Path(f"{restored}-wal").write_text("stale", encoding="utf-8")
        Path(f"{restored}-shm").write_text("stale", encoding="utf-8")
        subprocess.run(
            [sys.executable, str(root / "scripts/sqlite_snapshot.py"), "restore", str(backup), str(restored)],
            check=True, capture_output=True, text=True,
        )
        self.assertFalse(Path(f"{restored}-wal").exists())
        self.assertFalse(Path(f"{restored}-shm").exists())
        with sqlite3.connect(restored) as connection:
            self.assertEqual(connection.execute("SELECT value FROM events").fetchone()[0], "committed-in-wal")


if __name__ == "__main__":
    unittest.main()

class ServerCommandRoutingTests(unittest.TestCase):
    def test_healthcheck_cli_does_not_load_configuration(self) -> None:
        from traderrd.cli import main

        with patch("sys.argv", ["traderrd", "healthcheck", "--database-path", "/missing.db"]), patch(
            "traderrd.config.load_config", side_effect=AssertionError("must not load credentials")
        ):
            self.assertEqual(main(), 1)

    def test_telegram_auth_cli_uses_authorization_entrypoint(self) -> None:
        from traderrd.cli import main
        config = object()
        with patch("sys.argv", ["traderrd", "telegram-auth"]), patch(
            "traderrd.cli.load_config", return_value=config
        ), patch("traderrd.telegram_auth.run_telegram_auth", return_value=0) as run_auth:
            self.assertEqual(main(), 0)
        run_auth.assert_called_once_with(config)
