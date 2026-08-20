from __future__ import annotations

from io import StringIO
from pathlib import Path
import signal
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from traderrd.cli import main
from traderrd.demo_runtime import (
    DemoRuntimeAlreadyRunningError,
    DemoRuntimeSupervisor,
    _exclusive_runtime_lock,
)


class _Process:
    def __init__(self, *, exit_code: int | None = None, output: str = "") -> None:
        self.stdout = StringIO(output)
        self._exit_code = exit_code
        self.signals: list[int] = []
        self.terminated = False
        self.killed = False

    def poll(self) -> int | None:
        return self._exit_code

    def send_signal(self, value: int) -> None:
        self.signals.append(value)
        self._exit_code = 130

    def wait(self, timeout: float | None = None) -> int:
        if self._exit_code is None:
            raise subprocess.TimeoutExpired("demo", timeout)
        return self._exit_code

    def terminate(self) -> None:
        self.terminated = True
        self._exit_code = -15

    def kill(self) -> None:
        self.killed = True
        self._exit_code = -9


class DemoRuntimeTests(unittest.TestCase):
    def test_command_builds_demo_only_children_and_shared_database(self) -> None:
        supervisor = DemoRuntimeSupervisor("runtime.sqlite3", "demo.env", 30.0)
        observer, worker, monitor = supervisor._children()

        self.assertEqual(observer.name, "observer")
        self.assertIn("run", observer.command)
        self.assertIn("demo-worker", worker.command)
        self.assertIn("--apply-demo-worker", worker.command)
        self.assertIn("demo-monitor", monitor.command)
        self.assertIn("--apply-demo-monitor", monitor.command)
        self.assertNotIn("mainnet", " ".join(worker.command + monitor.command))
        self.assertIn("runtime.sqlite3", worker.command)
        self.assertIn("runtime.sqlite3", monitor.command)

    def test_supervisor_labels_logs_and_stops_all_children_when_one_exits(self) -> None:
        processes = [
            _Process(output="observer log\n"),
            _Process(exit_code=1, output="worker failed\n"),
            _Process(output="monitor log\n"),
        ]
        output = StringIO()
        with tempfile.TemporaryDirectory() as directory:
            supervisor = DemoRuntimeSupervisor(
                Path(directory) / "runtime.sqlite3",
                "demo.env",
                30.0,
                process_factory=lambda *args, **kwargs: processes.pop(0),
                output=output,
                sleep=lambda _: None,
            )
            self.assertEqual(supervisor.run(), 1)

        rendered = output.getvalue()
        self.assertIn("[observer] started", rendered)
        self.assertIn("[worker] worker failed", rendered)
        self.assertIn("[worker] exited code=1", rendered)
        self.assertEqual(processes, [])

    def test_ctrl_c_sends_interrupt_to_every_child(self) -> None:
        children = [_Process(), _Process(), _Process()]
        started = list(children)
        output = StringIO()

        def interrupt(_: float) -> None:
            raise KeyboardInterrupt

        with tempfile.TemporaryDirectory() as directory:
            supervisor = DemoRuntimeSupervisor(
                Path(directory) / "runtime.sqlite3",
                "demo.env",
                30.0,
                process_factory=lambda *args, **kwargs: children.pop(0),
                output=output,
                sleep=interrupt,
            )
            self.assertEqual(supervisor.run(), 130)

        self.assertEqual(children, [])
        self.assertTrue(all(process.signals == [signal.SIGINT] for process in started))

    def test_supervisor_routes_logs_to_dashboard_and_quit_stops_children(self) -> None:
        class Dashboard:
            def __init__(self) -> None:
                self.logs: list[str] = []
                self.started = False
                self.closed = False
            def start(self) -> None: self.started = True
            def log(self, value: str) -> None: self.logs.append(value)
            def step(self) -> bool: return False
            def close(self) -> None: self.closed = True

        dashboard = Dashboard()
        children = [_Process(output="received\n"), _Process(), _Process()]
        with tempfile.TemporaryDirectory() as directory:
            supervisor = DemoRuntimeSupervisor(
                Path(directory) / "runtime.sqlite3", "demo.env", 30.0,
                process_factory=lambda *args, **kwargs: children.pop(0),
                use_tui=True, dashboard_factory=lambda _: dashboard,
            )
            self.assertEqual(supervisor.run(), 0)
        self.assertTrue(dashboard.started)
        self.assertTrue(dashboard.closed)
        self.assertTrue(any("dashboard started" in line for line in dashboard.logs))

    def test_noninteractive_runtime_announces_log_fallback(self) -> None:
        children = [_Process(), _Process(), _Process()]
        output = StringIO()
        def interrupt(_: float) -> None: raise KeyboardInterrupt
        with tempfile.TemporaryDirectory() as directory:
            supervisor = DemoRuntimeSupervisor(
                Path(directory) / "runtime.sqlite3", "demo.env", 30.0,
                process_factory=lambda *args, **kwargs: children.pop(0),
                output=output, sleep=interrupt,
            )
            with patch("traderrd.tui.TerminalDashboard.supported", return_value=False), patch(
                "traderrd.tui.TerminalDashboard.unavailable_reason", return_value="TERM='dumb' has no supported full-screen capability"
            ):
                self.assertEqual(supervisor.run(), 130)
        self.assertIn("dashboard unavailable", output.getvalue())
        self.assertIn("using prefixed logs", output.getvalue())

    def test_runtime_lock_rejects_a_second_owner_and_clears_on_exit(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            lock_path = Path(directory) / "demo.lock"
            with _exclusive_runtime_lock(lock_path):
                with self.assertRaises(DemoRuntimeAlreadyRunningError):
                    with _exclusive_runtime_lock(lock_path):
                        pass
            with _exclusive_runtime_lock(lock_path):
                pass

    def test_cli_routes_demo_run_with_interval_and_no_real_account_switch(self) -> None:
        with (
            patch(
                "sys.argv",
                [
                    "traderrd",
                    "demo-run",
                    "--interval-seconds",
                    "12",
                    "--database-path",
                    "runtime.sqlite3",
                    "--env-file",
                    "demo.env",
                ],
            ),
            patch("traderrd.demo_cli.run_demo_runtime", return_value=0) as run,
        ):
            self.assertEqual(main(), 0)
        run.assert_called_once_with("runtime.sqlite3", "demo.env", 12.0)

    def test_cli_rejects_mutation_flags_on_demo_run(self) -> None:
        with (
            patch(
                "sys.argv",
                ["traderrd", "demo-run", "--apply-demo-worker"],
            ),
            self.assertRaises(SystemExit) as raised,
        ):
            main()
        self.assertEqual(raised.exception.code, 2)


if __name__ == "__main__":
    unittest.main()
