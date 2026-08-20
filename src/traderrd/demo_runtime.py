"""Foreground supervisor for the Demo-only operational runtime."""

from __future__ import annotations

from dataclasses import dataclass
import fcntl
import os
from pathlib import Path
import queue
import signal
import subprocess
import sys
import threading
import time
from typing import Callable, TextIO


_LOCK_NAME = ".traderrd-demo-run.lock"
_SHUTDOWN_TIMEOUT_SECONDS = 5.0


class DemoRuntimeAlreadyRunningError(RuntimeError):
    """Raised when another foreground Demo runtime holds the operation lock."""


@dataclass(frozen=True, slots=True)
class RuntimeChild:
    name: str
    command: tuple[str, ...]


ProcessFactory = Callable[..., subprocess.Popen[str]]


class DemoRuntimeSupervisor:
    """Own three existing commands while keeping their logs visible in one TTY."""

    def __init__(
        self,
        database_path: str | Path,
        env_path: str | Path,
        interval_seconds: float,
        *,
        process_factory: ProcessFactory = subprocess.Popen,
        output: TextIO | None = None,
        sleep: Callable[[float], None] = time.sleep,
        use_tui: bool | None = None,
        dashboard_factory: Callable[[str | Path], object] | None = None,
    ) -> None:
        self._database_path = str(database_path)
        self._env_path = str(env_path)
        self._interval_seconds = interval_seconds
        self._process_factory = process_factory
        self._output = output or sys.stdout
        self._sleep = sleep
        self._processes: list[tuple[str, subprocess.Popen[str]]] = []
        self._lines: queue.Queue[tuple[str, str | None]] = queue.Queue()
        self._use_tui = use_tui
        self._dashboard_factory = dashboard_factory
        self._dashboard: object | None = None

    def run(self) -> int:
        if self._interval_seconds <= 0:
            raise ValueError("Demo runtime interval must be positive")
        with _exclusive_runtime_lock(Path(self._database_path).parent / _LOCK_NAME):
            self._write("TraderRd Demo runtime starting (Demo Trading only; mainnet disabled)")
            try:
                self._start_dashboard_if_supported()
                self._start_children()
                return self._supervise()
            except KeyboardInterrupt:
                self._write("TraderRd Demo runtime stopping on Ctrl-C")
                return 130
            finally:
                self._stop_children()
                self._close_dashboard()

    def _children(self) -> tuple[RuntimeChild, ...]:
        interval = str(self._interval_seconds)
        common = ("--env-file", self._env_path)
        return (
            RuntimeChild(
                "observer",
                (sys.executable, "-m", "traderrd", "run", *common),
            ),
            RuntimeChild(
                "worker",
                (
                    sys.executable,
                    "-m",
                    "traderrd",
                    "demo-worker",
                    "--apply-demo-worker",
                    "--watch",
                    "--interval-seconds",
                    interval,
                    "--database-path",
                    self._database_path,
                    *common,
                ),
            ),
            RuntimeChild(
                "monitor",
                (
                    sys.executable,
                    "-m",
                    "traderrd",
                    "demo-monitor",
                    "--apply-demo-monitor",
                    "--watch",
                    "--interval-seconds",
                    interval,
                    "--database-path",
                    self._database_path,
                    *common,
                ),
            ),
        )

    def _start_children(self) -> None:
        child_env = os.environ.copy()
        # The observer reads its database path from configuration; make it use
        # exactly the same durable state as worker and monitor.
        child_env["TRADERRD_DB_PATH"] = self._database_path
        for child in self._children():
            process = self._process_factory(
                child.command,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                env=child_env,
            )
            self._processes.append((child.name, process))
            threading.Thread(
                target=self._read_output,
                args=(child.name, process),
                daemon=True,
            ).start()
            self._write(f"[{child.name}] started")

    def _read_output(self, name: str, process: subprocess.Popen[str]) -> None:
        stream = process.stdout
        if stream is None:
            self._lines.put((name, None))
            return
        for line in stream:
            self._lines.put((name, line.rstrip("\n")))
        self._lines.put((name, None))

    def _supervise(self) -> int:
        while True:
            self._drain_lines()
            if self._dashboard is not None and not self._dashboard.step():
                self._write("TraderRd dashboard requested shutdown")
                return 0
            for name, process in self._processes:
                exit_code = process.poll()
                if exit_code is not None:
                    self._drain_lines()
                    self._write(
                        f"[{name}] exited code={exit_code}; stopping all components"
                    )
                    return exit_code if exit_code != 0 else 1
            self._sleep(0.1)

    def _drain_lines(self) -> None:
        while True:
            try:
                name, line = self._lines.get_nowait()
            except queue.Empty:
                return
            if line:
                message = f"[{name}] {line}"
                if self._dashboard is not None:
                    self._dashboard.log(message)
                else:
                    self._write(message)

    def _stop_children(self) -> None:
        for _, process in reversed(self._processes):
            if process.poll() is None:
                process.send_signal(signal.SIGINT)
        deadline = time.monotonic() + _SHUTDOWN_TIMEOUT_SECONDS
        for _, process in reversed(self._processes):
            remaining = max(0.0, deadline - time.monotonic())
            try:
                process.wait(timeout=remaining)
            except subprocess.TimeoutExpired:
                process.terminate()
        for _, process in reversed(self._processes):
            if process.poll() is None:
                try:
                    process.wait(timeout=1.0)
                except subprocess.TimeoutExpired:
                    process.kill()
        self._drain_lines()
        self._processes.clear()

    def _start_dashboard_if_supported(self) -> None:
        from traderrd.tui import TerminalDashboard

        enabled = self._use_tui
        if enabled is None:
            enabled = TerminalDashboard.supported()
        if not enabled:
            self._write(
                "TraderRd dashboard unavailable ("
                + TerminalDashboard.unavailable_reason()
                + "); using prefixed logs. Use --no-tui to request this mode."
            )
            return
        factory = self._dashboard_factory or (lambda database_path: TerminalDashboard(database_path))
        self._dashboard = factory(self._database_path)
        self._dashboard.start()
        self._dashboard.log("[runtime] dashboard started; Demo Trading only")

    def _close_dashboard(self) -> None:
        if self._dashboard is None:
            return
        try:
            self._dashboard.close()
        finally:
            self._dashboard = None

    def _write(self, message: str) -> None:
        if self._dashboard is not None:
            self._dashboard.log(f"[runtime] {message}")
        else:
            print(message, file=self._output, flush=True)


class _exclusive_runtime_lock:
    def __init__(self, path: Path) -> None:
        self._path = path
        self._file: TextIO | None = None

    def __enter__(self) -> "_exclusive_runtime_lock":
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._file = self._path.open("a+", encoding="utf-8")
        try:
            fcntl.flock(self._file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            self._file.close()
            self._file = None
            raise DemoRuntimeAlreadyRunningError(
                "Another 'traderrd demo-run' is already active for this database. "
                "Stop that terminal first; the lock clears automatically when it exits."
            ) from exc
        self._file.seek(0)
        self._file.truncate()
        self._file.write(f"pid={os.getpid()}\n")
        self._file.flush()
        return self

    def __exit__(self, *_: object) -> None:
        if self._file is not None:
            fcntl.flock(self._file.fileno(), fcntl.LOCK_UN)
            self._file.close()
            self._file = None


def run_demo_runtime(
    database_path: str | Path,
    env_path: str | Path,
    interval_seconds: float,
    *,
    use_tui: bool | None = None,
) -> int:
    try:
        return DemoRuntimeSupervisor(
            database_path, env_path, interval_seconds, use_tui=use_tui
        ).run()
    except (DemoRuntimeAlreadyRunningError, OSError, ValueError) as exc:
        print(f"TraderRd Demo runtime failed: {exc}")
        return 1
    except Exception as exc:
        # A terminal capability/render failure must not strand a partially
        # started runtime; ``run`` has already cleaned children and restored curses.
        print(f"TraderRd Demo runtime failed: {type(exc).__name__}: {exc}")
        return 1
