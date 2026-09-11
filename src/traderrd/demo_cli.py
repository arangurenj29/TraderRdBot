from __future__ import annotations

import os
from pathlib import Path
import sqlite3
import time

from traderrd.application.demo_execution import (
    DemoExecutionPlanner,
    DemoExecutionService,
)
from traderrd.application.demo_bridge import DemoBridgeError
from traderrd.application.demo_monitor import (
    DemoLifecycleMonitor,
    DemoMonitorError,
)
from traderrd.application.demo_worker import (
    DemoSignalWorker,
    DemoWorkerError,
)
from traderrd.demo_runtime import run_demo_runtime, _exclusive_runtime_lock, _LOCK_NAME, DemoRuntimeAlreadyRunningError
from traderrd.config import load_dotenv
from traderrd.domain.execution import ExecutionIntentKind, ExecutionIntentState
from traderrd.infrastructure.bybit_demo import (
    BybitDemoConfig,
    BybitDemoCredentials,
    BybitDemoPreflight,
    BybitV5DemoClient,
    BybitDemoAccountSnapshotProvider,
    DEMO_BASE_URL,
    DemoExecutionError,
)
from traderrd.infrastructure.execution_repository import (
    SQLiteDemoExecutionRepository,
)
from traderrd.infrastructure.bridge_repository import SQLiteDemoBridgeRepository
from traderrd.infrastructure.heartbeat_repository import SafeHeartbeat
from traderrd.infrastructure.risk_repository import SQLiteRiskStateRepository


def load_demo_client(env_path: str | Path = ".env") -> BybitV5DemoClient:
    load_dotenv(env_path)
    credentials = BybitDemoCredentials(
        api_key=os.getenv("BYBIT_DEMO_API_KEY", ""),
        api_secret=os.getenv("BYBIT_DEMO_API_SECRET", ""),
    )
    config = BybitDemoConfig(
        credentials=credentials,
        base_url=os.getenv("BYBIT_DEMO_BASE_URL", DEMO_BASE_URL),
        recv_window_ms=_integer_env("BYBIT_DEMO_RECV_WINDOW_MS", 5000),
        timeout_seconds=_float_env("BYBIT_DEMO_TIMEOUT_SECONDS", 5.0),
        read_retries=_integer_env("BYBIT_DEMO_READ_RETRIES", 2),
    )
    return BybitV5DemoClient(config)


def run_demo_preflight(symbol: str, env_path: str | Path = ".env") -> int:
    try:
        report = BybitDemoPreflight(load_demo_client(env_path)).run(symbol)
        rules = report.instrument
        print(
            "Bybit Demo Trading preflight passed: "
            f"symbol={report.symbol} isolated_margin=true one_way_mode=true "
            f"available_balance={report.available_balance} "
            f"exchange_fee_verification={report.fee_verification.value} "
            "risk_cost_source=configured_estimate "
            f"tick_size={rules.tick_size} quantity_step={rules.quantity_step} "
            f"min_quantity={rules.min_quantity} "
            f"min_notional={rules.min_notional} "
            f"time_offset_ms={report.server_time_offset_ms} read_only=true"
        )
        return 0
    except (ValueError, DemoExecutionError) as exc:
        print(f"Bybit Demo Trading preflight failed: {exc}")
        return 1


def run_demo_execute(
    database_path: str | Path,
    symbol: str,
    risk_command_id: str | None,
    submit_demo: bool,
    env_path: str | Path = ".env",
) -> int:
    try:
        planner = DemoExecutionPlanner(database_path)
        if not submit_demo:
            count = planner.action_count(risk_command_id)
            print(
                "Bybit Demo Trading execution dry run: "
                f"risk_actions={count} private_requests=0 database_writes=false "
                "orders_submitted=0 mainnet_enabled=false"
            )
            return 0
        if not risk_command_id:
            raise ValueError("Explicit submission requires one risk command ID")
        client = load_demo_client(env_path)
        report = BybitDemoPreflight(client).run(symbol)
        intents = planner.preview(report.instrument, risk_command_id)
        if not intents:
            raise ValueError("Risk command produced no executable action for symbol")
        repository = SQLiteDemoExecutionRepository(database_path)
        repository.initialize()
        service = DemoExecutionService(client, repository, report.instrument)
        acknowledged = 0
        blocked_dependencies = 0
        cancellation_confirmed = True
        for intent in intents:
            if intent.kind is ExecutionIntentKind.CANCEL_ENTRY:
                stored = service.submit(intent)
                acknowledged += int(stored.state is ExecutionIntentState.ACKNOWLEDGED)
                cancellation_confirmed = stored.state is ExecutionIntentState.CANCELLED
                continue
            if (
                intent.kind is ExecutionIntentKind.ENTRY
                and not cancellation_confirmed
            ):
                blocked_dependencies += 1
                continue
            stored = service.submit(intent)
            acknowledged += int(stored.state is ExecutionIntentState.ACKNOWLEDGED)
        print(
            "Bybit Demo Trading submission complete: "
            f"intents={len(intents)} acknowledgements={acknowledged} "
            f"blocked_dependencies={blocked_dependencies} "
            "fills=0 acknowledgement_is_fill=false mainnet_enabled=false"
        )
        return 0
    except (ValueError, sqlite3.Error, DemoExecutionError) as exc:
        print(f"Bybit Demo Trading execution failed: {exc}")
        return 1


def run_demo_reconcile(
    database_path: str | Path,
    intent_id: str,
    apply_reconciliation: bool,
    env_path: str | Path = ".env",
    *,
    close_owned_position: bool = False,
) -> int:
    if close_owned_position and apply_reconciliation:
        try:
            with _exclusive_runtime_lock(Path(database_path).resolve().parent / _LOCK_NAME):
                return _run_demo_reconcile(database_path, intent_id, apply_reconciliation,
                                           env_path, close_owned_position=True)
        except (DemoRuntimeAlreadyRunningError, OSError) as exc:
            print(f"Bybit Demo Trading reconciliation failed: {exc}")
            return 1
    return _run_demo_reconcile(database_path, intent_id, apply_reconciliation, env_path,
                               close_owned_position=close_owned_position)


def _run_demo_reconcile(
    database_path: str | Path,
    intent_id: str,
    apply_reconciliation: bool,
    env_path: str | Path = ".env",
    *,
    close_owned_position: bool = False,
) -> int:
    try:
        repository = SQLiteDemoExecutionRepository(database_path)
        intent = repository.get(intent_id)
        if intent is None:
            raise ValueError("Execution intent does not exist")
        if not apply_reconciliation:
            print(
                "Bybit Demo Trading reconciliation dry run: "
                f"state={intent.state.value} private_requests=0 "
                "database_writes=false mainnet_enabled=false"
            )
            return 0
        client = load_demo_client(env_path)
        report = BybitDemoPreflight(client).run(intent.symbol)
        service = DemoExecutionService(client, repository, report.instrument)
        if close_owned_position:
            repository.initialize()
            risk = SQLiteRiskStateRepository(database_path)
            risk.initialize()
            reconciled = DemoLifecycleMonitor(repository, risk, client,
                BybitDemoAccountSnapshotProvider(client)).close_owned_position(intent.intent_id)
            state = risk.load_state()
            released = state is not None and state.reservations[intent.risk_reservation_id].status.value == "closed"
            print("Bybit Demo owned operator close: "
                  f"state={reconciled.state.value} risk_released={str(released).lower()} mainnet_enabled=false")
            return 0 if released else 1
        else:
            reconciled = service.reconcile(intent.intent_id)
        print(
            "Bybit Demo Trading reconciliation complete: "
            f"state={reconciled.state.value} "
            "acknowledgement_is_fill=false mainnet_enabled=false"
        )
        return 0
    except (ValueError, sqlite3.Error, DemoExecutionError, DemoMonitorError) as exc:
        print(f"Bybit Demo Trading reconciliation failed: {exc}")
        return 1


def run_demo_monitor(
    database_path: str | Path,
    apply_monitor: bool,
    watch: bool,
    interval_seconds: float,
    env_path: str | Path = ".env",
) -> int:
    repository = SQLiteDemoExecutionRepository(database_path)
    if not apply_monitor:
        try:
            active = len(repository.monitorable_intents())
        except sqlite3.OperationalError:
            active = 0
        print(
            "Bybit Demo lifecycle monitor dry run: "
            f"active_intents={active} private_requests=0 "
            "database_writes=false orders_submitted=0 mainnet_enabled=false"
        )
        return 0

    heartbeat = SafeHeartbeat(database_path, "monitor")
    heartbeat.ensure()
    heartbeat.started("startup")
    try:
        if interval_seconds <= 0:
            raise ValueError("Demo monitor interval must be positive")
        repository.initialize()
        risk_repository = SQLiteRiskStateRepository(database_path)
        risk_repository.initialize()
        client = load_demo_client(env_path)
        monitor = DemoLifecycleMonitor(
            repository,
            risk_repository,
            client,
            BybitDemoAccountSnapshotProvider(client),
        )
    except (ValueError, sqlite3.Error, DemoExecutionError) as exc:
        heartbeat.error(_runtime_error_code(exc, "monitor_setup_error"), "setup")
        print(
            "Bybit Demo lifecycle monitor failed: "
            f"code={getattr(exc, 'code', 'monitor_setup_error')}"
        )
        return 1
    try:
        while True:
            try:
                result = monitor.run_cycle()
            except (DemoExecutionError, DemoMonitorError, ValueError, sqlite3.Error) as exc:
                heartbeat.error(_runtime_error_code(exc, "monitor_error"), "cycle")
                print(
                    "Bybit Demo lifecycle monitor failed: "
                    f"code={getattr(exc, 'code', 'monitor_error')}"
                )
                if not watch:
                    return 1
            else:
                heartbeat.healthy(
                    "cycle",
                    inspected=result.inspected,
                    risk_updates=result.risk_updates,
                )
                print(
                    "Bybit Demo lifecycle monitor: "
                    f"inspected={result.inspected} working={result.working} "
                    f"filled={result.filled} protected={result.protected} "
                    f"cancelled={result.cancelled} risk_updates={result.risk_updates} "
                    "orders_submitted=0 mainnet_enabled=false"
                )
                if not watch:
                    heartbeat.stopped("one_shot_complete")
                    return 0
            time.sleep(interval_seconds)
    except KeyboardInterrupt:
        heartbeat.stopped("user_interrupt")
        print("Bybit Demo lifecycle monitor stopped by user")
        return 130


def run_demo_worker(
    database_path: str | Path,
    apply_worker: bool,
    watch: bool,
    interval_seconds: float,
    env_path: str | Path = ".env",
) -> int:
    """Run the fresh-signal Demo worker; dry-run is the default."""
    heartbeat = (
        SafeHeartbeat(database_path, "worker") if (apply_worker or watch) else None
    )
    if heartbeat is not None:
        heartbeat.ensure()
        heartbeat.started("startup")
    try:
        if interval_seconds <= 0:
            raise ValueError("Demo worker interval must be positive")
        repository = SQLiteDemoExecutionRepository(database_path)
        bridge_repository = SQLiteDemoBridgeRepository(database_path)
        risk_repository = SQLiteRiskStateRepository(database_path)
        client = load_demo_client(env_path) if apply_worker else None
        worker = DemoSignalWorker(
            bridge_repository,
            risk_repository,
            repository,
            BybitDemoAccountSnapshotProvider(client) if client is not None else None,
            client,
        )
    except (ValueError, sqlite3.Error, DemoExecutionError) as exc:
        if heartbeat is not None:
            heartbeat.error(_runtime_error_code(exc, "worker_setup_error"), "setup")
        print(
            "Bybit Demo signal worker failed: "
            f"code={getattr(exc, 'code', 'worker_setup_error')}"
        )
        return 1

    try:
        while True:
            try:
                result = worker.run_cycle(apply=apply_worker)
            except (DemoExecutionError, DemoWorkerError, DemoBridgeError, ValueError, sqlite3.Error) as exc:
                if heartbeat is not None:
                    heartbeat.error(_runtime_error_code(exc, "worker_error"), "cycle")
                print(
                    "Bybit Demo signal worker failed: "
                    f"code={getattr(exc, 'code', 'worker_error')}"
                )
                if not watch:
                    return 1
            else:
                if heartbeat is not None:
                    heartbeat.healthy(
                        "cycle",
                        fresh=result.fresh,
                        orders_submitted=result.orders_submitted,
                        cursor=result.cursor,
                    )
                print(
                    "Bybit Demo signal worker: "
                    f"scanned={result.scanned} fresh={result.fresh} "
                    f"stale={result.stale} deferred={result.deferred} "
                    f"duplicate={result.duplicate} "
                    f"risk_accepted={result.risk_accepted} "
                    f"risk_rejected={result.risk_rejected} "
                    f"orders_submitted={result.orders_submitted} "
                    f"blocked_dependencies={result.blocked_dependencies} "
                    f"cursor={result.cursor} "
                    f"database_writes={str(result.database_writes).lower()} "
                    "mainnet_enabled=false"
                )
                if not watch:
                    if heartbeat is not None:
                        heartbeat.stopped("one_shot_complete")
                    return 0
            time.sleep(interval_seconds)
    except KeyboardInterrupt:
        if heartbeat is not None:
            heartbeat.stopped("user_interrupt")
        print("Bybit Demo signal worker stopped by user")
        return 130


def _integer_env(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer") from exc


def _float_env(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except ValueError as exc:
        raise ValueError(f"{name} must be numeric") from exc


def _runtime_error_code(error: BaseException, fallback: str) -> str:
    code = getattr(error, "code", None)
    return str(code) if code else fallback
