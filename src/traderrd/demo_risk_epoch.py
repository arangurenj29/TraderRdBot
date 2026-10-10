"""Explicit, offline Demo risk boundary; never sends exchange mutations."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import DecimalException
from pathlib import Path
import re
import sqlite3

from traderrd.demo_cli import load_demo_client
from traderrd.demo_runtime import _LOCK_NAME, _exclusive_runtime_lock, _demo_operation_lock
from traderrd.domain.bridge import DemoStrategyAccountSnapshot
from traderrd.domain.risk import StartDemoRiskEpoch
from traderrd.infrastructure.bybit_demo import (
    BybitDemoAccountSnapshotProvider, BybitV5DemoClient, DEMO_BASE_URL,
)
from traderrd.infrastructure.risk_repository import SQLiteRiskStateRepository


class _GetOnlyDemoClient:
    """Retain the exact Demo endpoint and forbid even accidental POST calls."""

    def __init__(self, client: BybitV5DemoClient) -> None:
        if client.config.base_url != DEMO_BASE_URL:
            raise ValueError("Risk epoch evidence requires Bybit Demo Trading")
        self._client = client

    def request(
        self, method: str, path: str, params: dict[str, str] | None = None,
        *, private: bool = True,
    ) -> dict:
        if method != "GET":
            raise ValueError("Risk epoch evidence permits GET requests only")
        return self._client.request(method, path, params, private=private)


def _validate_snapshot(snapshot: DemoStrategyAccountSnapshot, now: datetime) -> None:
    age = now - snapshot.captured_at
    if not timedelta(0) <= age <= timedelta(seconds=10):
        raise ValueError("Demo epoch requires fresh account evidence")
    if snapshot.positions or snapshot.orders:
        raise ValueError("Demo epoch requires a flat account with no open orders")
    if (
        snapshot.wallet_balance is None or snapshot.unrealised_pnl is None
        or snapshot.position_initial_margin != 0 or snapshot.order_initial_margin != 0
        or snapshot.unrealised_pnl != 0
        or snapshot.equity != snapshot.wallet_balance + snapshot.unrealised_pnl
    ):
        raise ValueError("Demo epoch account evidence is incomplete or contradictory")


def run_demo_risk_epoch(
    database_path: str | Path,
    epoch_id: str | None,
    operator_reference: str | None,
    apply_epoch: bool,
    env_path: str | Path = ".env",
    symbol: str = "BTCUSDT",
) -> int:
    try:
        repository = SQLiteRiskStateRepository(database_path)
        if not apply_epoch:
            state = repository.check_demo_epoch_ready()
            print("Demo risk epoch dry run: local_ready=true exchange_flatness=not_verified "
                  f"current_epoch={state.risk_epoch_id} private_requests=0 database_writes=false")
            return 0
        if (
            not epoch_id or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,119}", epoch_id)
            or not operator_reference or not operator_reference.strip()
            or len(operator_reference) > 200
        ):
            raise ValueError("An epoch ID and operator reference are required")
        normalized = symbol.upper()
        if not normalized.isalnum() or not normalized.endswith("USDT"):
            raise ValueError("Demo epoch evidence requires a linear USDT symbol")
        with (
            _exclusive_runtime_lock(Path(database_path).resolve().parent / _LOCK_NAME),
            _demo_operation_lock(database_path, exclusive=True),
        ):
            command_id = f"demo-risk-epoch:{epoch_id}"
            existing = repository.get_command_result(command_id)
            if existing is not None:
                stored = repository.get_command(command_id)
                if (
                    not isinstance(stored, StartDemoRiskEpoch)
                    or stored.operator_reference != operator_reference
                    or stored.evidence_symbol != normalized
                ):
                    raise ValueError("Demo epoch ID conflicts with its audited input")
                # The immutable operation already committed. Never fetch new equity
                # or repeat a historical boundary against the current risk state.
                result = existing
            else:
                state = repository.check_demo_epoch_ready()
                provider = BybitDemoAccountSnapshotProvider(
                    _GetOnlyDemoClient(load_demo_client(env_path))
                )
                snapshot, _ = provider.fetch(normalized)
                now = datetime.now(timezone.utc)
                _validate_snapshot(snapshot, now)
                command = StartDemoRiskEpoch(
                    command_id, now, snapshot.equity, operator_reference,
                    state.risk_epoch_id, snapshot.captured_at,
                    snapshot.wallet_balance, snapshot.unrealised_pnl, normalized,
                )
                result = repository.start_demo_epoch(command)
            print("Demo risk epoch: "
                  f"status={result.decision.status} current_epoch={result.state.risk_epoch_id} "
                  f"mode={result.state.mode.value} duplicate={str(result.duplicate_command).lower()} "
                  "exchange_mutations=0 history_preserved=true mainnet_enabled=false")
            return 0 if result.decision.status == "accepted" else 1
    except (ValueError, DecimalException, sqlite3.Error, OSError, RuntimeError) as exc:
        # Do not echo exceptions containing request data, credentials, or paths.
        code = getattr(exc, "code", "epoch_guard_rejected")
        print(f"Demo risk epoch failed: code={code}")
        return 1
