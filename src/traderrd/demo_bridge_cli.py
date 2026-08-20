from __future__ import annotations

from decimal import Decimal
import os
from pathlib import Path
import sqlite3

from traderrd.application.demo_bridge import DemoBridgeError, DemoSignalBridgeService
from traderrd.demo_cli import load_demo_client
from traderrd.infrastructure.bridge_repository import SQLiteDemoBridgeRepository
from traderrd.infrastructure.bybit_demo import (
    BybitDemoAccountSnapshotProvider,
    DemoExecutionError,
)
from traderrd.infrastructure.execution_repository import (
    SQLiteDemoExecutionRepository,
)
from traderrd.infrastructure.risk_repository import SQLiteRiskStateRepository


def run_demo_bridge(
    database_path: str | Path,
    source_message_id: int,
    source_sender_id: int,
    persist_risk: bool,
    estimated_cost_rate: Decimal,
    env_path: str | Path = ".env",
) -> int:
    try:
        client = load_demo_client(env_path)
        configured_sender_id = _configured_sender_id()
        if source_sender_id != configured_sender_id:
            raise DemoBridgeError(
                "source_sender_mismatch",
                "The requested sender does not match the confirmed source sender",
            )
        service = DemoSignalBridgeService(
            SQLiteDemoBridgeRepository(database_path),
            SQLiteRiskStateRepository(database_path),
            SQLiteDemoExecutionRepository(database_path),
            BybitDemoAccountSnapshotProvider(client),
            estimated_cost_rate=estimated_cost_rate,
        )
        result = service.process(
            source_message_id=source_message_id,
            source_sender_id=source_sender_id,
            persist_risk=persist_risk,
        )
        decision = result.decision
        print(
            "Bybit Demo supervised bridge: "
            f"status={result.status.value} reason={result.reason} "
            f"strategy_equity={result.equity or 'unavailable'} "
            f"decision={decision.status if decision else 'none'} "
            f"risk_command_id={result.risk_command_id or 'none'} "
            f"notional={decision.notional if decision else 'none'} "
            f"projected_risk={decision.projected_risk if decision else 'none'} "
            f"intents={len(result.intents)} "
            f"database_writes={'true' if persist_risk else 'false'} "
            "orders_submitted=0 mainnet_enabled=false"
        )
        return 0
    except (ValueError, sqlite3.Error, DemoExecutionError, DemoBridgeError) as exc:
        code = getattr(exc, "code", "bridge_unavailable")
        print(f"Bybit Demo supervised bridge failed: code={code} detail={exc}")
        return 1


def _configured_sender_id() -> int:
    raw = os.getenv("TELEGRAM_EXPECTED_SENDER_ID", "").strip()
    try:
        value = int(raw)
    except ValueError as exc:
        raise DemoBridgeError(
            "source_sender_configuration",
            "A confirmed numeric Telegram sender configuration is required",
        ) from exc
    if value == 0:
        raise DemoBridgeError(
            "source_sender_configuration",
            "A confirmed numeric Telegram sender configuration is required",
        )
    return value
