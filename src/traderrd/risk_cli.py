from __future__ import annotations

from collections import Counter
from decimal import DecimalException
import json
from pathlib import Path
import sqlite3

from traderrd.application.risk_engine import (
    InMemoryRiskSession,
    RiskEngineService,
    command_from_dict,
)
from traderrd.domain.models import canonical_decimal
from traderrd.domain.risk import ReservationStatus
from traderrd.infrastructure.risk_repository import SQLiteRiskStateRepository


def run_risk_replay(
    commands_file: str | Path,
    database_path: str | Path,
    apply_state: bool,
) -> int:
    try:
        commands = _read_commands(Path(commands_file))
        if apply_state:
            repository = SQLiteRiskStateRepository(database_path)
            repository.initialize()
            executor = RiskEngineService(repository)
        else:
            executor = InMemoryRiskSession()

        statuses: Counter[str] = Counter()
        action_count = 0
        duplicates = 0
        for command in commands:
            result = executor.execute(command)
            statuses[result.decision.status] += 1
            action_count += len(result.decision.actions)
            duplicates += int(result.duplicate_command)
        print(
            "Risk replay complete: "
            f"commands={len(commands)} accepted={statuses['accepted']} "
            f"staged={statuses['staged']} rejected={statuses['rejected']} "
            f"duplicate_decisions={statuses['duplicate']} "
            f"duplicate_commands={duplicates} actions={action_count} "
            f"state_applied={str(apply_state).lower()} exchange_execution=false"
        )
        return 0
    except (
        ValueError,
        DecimalException,
        OSError,
        sqlite3.Error,
        json.JSONDecodeError,
    ) as exc:
        print(f"Risk replay error: {exc}")
        return 1


def run_risk_report(database_path: str | Path) -> int:
    repository = SQLiteRiskStateRepository(database_path)
    state = repository.load_state()
    if state is None:
        print("Risk report: initialized=false")
        return 1
    statuses = Counter(item.status.value for item in state.reservations.values())
    print(
        "Risk report: "
        f"initialized=true mode={state.mode.value} "
        f"equity={canonical_decimal(state.equity)} "
        f"high_watermark={canonical_decimal(state.high_watermark)} "
        f"active_reservations={len(state.active_reservations)} "
        f"pending={statuses[ReservationStatus.PENDING.value]} "
        f"filled={statuses[ReservationStatus.FILLED.value]} "
        f"close_requested={statuses[ReservationStatus.CLOSE_REQUESTED.value]} "
        f"reserved_risk={canonical_decimal(state.total_reserved_risk)} "
        f"daily_halted={str(state.daily_halted).lower()} "
        f"weekly_halted={str(state.weekly_halted).lower()} "
        "exchange_execution=false"
    )
    return 0


def _read_commands(path: Path) -> list[object]:
    if not path.is_file():
        raise ValueError("Risk command file does not exist")
    if path.stat().st_size > 5_000_000:
        raise ValueError("Risk command file exceeds the 5 MB limit")
    commands = []
    with path.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            stripped = line.strip()
            if not stripped or stripped.startswith("#"):
                continue
            payload = json.loads(stripped)
            if not isinstance(payload, dict):
                raise ValueError(
                    f"Risk command line {line_number} must be a JSON object"
                )
            commands.append(command_from_dict(payload))
    if not commands:
        raise ValueError("Risk command file contains no commands")
    return commands
