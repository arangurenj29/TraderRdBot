from __future__ import annotations

from datetime import timedelta
from decimal import Decimal
from pathlib import Path

from traderrd.application.historical_simulation import (
    HistoricalSimulationService,
    SimulationCounters,
)
from traderrd.domain.simulation import SimulationSettings
from traderrd.infrastructure.bybit_kline import BybitPublicKlineClient
from traderrd.infrastructure.simulation_repository import (
    SQLiteHistoricalSimulationRepository,
)
from traderrd.infrastructure.sqlite_repository import SQLiteSignalRepository


def run_historical_simulation(
    database_path: str | Path,
    source_chat_id: int,
    source_topic_id: int,
    exit_horizon_hours: int,
    limit: int | None,
    fetch_public_data: bool,
    bybit_base_url: str = "https://api.bybit.com",
    bybit_timeout_seconds: float = 5.0,
    provider: object | None = None,
) -> int:
    path = Path(database_path)
    if not path.is_file():
        print(f"Simulation error: SQLite database does not exist at {path}")
        return 1

    repository = SQLiteHistoricalSimulationRepository(path)
    try:
        signals = repository.list_eligible_signals(
            source_chat_id, source_topic_id, limit
        )
    except Exception:
        print("Simulation error: database schema is not initialized")
        return 1

    if not fetch_public_data:
        print(
            "Historical simulation dry run: "
            f"eligible={len(signals)} public_data_requested=false "
            "database_writes=false"
        )
        return 0

    SQLiteSignalRepository(path).initialize()
    repository.initialize()
    settings = SimulationSettings(
        entry_window=timedelta(hours=3),
        exit_horizon=timedelta(hours=exit_horizon_hours),
        interval_minutes=1,
    )
    model_version = (
        "bybit-linear-1m-post-only-v1-"
        f"entry3h-exit{exit_horizon_hours}h"
    )
    market_provider = provider or BybitPublicKlineClient(
        base_url=bybit_base_url,
        category="linear",
        timeout_seconds=bybit_timeout_seconds,
    )
    service = HistoricalSimulationService(
        repository=repository,
        provider=market_provider,
        source_chat_id=source_chat_id,
        source_topic_id=source_topic_id,
        settings=settings,
        model_version=model_version,
    )
    counters = service.run(limit=limit)
    print(_format_counters(counters))
    return 0 if counters.errors == 0 else 1


def _format_counters(counters: SimulationCounters) -> str:
    gross = _format_decimal(counters.resolved_gross_return_percent)
    return (
        "Historical simulation complete: "
        f"eligible={counters.eligible} attempted={counters.attempted} "
        f"skipped_existing={counters.skipped_existing} "
        f"take_profit_first={counters.take_profit_first} "
        f"stop_loss_first={counters.stop_loss_first} "
        f"no_entry_before_expiry={counters.no_entry_before_expiry} "
        f"no_exit_within_horizon={counters.no_exit_within_horizon} "
        f"intrabar_ambiguity={counters.intrabar_ambiguity} "
        f"data_unavailable={counters.data_unavailable} "
        f"resolved={counters.resolved} "
        f"resolved_gross_return_percent={gross} errors={counters.errors}"
    )


def _format_decimal(value: Decimal) -> str:
    normalized = format(value.normalize(), "f")
    return "0" if normalized in {"-0", ""} else normalized
