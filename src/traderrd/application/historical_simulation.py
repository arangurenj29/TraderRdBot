from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import Any, Protocol

from traderrd.domain.market_data import MarketDataError
from traderrd.domain.simulation import (
    HistoricalOutcomeSimulator,
    InsufficientCandleData,
    SimulationOutcome,
    SimulationResult,
    SimulationSettings,
    SimulationSignal,
)


class HistoricalKlineProvider(Protocol):
    def fetch_candles(
        self,
        symbol: str,
        interval_minutes: int,
        start: datetime,
        end: datetime,
    ) -> Any: ...


class HistoricalSimulationRepository(Protocol):
    def list_eligible_signals(
        self,
        source_chat_id: int,
        source_topic_id: int,
        limit: int | None = None,
    ) -> list[SimulationSignal]: ...

    def has_terminal_outcome(self, fingerprint: str, model_version: str) -> bool: ...

    def record_success(
        self,
        signal: SimulationSignal,
        source_topic_id: int,
        model_version: str,
        settings: SimulationSettings,
        requested_start: datetime,
        requested_end: datetime,
        result: SimulationResult,
        evidence: Any,
    ) -> None: ...

    def record_failure(
        self,
        signal: SimulationSignal,
        source_topic_id: int,
        model_version: str,
        settings: SimulationSettings,
        requested_start: datetime,
        requested_end: datetime,
        error_code: str,
        error_detail: str,
        evidence: Any | None = None,
    ) -> None: ...


@dataclass(slots=True)
class SimulationCounters:
    eligible: int = 0
    attempted: int = 0
    skipped_existing: int = 0
    take_profit_first: int = 0
    stop_loss_first: int = 0
    no_entry_before_expiry: int = 0
    no_exit_within_horizon: int = 0
    intrabar_ambiguity: int = 0
    data_unavailable: int = 0
    errors: int = 0
    resolved_gross_return_percent: Decimal = field(
        default_factory=lambda: Decimal("0")
    )

    @property
    def resolved(self) -> int:
        return self.take_profit_first + self.stop_loss_first

    def record(self, result: SimulationResult) -> None:
        attribute = result.outcome.value
        setattr(self, attribute, getattr(self, attribute) + 1)
        if result.gross_return_percent is not None:
            self.resolved_gross_return_percent += result.gross_return_percent


class HistoricalSimulationService:
    def __init__(
        self,
        repository: HistoricalSimulationRepository,
        provider: HistoricalKlineProvider,
        source_chat_id: int,
        source_topic_id: int,
        settings: SimulationSettings | None = None,
        model_version: str = "bybit-linear-1m-post-only-v1",
    ) -> None:
        if source_chat_id <= 0 or source_topic_id <= 0:
            raise ValueError("Historical simulation source IDs must be positive")
        self._repository = repository
        self._provider = provider
        self._source_chat_id = source_chat_id
        self._source_topic_id = source_topic_id
        self._settings = settings or SimulationSettings()
        self._model_version = model_version
        self._simulator = HistoricalOutcomeSimulator(self._settings)

    def run(self, limit: int | None = None) -> SimulationCounters:
        if limit is not None and limit <= 0:
            raise ValueError("Historical simulation limit must be positive")
        signals = self._repository.list_eligible_signals(
            self._source_chat_id,
            self._source_topic_id,
            None,
        )
        counters = SimulationCounters(eligible=len(signals))
        for signal in signals:
            if self._repository.has_terminal_outcome(
                signal.fingerprint, self._model_version
            ):
                counters.skipped_existing += 1
                continue
            if limit is not None and counters.attempted >= limit:
                break

            counters.attempted += 1
            requested_start, requested_end = self._simulator.required_range(signal)
            evidence = None
            try:
                evidence = self._provider.fetch_candles(
                    signal.symbol,
                    self._settings.interval_minutes,
                    requested_start,
                    requested_end,
                )
                result = self._simulator.simulate(signal, evidence.candles)
                self._repository.record_success(
                    signal,
                    self._source_topic_id,
                    self._model_version,
                    self._settings,
                    requested_start,
                    requested_end,
                    result,
                    evidence,
                )
                counters.record(result)
            except MarketDataError as exc:
                self._record_failure(
                    signal,
                    requested_start,
                    requested_end,
                    exc.code,
                    str(exc),
                    evidence,
                )
                counters.data_unavailable += 1
                counters.errors += 1
            except InsufficientCandleData as exc:
                self._record_failure(
                    signal,
                    requested_start,
                    requested_end,
                    "insufficient_candle_data",
                    str(exc),
                    evidence,
                )
                counters.data_unavailable += 1
                counters.errors += 1
        return counters

    def _record_failure(
        self,
        signal: SimulationSignal,
        requested_start: datetime,
        requested_end: datetime,
        error_code: str,
        error_detail: str,
        evidence: Any | None,
    ) -> None:
        self._repository.record_failure(
            signal,
            self._source_topic_id,
            self._model_version,
            self._settings,
            requested_start,
            requested_end,
            error_code,
            error_detail,
            evidence,
        )
