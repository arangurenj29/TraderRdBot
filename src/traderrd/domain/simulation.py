from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from enum import StrEnum

from traderrd.domain.models import Direction


class SimulationOutcome(StrEnum):
    NO_ENTRY_BEFORE_EXPIRY = "no_entry_before_expiry"
    TAKE_PROFIT_FIRST = "take_profit_first"
    STOP_LOSS_FIRST = "stop_loss_first"
    NO_EXIT_WITHIN_HORIZON = "no_exit_within_horizon"
    INTRABAR_AMBIGUITY = "intrabar_ambiguity"
    DATA_UNAVAILABLE = "data_unavailable"


class InsufficientCandleData(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class HistoricalCandle:
    symbol: str
    interval_minutes: int
    open_time: datetime
    open_price: Decimal
    high_price: Decimal
    low_price: Decimal
    close_price: Decimal
    volume: Decimal
    turnover: Decimal

    def __post_init__(self) -> None:
        if self.open_time.tzinfo is None:
            raise ValueError("Candle timestamp must be timezone-aware")
        if self.open_time.second != 0 or self.open_time.microsecond != 0:
            raise ValueError("Candle timestamp must align to a complete minute")
        if not self.symbol or self.symbol != self.symbol.upper():
            raise ValueError("Candle symbol must be uppercase")
        if self.interval_minutes <= 0:
            raise ValueError("Candle interval must be positive")
        if min(
            self.open_price,
            self.high_price,
            self.low_price,
            self.close_price,
        ) <= 0:
            raise ValueError("Candle prices must be positive")
        if self.high_price < max(self.open_price, self.close_price, self.low_price):
            raise ValueError("Candle high price is inconsistent")
        if self.low_price > min(self.open_price, self.close_price, self.high_price):
            raise ValueError("Candle low price is inconsistent")
        if any(
            not value.is_finite() or value < 0
            for value in (self.volume, self.turnover)
        ):
            raise ValueError("Candle volume and turnover must be non-negative")


@dataclass(frozen=True, slots=True)
class SimulationSignal:
    fingerprint: str
    direction: Direction
    symbol: str
    entry: Decimal
    take_profit: Decimal
    stop_loss: Decimal
    signal_timestamp: datetime
    telegram_received_at: datetime
    source_chat_id: int
    source_message_id: int

    def __post_init__(self) -> None:
        if self.telegram_received_at.tzinfo is None:
            raise ValueError("Telegram receipt timestamp must be timezone-aware")
        if not self.symbol or self.symbol != self.symbol.upper():
            raise ValueError("Simulation symbol must be uppercase")
        if min(self.entry, self.take_profit, self.stop_loss) <= 0:
            raise ValueError("Simulation prices must be positive")
        if self.direction is Direction.LONG and not (
            self.take_profit > self.entry > self.stop_loss
        ):
            raise ValueError("LONG simulation prices are inconsistent")
        if self.direction is Direction.SHORT and not (
            self.stop_loss > self.entry > self.take_profit
        ):
            raise ValueError("SHORT simulation prices are inconsistent")
        if min(self.source_chat_id, self.source_message_id) <= 0:
            raise ValueError("Simulation source IDs must be positive")


@dataclass(frozen=True, slots=True)
class SimulationSettings:
    entry_window: timedelta = timedelta(hours=3)
    exit_horizon: timedelta = timedelta(hours=24)
    interval_minutes: int = 1

    def __post_init__(self) -> None:
        if self.entry_window <= timedelta(0) or self.exit_horizon <= timedelta(0):
            raise ValueError("Simulation windows must be positive")
        if self.interval_minutes <= 0:
            raise ValueError("Simulation interval must be positive")


@dataclass(frozen=True, slots=True)
class SimulationResult:
    outcome: SimulationOutcome
    evaluation_start: datetime
    entry_expiry: datetime
    exit_horizon_end: datetime | None
    entry_candle: HistoricalCandle | None
    exit_candle: HistoricalCandle | None
    candle_count: int
    gross_return_percent: Decimal | None
    ambiguity_reason: str | None = None


class HistoricalOutcomeSimulator:
    def __init__(self, settings: SimulationSettings | None = None) -> None:
        self.settings = settings or SimulationSettings()

    def required_range(self, signal: SimulationSignal) -> tuple[datetime, datetime]:
        start = _ceil_minute(signal.telegram_received_at)
        end = signal.telegram_received_at + self.settings.entry_window
        end += self.settings.exit_horizon
        return start, end

    def simulate(
        self,
        signal: SimulationSignal,
        candles: list[HistoricalCandle],
    ) -> SimulationResult:
        evaluation_start, _ = self.required_range(signal)
        entry_expiry = signal.telegram_received_at + self.settings.entry_window
        interval = timedelta(minutes=self.settings.interval_minutes)
        ordered = sorted(candles, key=lambda candle: candle.open_time)
        if any(
            candle.symbol != signal.symbol
            or candle.interval_minutes != self.settings.interval_minutes
            for candle in ordered
        ):
            raise InsufficientCandleData(
                "Historical candles do not match the signal instrument and interval"
            )
        eligible_entry = [
            candle
            for candle in ordered
            if candle.open_time >= evaluation_start
            and candle.open_time + interval <= entry_expiry
        ]
        self._require_contiguous(
            eligible_entry,
            evaluation_start,
            _floor_minute(entry_expiry) - interval,
        )

        entry_index: int | None = None
        eligible_times = {candle.open_time for candle in eligible_entry}
        for index, candle in enumerate(ordered):
            if candle.open_time not in eligible_times:
                continue
            if candle.low_price <= signal.entry <= candle.high_price:
                entry_index = index
                break

        if entry_index is None:
            return SimulationResult(
                outcome=SimulationOutcome.NO_ENTRY_BEFORE_EXPIRY,
                evaluation_start=evaluation_start,
                entry_expiry=entry_expiry,
                exit_horizon_end=None,
                entry_candle=None,
                exit_candle=None,
                candle_count=len(eligible_entry),
                gross_return_percent=None,
            )

        entry_candle = ordered[entry_index]
        entry_tp, entry_sl = self._barrier_hits(signal, entry_candle)
        horizon_end = entry_candle.open_time + self.settings.exit_horizon
        if entry_tp or entry_sl:
            return SimulationResult(
                outcome=SimulationOutcome.INTRABAR_AMBIGUITY,
                evaluation_start=evaluation_start,
                entry_expiry=entry_expiry,
                exit_horizon_end=horizon_end,
                entry_candle=entry_candle,
                exit_candle=entry_candle,
                candle_count=entry_index + 1,
                gross_return_percent=None,
                ambiguity_reason="entry_and_exit_threshold_share_candle",
            )

        evaluated = [entry_candle]
        expected_next = entry_candle.open_time + interval
        for candle in ordered[entry_index + 1 :]:
            if candle.open_time + interval > horizon_end:
                break
            if candle.open_time != expected_next:
                raise InsufficientCandleData(
                    "Historical candles contain a gap before outcome resolution"
                )
            evaluated.append(candle)
            expected_next += interval
            tp_hit, sl_hit = self._barrier_hits(signal, candle)
            if tp_hit and sl_hit:
                return SimulationResult(
                    outcome=SimulationOutcome.INTRABAR_AMBIGUITY,
                    evaluation_start=evaluation_start,
                    entry_expiry=entry_expiry,
                    exit_horizon_end=horizon_end,
                    entry_candle=entry_candle,
                    exit_candle=candle,
                    candle_count=len(evaluated),
                    gross_return_percent=None,
                    ambiguity_reason="take_profit_and_stop_loss_share_candle",
                )
            if tp_hit:
                return self._resolved_result(
                    signal,
                    SimulationOutcome.TAKE_PROFIT_FIRST,
                    evaluation_start,
                    entry_expiry,
                    horizon_end,
                    entry_candle,
                    candle,
                    len(evaluated),
                )
            if sl_hit:
                return self._resolved_result(
                    signal,
                    SimulationOutcome.STOP_LOSS_FIRST,
                    evaluation_start,
                    entry_expiry,
                    horizon_end,
                    entry_candle,
                    candle,
                    len(evaluated),
                )

        if expected_next < _floor_minute(horizon_end):
            raise InsufficientCandleData(
                "Historical candles do not cover the complete exit horizon"
            )
        return SimulationResult(
            outcome=SimulationOutcome.NO_EXIT_WITHIN_HORIZON,
            evaluation_start=evaluation_start,
            entry_expiry=entry_expiry,
            exit_horizon_end=horizon_end,
            entry_candle=entry_candle,
            exit_candle=None,
            candle_count=len(evaluated),
            gross_return_percent=None,
        )

    def _require_contiguous(
        self,
        candles: list[HistoricalCandle],
        expected_start: datetime,
        expected_last: datetime,
    ) -> None:
        if expected_last < expected_start:
            raise InsufficientCandleData("Entry window contains no complete candles")
        if not candles or candles[0].open_time != expected_start:
            raise InsufficientCandleData(
                "Historical candles do not cover the entry window start"
            )
        interval = timedelta(minutes=self.settings.interval_minutes)
        expected = expected_start
        for candle in candles:
            if candle.open_time != expected:
                raise InsufficientCandleData(
                    "Historical candles contain a gap in the entry window"
                )
            expected += interval
        if candles[-1].open_time < expected_last:
            raise InsufficientCandleData(
                "Historical candles do not cover the entry window expiry"
            )

    @staticmethod
    def _barrier_hits(
        signal: SimulationSignal,
        candle: HistoricalCandle,
    ) -> tuple[bool, bool]:
        if signal.direction is Direction.LONG:
            return (
                candle.high_price >= signal.take_profit,
                candle.low_price <= signal.stop_loss,
            )
        return (
            candle.low_price <= signal.take_profit,
            candle.high_price >= signal.stop_loss,
        )

    @staticmethod
    def _resolved_result(
        signal: SimulationSignal,
        outcome: SimulationOutcome,
        evaluation_start: datetime,
        entry_expiry: datetime,
        horizon_end: datetime,
        entry_candle: HistoricalCandle,
        exit_candle: HistoricalCandle,
        candle_count: int,
    ) -> SimulationResult:
        exit_price = (
            signal.take_profit
            if outcome is SimulationOutcome.TAKE_PROFIT_FIRST
            else signal.stop_loss
        )
        if signal.direction is Direction.LONG:
            gross_return = (exit_price - signal.entry) / signal.entry * Decimal("100")
        else:
            gross_return = (signal.entry - exit_price) / signal.entry * Decimal("100")
        return SimulationResult(
            outcome=outcome,
            evaluation_start=evaluation_start,
            entry_expiry=entry_expiry,
            exit_horizon_end=horizon_end,
            entry_candle=entry_candle,
            exit_candle=exit_candle,
            candle_count=candle_count,
            gross_return_percent=gross_return,
        )


def _ceil_minute(value: datetime) -> datetime:
    aware = value.astimezone(timezone.utc)
    floored = aware.replace(second=0, microsecond=0)
    return floored if aware == floored else floored + timedelta(minutes=1)


def _floor_minute(value: datetime) -> datetime:
    return value.astimezone(timezone.utc).replace(second=0, microsecond=0)
