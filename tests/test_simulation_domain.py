from datetime import datetime, timedelta, timezone
from decimal import Decimal
import unittest

from traderrd.domain.models import Direction
from traderrd.domain.simulation import (
    HistoricalCandle,
    HistoricalOutcomeSimulator,
    InsufficientCandleData,
    SimulationOutcome,
    SimulationSettings,
    SimulationSignal,
)


UTC = timezone.utc


def candle(
    minute: int,
    low: str = "99.5",
    high: str = "100.5",
    symbol: str = "TESTUSDT",
) -> HistoricalCandle:
    open_time = datetime(2026, 8, 15, tzinfo=UTC) + timedelta(minutes=minute)
    midpoint = (Decimal(low) + Decimal(high)) / 2
    return HistoricalCandle(
        symbol=symbol,
        interval_minutes=1,
        open_time=open_time,
        open_price=midpoint,
        high_price=Decimal(high),
        low_price=Decimal(low),
        close_price=midpoint,
        volume=Decimal("10"),
        turnover=Decimal("1000"),
    )


def signal(direction: Direction = Direction.LONG) -> SimulationSignal:
    take_profit = Decimal("100.8") if direction is Direction.LONG else Decimal("99.2")
    stop_loss = Decimal("97") if direction is Direction.LONG else Decimal("103")
    return SimulationSignal(
        fingerprint="fingerprint",
        direction=direction,
        symbol="TESTUSDT",
        entry=Decimal("100"),
        take_profit=take_profit,
        stop_loss=stop_loss,
        signal_timestamp=datetime(2026, 8, 15),
        telegram_received_at=datetime(2026, 8, 15, tzinfo=UTC),
        source_chat_id=2180632014,
        source_message_id=231906,
    )


class HistoricalOutcomeSimulatorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.simulator = HistoricalOutcomeSimulator(
            SimulationSettings(
                entry_window=timedelta(minutes=3),
                exit_horizon=timedelta(minutes=3),
            )
        )

    def test_no_entry_before_expiry(self) -> None:
        candles = [candle(index, "101", "102") for index in range(3)]

        result = self.simulator.simulate(signal(), candles)

        self.assertEqual(result.outcome, SimulationOutcome.NO_ENTRY_BEFORE_EXPIRY)
        self.assertIsNone(result.entry_candle)

    def test_take_profit_first_after_entry(self) -> None:
        candles = [candle(0), candle(1, "99.5", "101"), candle(2)]

        result = self.simulator.simulate(signal(), candles)

        self.assertEqual(result.outcome, SimulationOutcome.TAKE_PROFIT_FIRST)
        self.assertEqual(result.gross_return_percent, Decimal("0.800"))

    def test_stop_loss_first_after_entry(self) -> None:
        candles = [candle(0), candle(1, "96.5", "100.5"), candle(2)]

        result = self.simulator.simulate(signal(), candles)

        self.assertEqual(result.outcome, SimulationOutcome.STOP_LOSS_FIRST)
        self.assertEqual(result.gross_return_percent, Decimal("-3.00"))

    def test_no_exit_within_horizon(self) -> None:
        candles = [candle(index) for index in range(3)]

        result = self.simulator.simulate(signal(), candles)

        self.assertEqual(result.outcome, SimulationOutcome.NO_EXIT_WITHIN_HORIZON)

    def test_same_candle_take_profit_and_stop_is_ambiguous(self) -> None:
        candles = [candle(0), candle(1, "96", "101"), candle(2)]

        result = self.simulator.simulate(signal(), candles)

        self.assertEqual(result.outcome, SimulationOutcome.INTRABAR_AMBIGUITY)
        self.assertEqual(
            result.ambiguity_reason,
            "take_profit_and_stop_loss_share_candle",
        )
        self.assertIsNone(result.gross_return_percent)

    def test_entry_and_exit_threshold_in_same_candle_is_ambiguous(self) -> None:
        candles = [candle(0, "99.5", "101"), candle(1), candle(2)]

        result = self.simulator.simulate(signal(), candles)

        self.assertEqual(result.outcome, SimulationOutcome.INTRABAR_AMBIGUITY)
        self.assertEqual(
            result.ambiguity_reason,
            "entry_and_exit_threshold_share_candle",
        )

    def test_gap_before_outcome_is_data_unavailable(self) -> None:
        candles = [candle(0), candle(2), candle(3)]

        with self.assertRaises(InsufficientCandleData):
            self.simulator.simulate(signal(), candles)

    def test_short_take_profit_return_is_positive(self) -> None:
        candles = [candle(0), candle(1, "99", "100.5"), candle(2)]

        result = self.simulator.simulate(signal(Direction.SHORT), candles)

        self.assertEqual(result.outcome, SimulationOutcome.TAKE_PROFIT_FIRST)
        self.assertEqual(result.gross_return_percent, Decimal("0.800"))

    def test_gap_beyond_long_take_profit_is_still_a_take_profit_hit(self) -> None:
        candles = [candle(0), candle(1, "101", "102"), candle(2)]

        result = self.simulator.simulate(signal(), candles)

        self.assertEqual(result.outcome, SimulationOutcome.TAKE_PROFIT_FIRST)


if __name__ == "__main__":
    unittest.main()
