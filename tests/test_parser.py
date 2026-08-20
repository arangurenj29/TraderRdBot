from decimal import Decimal
import unittest

from traderrd.domain.models import Direction
from traderrd.domain.parser import SignalParseError, SignalParser
from tests.samples import LONG_SIGNAL, SHORT_SIGNAL


class SignalParserTests(unittest.TestCase):
    def setUp(self) -> None:
        self.parser = SignalParser()

    def test_parses_actual_long_sample(self) -> None:
        signal = self.parser.parse(LONG_SIGNAL)
        self.assertEqual(signal.direction, Direction.LONG)
        self.assertEqual(signal.symbol, "XLMUSDT")
        self.assertEqual(signal.timeframe_minutes, 15)
        self.assertEqual(signal.entry, Decimal("0.15688"))
        self.assertEqual(signal.take_profit, Decimal("0.15813504"))
        self.assertEqual(signal.stop_loss, Decimal("0.1521736"))
        self.assertEqual(signal.signal_timestamp.isoformat(), "2026-08-16T02:15:00")

    def test_parses_actual_short_sample_with_hyphen_and_noise(self) -> None:
        signal = self.parser.parse(SHORT_SIGNAL)
        self.assertEqual(signal.direction, Direction.SHORT)
        self.assertEqual(signal.symbol, "HBARUSDT")
        self.assertEqual(signal.take_profit, Decimal("0.06569024"))
        self.assertEqual(signal.stop_loss, Decimal("0.0682066"))

    def test_fingerprint_is_independent_of_decimal_trailing_zeros(self) -> None:
        first = self.parser.parse(LONG_SIGNAL)
        second = self.parser.parse(LONG_SIGNAL.replace("0.15688", "0.156880"))
        self.assertEqual(first.fingerprint, second.fingerprint)

    def test_rejects_malformed_message_with_actionable_error(self) -> None:
        with self.assertRaises(SignalParseError) as raised:
            self.parser.parse("SEÑAL LONG — BTCUSDT")
        self.assertEqual(raised.exception.code, "missing_field")
        self.assertIn("pair/timeframe", str(raised.exception))

    def test_rejects_symbol_mismatch(self) -> None:
        mismatched = LONG_SIGNAL.replace("Par: XLMUSDT", "Par: ETHUSDT")
        with self.assertRaises(SignalParseError) as raised:
            self.parser.parse(mismatched)
        self.assertEqual(raised.exception.code, "symbol_mismatch")

    def test_rejects_wrong_price_direction(self) -> None:
        wrong = SHORT_SIGNAL.replace("0.06569024", "0.067")
        with self.assertRaises(SignalParseError) as raised:
            self.parser.parse(wrong)
        self.assertEqual(raised.exception.code, "invalid_price_order")

    def test_rejects_percentage_price_mismatch(self) -> None:
        wrong = LONG_SIGNAL.replace("0.15813504", "0.1584488")
        with self.assertRaises(SignalParseError) as raised:
            self.parser.parse(wrong)
        self.assertEqual(raised.exception.code, "percentage_price_mismatch")

    def test_rejects_percentage_label_mismatch(self) -> None:
        wrong = LONG_SIGNAL.replace("(+0.8%)", "(+1%)")
        with self.assertRaises(SignalParseError) as raised:
            self.parser.parse(wrong)
        self.assertEqual(raised.exception.code, "percentage_label_mismatch")


if __name__ == "__main__":
    unittest.main()
