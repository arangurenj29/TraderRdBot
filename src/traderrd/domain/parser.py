from __future__ import annotations

from datetime import datetime
from decimal import Decimal, InvalidOperation
import re
import unicodedata

from traderrd.domain.models import Direction, SignalDraft


class SignalParseError(ValueError):
    """A rejected signal body with a stable machine-readable reason."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


class SignalParser:
    _HEADER = re.compile(
        r"SE(?:Ñ|N)AL\s+(LONG|SHORT)\s*[—–-]\s*([A-Z0-9]{3,30})\b",
        re.IGNORECASE,
    )
    _PAIR = re.compile(
        r"\bPar\s*:\s*([A-Z0-9]{3,30})\s*\|\s*TF\s*:\s*(\d+)\b",
        re.IGNORECASE,
    )
    _ENTRY = re.compile(r"\bEntrada\s*:\s*([0-9]+(?:[.,][0-9]+)?)", re.IGNORECASE)
    _TAKE_PROFIT = re.compile(
        r"\bTake\s+Profit\s*\(\s*([+-]?)\s*([0-9]+(?:[.,][0-9]+)?)\s*%\s*\)"
        r"\s*:\s*([0-9]+(?:[.,][0-9]+)?)",
        re.IGNORECASE,
    )
    _STOP_LOSS = re.compile(
        r"\bStop\s+Loss\s*\(\s*([+-]?)\s*([0-9]+(?:[.,][0-9]+)?)\s*%\s*\)"
        r"\s*:\s*([0-9]+(?:[.,][0-9]+)?)",
        re.IGNORECASE,
    )
    _TIMESTAMP = re.compile(
        r"(?m)^[^\d\r\n]*(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2})\s*$"
    )

    _TP_RATE = Decimal("0.008")
    _SL_RATE = Decimal("0.03")
    _RELATIVE_TOLERANCE = Decimal("0.00001")
    _ABSOLUTE_TOLERANCE = Decimal("1e-12")

    def parse(self, body: str) -> SignalDraft:
        if not body or not body.strip():
            raise SignalParseError("empty_message", "The Telegram message body is empty")

        normalized = unicodedata.normalize("NFKC", body)
        header = self._require(self._HEADER, normalized, "header")
        pair = self._require(self._PAIR, normalized, "pair/timeframe")
        entry_match = self._require(self._ENTRY, normalized, "entry")
        tp_match = self._require(self._TAKE_PROFIT, normalized, "take profit")
        sl_match = self._require(self._STOP_LOSS, normalized, "stop loss")
        timestamp_match = self._require(self._TIMESTAMP, normalized, "signal timestamp")

        direction = Direction(header.group(1).upper())
        header_symbol = header.group(2).upper()
        pair_symbol = pair.group(1).upper()
        if header_symbol != pair_symbol:
            raise SignalParseError(
                "symbol_mismatch",
                f"Header symbol {header_symbol} does not match pair symbol {pair_symbol}",
            )

        try:
            timeframe = int(pair.group(2))
        except ValueError as exc:
            raise SignalParseError("invalid_timeframe", "Timeframe must be an integer") from exc
        if not 1 <= timeframe <= 10_080:
            raise SignalParseError(
                "invalid_timeframe", "Timeframe must be between 1 and 10080 minutes"
            )

        entry = self._decimal(entry_match.group(1), "entry")
        take_profit = self._decimal(tp_match.group(3), "take profit")
        stop_loss = self._decimal(sl_match.group(3), "stop loss")
        if min(entry, take_profit, stop_loss) <= 0:
            raise SignalParseError("non_positive_price", "All prices must be positive")

        tp_label = self._signed_percentage(tp_match.group(1), tp_match.group(2))
        sl_label = self._signed_percentage(sl_match.group(1), sl_match.group(2))
        self._validate_percentage_labels(direction, tp_label, sl_label)
        self._validate_price_order(direction, entry, take_profit, stop_loss)
        self._validate_percentage_prices(direction, entry, take_profit, stop_loss)

        try:
            signal_timestamp = datetime.strptime(
                timestamp_match.group(1), "%Y-%m-%d %H:%M"
            )
        except ValueError as exc:
            raise SignalParseError(
                "invalid_timestamp", "Signal timestamp is not a valid calendar date"
            ) from exc

        return SignalDraft(
            direction=direction,
            symbol=pair_symbol,
            timeframe_minutes=timeframe,
            entry=entry,
            take_profit=take_profit,
            stop_loss=stop_loss,
            signal_timestamp=signal_timestamp,
        )

    @staticmethod
    def _require(pattern: re.Pattern[str], text: str, field: str) -> re.Match[str]:
        match = pattern.search(text)
        if match is None:
            raise SignalParseError("missing_field", f"Missing or malformed {field} field")
        return match

    @staticmethod
    def _decimal(raw: str, field: str) -> Decimal:
        try:
            return Decimal(raw.replace(",", "."))
        except InvalidOperation as exc:
            raise SignalParseError("invalid_decimal", f"Invalid decimal in {field}") from exc

    @staticmethod
    def _signed_percentage(sign: str, raw: str) -> Decimal:
        value = SignalParser._decimal(raw, "percentage")
        return -value if sign == "-" else value

    @staticmethod
    def _validate_percentage_labels(
        direction: Direction, tp_label: Decimal, sl_label: Decimal
    ) -> None:
        expected = {
            Direction.LONG: (Decimal("0.8"), Decimal("-3")),
            Direction.SHORT: (Decimal("-0.8"), Decimal("3")),
        }[direction]
        if (tp_label, sl_label) != expected:
            raise SignalParseError(
                "percentage_label_mismatch",
                f"{direction.value} requires TP {expected[0]:+}% and SL {expected[1]:+}% labels",
            )

    @staticmethod
    def _validate_price_order(
        direction: Direction, entry: Decimal, take_profit: Decimal, stop_loss: Decimal
    ) -> None:
        valid = (
            take_profit > entry > stop_loss
            if direction is Direction.LONG
            else stop_loss > entry > take_profit
        )
        if not valid:
            requirement = "TP > entry > SL" if direction is Direction.LONG else "SL > entry > TP"
            raise SignalParseError(
                "invalid_price_order", f"{direction.value} requires {requirement}"
            )

    def _validate_percentage_prices(
        self,
        direction: Direction,
        entry: Decimal,
        take_profit: Decimal,
        stop_loss: Decimal,
    ) -> None:
        if direction is Direction.LONG:
            expected_tp = entry * (Decimal("1") + self._TP_RATE)
            expected_sl = entry * (Decimal("1") - self._SL_RATE)
        else:
            expected_tp = entry * (Decimal("1") - self._TP_RATE)
            expected_sl = entry * (Decimal("1") + self._SL_RATE)

        self._require_close("take profit", take_profit, expected_tp)
        self._require_close("stop loss", stop_loss, expected_sl)

    def _require_close(self, field: str, actual: Decimal, expected: Decimal) -> None:
        tolerance = max(
            abs(expected) * self._RELATIVE_TOLERANCE,
            self._ABSOLUTE_TOLERANCE,
        )
        if abs(actual - expected) > tolerance:
            raise SignalParseError(
                "percentage_price_mismatch",
                f"{field.title()} price is inconsistent with the declared strategy percentage",
            )
