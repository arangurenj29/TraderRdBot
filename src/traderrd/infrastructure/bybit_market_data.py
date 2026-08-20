from __future__ import annotations

from collections.abc import Callable
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import json
import re
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlparse
from urllib.request import Request, urlopen

from traderrd.domain.market_data import MarketDataError, MarketQuote


class BybitPublicMarketDataClient:
    provider_name = "bybit"
    _SYMBOL = re.compile(r"^[A-Z0-9]{3,30}$")
    _MAX_RESPONSE_BYTES = 1_000_000

    def __init__(
        self,
        base_url: str = "https://api.bybit.com",
        category: str = "linear",
        timeout_seconds: float = 3.0,
        opener: Callable[..., Any] = urlopen,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        parsed_base_url = urlparse(base_url)
        if (
            parsed_base_url.scheme != "https"
            or not parsed_base_url.netloc
            or parsed_base_url.username is not None
            or parsed_base_url.path not in ("", "/")
            or parsed_base_url.query
            or parsed_base_url.fragment
        ):
            raise ValueError("Bybit base URL must be a public HTTPS base URL")
        if category != "linear":
            raise ValueError("TraderRd quote capture requires Bybit category=linear")
        if not 0 < timeout_seconds <= 30:
            raise ValueError("Bybit timeout must be greater than 0 and at most 30 seconds")
        self.base_url = base_url.rstrip("/")
        self.category = category
        self.timeout_seconds = timeout_seconds
        self._opener = opener
        self._clock = clock or (lambda: datetime.now(timezone.utc))

    def fetch_quote(self, symbol: str) -> MarketQuote:
        normalized_symbol = symbol.upper()
        if not self._SYMBOL.fullmatch(normalized_symbol):
            raise MarketDataError("invalid_symbol", "Bybit symbol is malformed")

        query = urlencode(
            {"category": self.category, "symbol": normalized_symbol}
        )
        request = Request(
            f"{self.base_url}/v5/market/tickers?{query}",
            headers={
                "Accept": "application/json",
                "User-Agent": "TraderRd-Observer/0.1",
            },
            method="GET",
        )
        payload = self._request_json(request)
        return self._parse_payload(payload, normalized_symbol)

    def _request_json(self, request: Request) -> object:
        try:
            with self._opener(request, timeout=self.timeout_seconds) as response:
                status = getattr(response, "status", None)
                if status is None:
                    status = response.getcode()
                if status != 200:
                    raise MarketDataError(
                        "http_error", f"Bybit returned HTTP status {status}"
                    )
                raw = response.read(self._MAX_RESPONSE_BYTES + 1)
        except HTTPError as exc:
            raise MarketDataError(
                "http_error", f"Bybit returned HTTP status {exc.code}"
            ) from exc
        except (URLError, TimeoutError, OSError) as exc:
            raise MarketDataError(
                "unavailable", "Bybit public market data is unavailable"
            ) from exc

        if len(raw) > self._MAX_RESPONSE_BYTES:
            raise MarketDataError(
                "response_too_large", "Bybit response exceeded the size limit"
            )
        try:
            return json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise MarketDataError(
                "malformed_response", "Bybit returned malformed JSON"
            ) from exc

    def _parse_payload(self, payload: object, symbol: str) -> MarketQuote:
        if not isinstance(payload, dict):
            raise self._malformed()
        ret_code = payload.get("retCode")
        if type(ret_code) is not int:
            raise self._malformed()
        if ret_code != 0:
            ret_msg = self._safe_message(payload.get("retMsg"))
            raise MarketDataError(
                "api_error", f"Bybit API error {ret_code}: {ret_msg}"
            )

        result = payload.get("result")
        if not isinstance(result, dict) or result.get("category") != self.category:
            raise self._malformed()
        rows = result.get("list")
        if not isinstance(rows, list):
            raise self._malformed()
        matches = [
            row for row in rows if isinstance(row, dict) and row.get("symbol") == symbol
        ]
        if not matches:
            raise MarketDataError(
                "unsupported_symbol", f"Bybit has no linear ticker for {symbol}"
            )
        if len(matches) != 1:
            raise self._malformed()
        row = matches[0]

        provider_timestamp = self._provider_timestamp(payload.get("time"))
        captured_at = self._clock()
        if captured_at.tzinfo is None:
            raise ValueError("Bybit client clock must return a timezone-aware datetime")
        try:
            return MarketQuote(
                provider=self.provider_name,
                category=self.category,
                symbol=symbol,
                best_bid=self._required_decimal(row, "bid1Price"),
                best_ask=self._required_decimal(row, "ask1Price"),
                last_price=self._required_decimal(row, "lastPrice"),
                mark_price=self._optional_decimal(row, "markPrice"),
                provider_timestamp=provider_timestamp,
                captured_at=captured_at,
            )
        except ValueError as exc:
            raise self._malformed() from exc

    @staticmethod
    def _required_decimal(row: dict[str, object], field: str) -> Decimal:
        raw = row.get(field)
        if not isinstance(raw, str) or not raw:
            raise ValueError(f"Missing {field}")
        try:
            value = Decimal(raw)
        except InvalidOperation as exc:
            raise ValueError(f"Invalid {field}") from exc
        if not value.is_finite() or value <= 0:
            raise ValueError(f"Invalid {field}")
        return value

    @classmethod
    def _optional_decimal(
        cls, row: dict[str, object], field: str
    ) -> Decimal | None:
        raw = row.get(field)
        if raw in (None, ""):
            return None
        return cls._required_decimal(row, field)

    @staticmethod
    def _provider_timestamp(raw: object) -> datetime:
        if type(raw) is not int or raw <= 0:
            raise MarketDataError(
                "malformed_response", "Bybit response has an invalid timestamp"
            )
        try:
            return datetime.fromtimestamp(raw / 1000, tz=timezone.utc)
        except (OverflowError, OSError, ValueError) as exc:
            raise MarketDataError(
                "malformed_response", "Bybit response has an invalid timestamp"
            ) from exc

    @staticmethod
    def _safe_message(raw: object) -> str:
        if not isinstance(raw, str):
            return "unknown error"
        cleaned = " ".join(raw.split())[:160]
        return cleaned or "unknown error"

    @staticmethod
    def _malformed() -> MarketDataError:
        return MarketDataError(
            "malformed_response", "Bybit returned an unexpected ticker payload"
        )
