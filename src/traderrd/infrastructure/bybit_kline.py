from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import json
import re
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlparse
from urllib.request import Request, urlopen

from traderrd.domain.market_data import MarketDataError
from traderrd.domain.simulation import HistoricalCandle


@dataclass(frozen=True, slots=True)
class KlineEvidence:
    candles: list[HistoricalCandle]
    page_count: int
    raw_row_count: int
    dataset_sha256: str
    provider_timestamps: tuple[datetime, ...]


class BybitPublicKlineClient:
    provider_name = "bybit"
    _SYMBOL = re.compile(r"^[A-Z0-9]{3,30}$")
    _MAX_RESPONSE_BYTES = 2_000_000
    _PAGE_LIMIT = 1000

    def __init__(
        self,
        base_url: str = "https://api.bybit.com",
        category: str = "linear",
        timeout_seconds: float = 5.0,
        opener: Callable[..., Any] = urlopen,
    ) -> None:
        parsed = urlparse(base_url)
        if (
            parsed.scheme != "https"
            or not parsed.netloc
            or parsed.username is not None
            or parsed.path not in ("", "/")
            or parsed.query
            or parsed.fragment
        ):
            raise ValueError("Bybit base URL must be a public HTTPS base URL")
        if category != "linear":
            raise ValueError("Historical simulation requires Bybit category=linear")
        if not 0 < timeout_seconds <= 30:
            raise ValueError(
                "Bybit timeout must be greater than 0 and at most 30 seconds"
            )
        self.base_url = base_url.rstrip("/")
        self.category = category
        self.timeout_seconds = timeout_seconds
        self._opener = opener

    def fetch_candles(
        self,
        symbol: str,
        interval_minutes: int,
        start: datetime,
        end: datetime,
    ) -> KlineEvidence:
        normalized_symbol = symbol.upper()
        if not self._SYMBOL.fullmatch(normalized_symbol):
            raise MarketDataError("invalid_symbol", "Bybit symbol is malformed")
        if interval_minutes != 1:
            raise MarketDataError(
                "unsupported_interval",
                "Historical simulation requires 1-minute candles",
            )
        if start.tzinfo is None or end.tzinfo is None or end <= start:
            raise ValueError("Kline range must be timezone-aware and increasing")

        start_ms = int(start.timestamp() * 1000)
        cursor_end_ms = int(end.timestamp() * 1000)
        rows_by_time: dict[int, list[str]] = {}
        provider_timestamps: list[datetime] = []
        page_count = 0
        raw_row_count = 0

        while cursor_end_ms >= start_ms:
            payload = self._request_page(
                normalized_symbol,
                interval_minutes,
                start_ms,
                cursor_end_ms,
            )
            page_rows, provider_timestamp = self._parse_page(
                payload, normalized_symbol
            )
            page_count += 1
            raw_row_count += len(page_rows)
            provider_timestamps.append(provider_timestamp)
            if not page_rows:
                break
            for row in page_rows:
                rows_by_time[int(row[0])] = row
            earliest = min(int(row[0]) for row in page_rows)
            if earliest <= start_ms or len(page_rows) < self._PAGE_LIMIT:
                break
            next_end = earliest - 1
            if next_end >= cursor_end_ms:
                raise MarketDataError(
                    "pagination_error", "Bybit kline pagination did not advance"
                )
            cursor_end_ms = next_end

        canonical_rows = [
            rows_by_time[key]
            for key in sorted(rows_by_time)
            if start_ms <= key < int(end.timestamp() * 1000)
        ]
        encoded = json.dumps(
            canonical_rows, separators=(",", ":"), ensure_ascii=True
        ).encode("utf-8")
        candles = [
            self._row_to_candle(normalized_symbol, interval_minutes, row)
            for row in canonical_rows
        ]
        return KlineEvidence(
            candles=candles,
            page_count=page_count,
            raw_row_count=raw_row_count,
            dataset_sha256=hashlib.sha256(encoded).hexdigest(),
            provider_timestamps=tuple(provider_timestamps),
        )

    def _request_page(
        self,
        symbol: str,
        interval_minutes: int,
        start_ms: int,
        end_ms: int,
    ) -> object:
        query = urlencode(
            {
                "category": self.category,
                "symbol": symbol,
                "interval": str(interval_minutes),
                "start": str(start_ms),
                "end": str(end_ms),
                "limit": str(self._PAGE_LIMIT),
            }
        )
        request = Request(
            f"{self.base_url}/v5/market/kline?{query}",
            headers={
                "Accept": "application/json",
                "User-Agent": "TraderRd-Observer/0.1",
            },
            method="GET",
        )
        try:
            with self._opener(request, timeout=self.timeout_seconds) as response:
                status = getattr(response, "status", None) or response.getcode()
                if status == 429:
                    raise MarketDataError("rate_limited", "Bybit rate limit reached")
                if status != 200:
                    raise MarketDataError(
                        "http_error", f"Bybit returned HTTP status {status}"
                    )
                raw = response.read(self._MAX_RESPONSE_BYTES + 1)
        except HTTPError as exc:
            code = "rate_limited" if exc.code == 429 else "http_error"
            raise MarketDataError(
                code, f"Bybit returned HTTP status {exc.code}"
            ) from exc
        except (URLError, TimeoutError, OSError) as exc:
            raise MarketDataError(
                "transport_error", "Bybit public kline data is unavailable"
            ) from exc
        if len(raw) > self._MAX_RESPONSE_BYTES:
            raise MarketDataError(
                "response_too_large", "Bybit kline response exceeded the size limit"
            )
        try:
            return json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise MarketDataError(
                "malformed_response", "Bybit returned malformed kline JSON"
            ) from exc

    def _parse_page(
        self, payload: object, symbol: str
    ) -> tuple[list[list[str]], datetime]:
        if not isinstance(payload, dict) or type(payload.get("retCode")) is not int:
            raise self._malformed()
        ret_code = payload["retCode"]
        if ret_code != 0:
            code = "rate_limited" if ret_code == 10006 else "api_error"
            raise MarketDataError(code, f"Bybit kline API error {ret_code}")
        result = payload.get("result")
        if (
            not isinstance(result, dict)
            or result.get("category") != self.category
            or result.get("symbol") != symbol
            or not isinstance(result.get("list"), list)
        ):
            raise self._malformed()
        rows: list[list[str]] = []
        for raw_row in result["list"]:
            if (
                not isinstance(raw_row, list)
                or len(raw_row) < 7
                or not all(isinstance(value, str) for value in raw_row[:7])
            ):
                raise self._malformed()
            rows.append(raw_row[:7])
        provider_time = payload.get("time")
        if type(provider_time) is not int or provider_time <= 0:
            raise self._malformed()
        try:
            timestamp = datetime.fromtimestamp(provider_time / 1000, tz=timezone.utc)
        except (ValueError, OverflowError, OSError) as exc:
            raise self._malformed() from exc
        return rows, timestamp

    @staticmethod
    def _row_to_candle(
        symbol: str,
        interval_minutes: int,
        row: list[str],
    ) -> HistoricalCandle:
        try:
            timestamp_ms = int(row[0])
            values = [Decimal(value) for value in row[1:7]]
            if any(not value.is_finite() or value < 0 for value in values):
                raise ValueError("Non-finite or negative kline value")
            return HistoricalCandle(
                symbol=symbol,
                interval_minutes=interval_minutes,
                open_time=datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc),
                open_price=values[0],
                high_price=values[1],
                low_price=values[2],
                close_price=values[3],
                volume=values[4],
                turnover=values[5],
            )
        except (InvalidOperation, ValueError, OverflowError, OSError) as exc:
            raise BybitPublicKlineClient._malformed() from exc

    @staticmethod
    def _malformed() -> MarketDataError:
        return MarketDataError(
            "malformed_response", "Bybit returned an unexpected kline payload"
        )
