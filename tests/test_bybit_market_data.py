from datetime import datetime, timezone
from decimal import Decimal
import json
import unittest
from urllib.error import HTTPError
from urllib.parse import parse_qs, urlparse

from traderrd.domain.market_data import MarketDataError
from traderrd.infrastructure.bybit_market_data import BybitPublicMarketDataClient


CAPTURED_AT = datetime(2026, 8, 16, 12, 0, tzinfo=timezone.utc)


def ticker_payload() -> dict[str, object]:
    return {
        "retCode": 0,
        "retMsg": "OK",
        "result": {
            "category": "linear",
            "list": [
                {
                    "symbol": "XLMUSDT",
                    "bid1Price": "0.15687",
                    "ask1Price": "0.15689",
                    "lastPrice": "0.15688",
                    "markPrice": "0.156875",
                }
            ],
        },
        "time": 1786881573000,
    }


class FakeResponse:
    def __init__(self, body: bytes, status: int = 200) -> None:
        self.body = body
        self.status = status

    def __enter__(self) -> "FakeResponse":
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def getcode(self) -> int:
        return self.status

    def read(self, limit: int) -> bytes:
        return self.body[:limit]


class FakeOpener:
    def __init__(self, payload: object) -> None:
        self.payload = payload
        self.request = None
        self.timeout = None
        self.calls = 0

    def __call__(self, request: object, *, timeout: float) -> FakeResponse:
        self.calls += 1
        self.request = request
        self.timeout = timeout
        return FakeResponse(json.dumps(self.payload).encode("utf-8"))


class BybitPublicMarketDataClientTests(unittest.TestCase):
    def client(self, opener: object) -> BybitPublicMarketDataClient:
        return BybitPublicMarketDataClient(
            opener=opener,
            timeout_seconds=2.5,
            clock=lambda: CAPTURED_AT,
        )

    def test_parses_public_linear_ticker_snapshot(self) -> None:
        opener = FakeOpener(ticker_payload())
        quote = self.client(opener).fetch_quote("xlmusdt")

        self.assertEqual(quote.symbol, "XLMUSDT")
        self.assertEqual(quote.best_bid, Decimal("0.15687"))
        self.assertEqual(quote.best_ask, Decimal("0.15689"))
        self.assertEqual(quote.last_price, Decimal("0.15688"))
        self.assertEqual(quote.mark_price, Decimal("0.156875"))
        self.assertEqual(quote.captured_at, CAPTURED_AT)
        self.assertEqual(quote.provider_timestamp.tzinfo, timezone.utc)
        self.assertEqual(opener.timeout, 2.5)
        query = parse_qs(urlparse(opener.request.full_url).query)
        self.assertEqual(query, {"category": ["linear"], "symbol": ["XLMUSDT"]})
        self.assertNotIn("X-BAPI-API-KEY", dict(opener.request.header_items()))

    def test_maps_bybit_api_error(self) -> None:
        payload = ticker_payload()
        payload["retCode"] = 10006
        payload["retMsg"] = "Too many visits!"
        with self.assertRaises(MarketDataError) as raised:
            self.client(FakeOpener(payload)).fetch_quote("XLMUSDT")
        self.assertEqual(raised.exception.code, "api_error")

    def test_maps_http_error(self) -> None:
        def failing_opener(request: object, *, timeout: float) -> object:
            raise HTTPError(request.full_url, 503, "Unavailable", {}, None)

        with self.assertRaises(MarketDataError) as raised:
            self.client(failing_opener).fetch_quote("XLMUSDT")
        self.assertEqual(raised.exception.code, "http_error")

    def test_rejects_malformed_payload(self) -> None:
        payload = ticker_payload()
        payload["result"]["list"][0].pop("bid1Price")
        with self.assertRaises(MarketDataError) as raised:
            self.client(FakeOpener(payload)).fetch_quote("XLMUSDT")
        self.assertEqual(raised.exception.code, "malformed_response")

    def test_rejects_malformed_json(self) -> None:
        def malformed_opener(request: object, *, timeout: float) -> FakeResponse:
            return FakeResponse(b"not-json")

        with self.assertRaises(MarketDataError) as raised:
            self.client(malformed_opener).fetch_quote("XLMUSDT")
        self.assertEqual(raised.exception.code, "malformed_response")

    def test_reports_missing_or_unsupported_symbol(self) -> None:
        payload = ticker_payload()
        payload["result"]["list"] = []
        with self.assertRaises(MarketDataError) as raised:
            self.client(FakeOpener(payload)).fetch_quote("XLMUSDT")
        self.assertEqual(raised.exception.code, "unsupported_symbol")


if __name__ == "__main__":
    unittest.main()
