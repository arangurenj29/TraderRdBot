from datetime import datetime, timedelta, timezone
import io
import json
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, urlparse
import unittest

from traderrd.domain.market_data import MarketDataError
from traderrd.infrastructure.bybit_kline import BybitPublicKlineClient


UTC = timezone.utc
START = datetime(2026, 8, 15, tzinfo=UTC)


def row(minute: int) -> list[str]:
    timestamp = int((START + timedelta(minutes=minute)).timestamp() * 1000)
    return [str(timestamp), "100", "101", "99", "100.5", "10", "1000"]


def payload(rows: list[list[str]], symbol: str = "BTCUSDT") -> bytes:
    return json.dumps(
        {
            "retCode": 0,
            "retMsg": "OK",
            "result": {"category": "linear", "symbol": symbol, "list": rows},
            "time": 1786752000000,
        }
    ).encode()


class FakeResponse:
    def __init__(self, body: bytes, status: int = 200) -> None:
        self._body = body
        self.status = status

    def __enter__(self) -> "FakeResponse":
        return self

    def __exit__(self, *_: object) -> None:
        return None

    def read(self, _: int) -> bytes:
        return self._body

    def getcode(self) -> int:
        return self.status


class QueueOpener:
    def __init__(self, responses: list[FakeResponse]) -> None:
        self.responses = responses
        self.urls: list[str] = []

    def __call__(self, request: object, timeout: float) -> FakeResponse:
        self.urls.append(request.full_url)  # type: ignore[attr-defined]
        return self.responses.pop(0)


class BybitPublicKlineClientTests(unittest.TestCase):
    def test_parses_decimal_candles_and_excludes_end_boundary(self) -> None:
        opener = QueueOpener([FakeResponse(payload([row(2), row(1), row(0)]))])
        client = BybitPublicKlineClient(opener=opener)

        evidence = client.fetch_candles(
            "btcusdt", 1, START, START + timedelta(minutes=2)
        )

        self.assertEqual(
            [item.open_time for item in evidence.candles],
            [START, START + timedelta(minutes=1)],
        )
        self.assertEqual(evidence.candles[0].symbol, "BTCUSDT")
        self.assertEqual(evidence.page_count, 1)
        self.assertEqual(evidence.raw_row_count, 3)
        self.assertEqual(len(evidence.dataset_sha256), 64)
        query = parse_qs(urlparse(opener.urls[0]).query)
        self.assertEqual(query["category"], ["linear"])
        self.assertNotIn("api_key", query)

    def test_paginates_backwards_without_duplicate_candles(self) -> None:
        opener = QueueOpener(
            [
                FakeResponse(payload([row(2), row(1)])),
                FakeResponse(payload([row(0)])),
            ]
        )
        client = BybitPublicKlineClient(opener=opener)
        client._PAGE_LIMIT = 2

        evidence = client.fetch_candles(
            "BTCUSDT", 1, START, START + timedelta(minutes=4)
        )

        self.assertEqual(len(evidence.candles), 3)
        self.assertEqual(evidence.page_count, 2)
        first_end = int(parse_qs(urlparse(opener.urls[0]).query)["end"][0])
        second_end = int(parse_qs(urlparse(opener.urls[1]).query)["end"][0])
        self.assertLess(second_end, first_end)

    def test_http_rate_limit_has_typed_error(self) -> None:
        def reject(*_: object, **__: object) -> object:
            raise HTTPError("https://api.bybit.com", 429, "rate", {}, io.BytesIO())

        client = BybitPublicKlineClient(opener=reject)

        with self.assertRaises(MarketDataError) as raised:
            client.fetch_candles("BTCUSDT", 1, START, START + timedelta(minutes=1))
        self.assertEqual(raised.exception.code, "rate_limited")

    def test_transport_error_has_typed_error(self) -> None:
        def reject(*_: object, **__: object) -> object:
            raise URLError("offline")

        client = BybitPublicKlineClient(opener=reject)

        with self.assertRaises(MarketDataError) as raised:
            client.fetch_candles("BTCUSDT", 1, START, START + timedelta(minutes=1))
        self.assertEqual(raised.exception.code, "transport_error")

    def test_api_error_has_typed_error(self) -> None:
        body = json.dumps({"retCode": 10006, "time": 1786752000000}).encode()
        client = BybitPublicKlineClient(opener=QueueOpener([FakeResponse(body)]))

        with self.assertRaises(MarketDataError) as raised:
            client.fetch_candles("BTCUSDT", 1, START, START + timedelta(minutes=1))
        self.assertEqual(raised.exception.code, "rate_limited")

    def test_malformed_payload_is_rejected(self) -> None:
        client = BybitPublicKlineClient(
            opener=QueueOpener([FakeResponse(payload([row(0)], "ETHUSDT"))])
        )

        with self.assertRaises(MarketDataError) as raised:
            client.fetch_candles("BTCUSDT", 1, START, START + timedelta(minutes=1))
        self.assertEqual(raised.exception.code, "malformed_response")

    def test_malformed_or_missing_symbol_is_rejected_before_request(self) -> None:
        opener = QueueOpener([])
        client = BybitPublicKlineClient(opener=opener)

        for symbol in ("", "BTC/USDT"):
            with self.subTest(symbol=symbol):
                with self.assertRaises(MarketDataError) as raised:
                    client.fetch_candles(
                        symbol,
                        1,
                        START,
                        START + timedelta(minutes=1),
                    )
                self.assertEqual(raised.exception.code, "invalid_symbol")
        self.assertEqual(opener.urls, [])


if __name__ == "__main__":
    unittest.main()
