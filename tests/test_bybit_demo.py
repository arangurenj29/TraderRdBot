from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import hmac
import json
from urllib.error import URLError
import unittest

from traderrd.infrastructure.bybit_demo import (
    BybitDemoConfig,
    BybitDemoCredentials,
    BybitDemoAccountSnapshotProvider,
    BybitDemoPreflight,
    BybitV5DemoClient,
    DemoExecutionError,
    available_usdt_balance,
    mark_to_market_usdt_equity,
)


class FakeResponse:
    def __init__(self, payload: dict[str, object]) -> None:
        self.body = json.dumps(payload).encode()
        self.status = 200

    def __enter__(self) -> "FakeResponse":
        return self

    def __exit__(self, *_: object) -> None:
        return None

    def read(self, _: int) -> bytes:
        return self.body

    def getcode(self) -> int:
        return self.status


class CapturingOpener:
    def __init__(self, outcomes: list[object]) -> None:
        self.outcomes = outcomes
        self.requests: list[object] = []

    def __call__(self, request: object, timeout: float) -> FakeResponse:
        self.requests.append(request)
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome  # type: ignore[return-value]


def success(result: dict[str, object] | None = None) -> FakeResponse:
    return FakeResponse({"retCode": 0, "result": result or {}, "time": 1000000})


class BybitDemoSigningTests(unittest.TestCase):
    def config(self) -> BybitDemoConfig:
        return BybitDemoConfig(BybitDemoCredentials("test-key", "test-secret"))

    def test_rejects_mainnet_and_unknown_private_endpoints(self) -> None:
        credentials = BybitDemoCredentials("key", "secret")
        for url in (
            "https://api.bybit.com",
            "https://api-testnet.bybit.com",
            "https://example.com",
        ):
            with self.subTest(url=url), self.assertRaises(ValueError):
                BybitDemoConfig(credentials, base_url=url)

    def test_uses_exact_demo_trading_rest_origin(self) -> None:
        self.assertEqual(self.config().base_url, "https://api-demo.bybit.com")

    def test_client_allowlist_excludes_withdrawal_endpoints(self) -> None:
        client = BybitV5DemoClient(self.config(), opener=CapturingOpener([]))

        with self.assertRaises(ValueError):
            client.request(
                "POST",
                "/v5/asset/withdraw/create",
                body={"coin": "USDT"},
            )

    def test_demo_client_does_not_support_fee_rate_endpoint(self) -> None:
        opener = CapturingOpener([])
        client = BybitV5DemoClient(self.config(), opener=opener)

        with self.assertRaises(ValueError):
            client.request(
                "GET",
                "/v5/account/fee-rate",
                {"category": "linear", "symbol": "BTCUSDT"},
            )

        self.assertEqual(opener.requests, [])

    def test_credential_repr_never_contains_key_values(self) -> None:
        credentials = BybitDemoCredentials("visible-key", "visible-secret")

        rendered = repr(credentials)

        self.assertNotIn("visible-key", rendered)
        self.assertNotIn("visible-secret", rendered)

    def test_canonical_get_signature_matches_transmitted_query(self) -> None:
        opener = CapturingOpener([success()])
        client = BybitV5DemoClient(
            self.config(), opener=opener, clock_ms=lambda: 1700000000000
        )

        client.request(
            "GET",
            "/v5/order/realtime",
            {"symbol": "BTCUSDT", "category": "linear"},
        )

        request = opener.requests[0]
        canonical = "category=linear&symbol=BTCUSDT"
        message = f"1700000000000test-key5000{canonical}".encode()
        expected = hmac.new(b"test-secret", message, hashlib.sha256).hexdigest()
        self.assertTrue(
            request.full_url.endswith(f"?{canonical}")  # type: ignore[attr-defined]
        )
        self.assertEqual(
            request.headers["X-bapi-sign"],  # type: ignore[attr-defined]
            expected,
        )

    def test_post_signs_exact_canonical_body_and_does_not_retry(self) -> None:
        opener = CapturingOpener([URLError("offline"), success()])
        sleeps: list[float] = []
        client = BybitV5DemoClient(
            self.config(),
            opener=opener,
            clock_ms=lambda: 1700000000000,
            sleeper=sleeps.append,
        )

        with self.assertRaises(DemoExecutionError) as raised:
            client.request(
                "POST",
                "/v5/order/create",
                body={"symbol": "BTCUSDT", "category": "linear"},
            )

        self.assertEqual(raised.exception.code, "transport_error")
        self.assertEqual(len(opener.requests), 1)
        self.assertEqual(sleeps, [])
        request = opener.requests[0]
        canonical = '{"category":"linear","symbol":"BTCUSDT"}'
        message = f"1700000000000test-key5000{canonical}".encode()
        expected = hmac.new(b"test-secret", message, hashlib.sha256).hexdigest()
        self.assertEqual(request.data.decode(), canonical)  # type: ignore[attr-defined]
        self.assertEqual(
            request.headers["X-bapi-sign"],  # type: ignore[attr-defined]
            expected,
        )

    def test_read_transport_failure_retries_with_bound(self) -> None:
        opener = CapturingOpener([URLError("offline"), success()])
        sleeps: list[float] = []
        client = BybitV5DemoClient(
            self.config(), opener=opener, sleeper=sleeps.append
        )

        payload = client.request("GET", "/v5/account/info")

        self.assertEqual(payload["retCode"], 0)
        self.assertEqual(len(opener.requests), 2)
        self.assertEqual(sleeps, [0.1])

    def test_api_rejection_reports_only_opaque_operation_code(self) -> None:
        response = FakeResponse(
            {
                "retCode": 10001,
                "retMsg": "raw-private-marker",
                "result": {},
            }
        )
        client = BybitV5DemoClient(
            self.config(),
            opener=CapturingOpener([response]),
        )

        with self.assertRaises(DemoExecutionError) as raised:
            client.request(
                "GET",
                "/v5/account/wallet-balance",
                {"accountType": "UNIFIED", "coin": "USDT"},
            )

        self.assertEqual(raised.exception.code, "wallet_balance_unavailable")
        rendered = str(raised.exception)
        self.assertIn("wallet_balance_unavailable", rendered)
        self.assertNotIn("raw-private-marker", rendered)
        self.assertNotIn("test-key", rendered)
        self.assertNotIn("test-secret", rendered)
        self.assertNotIn("/v5/", rendered)


class FakePreflightClient:
    def __init__(
        self,
        margin_mode: str = "ISOLATED_MARGIN",
        position_idx: int = 0,
    ) -> None:
        self.margin_mode = margin_mode
        self.position_idx = position_idx
        self.calls: list[tuple[str, str, bool]] = []

    def request(
        self,
        method: str,
        path: str,
        params: dict[str, str] | None = None,
        body: dict[str, object] | None = None,
        private: bool = True,
    ) -> dict[str, object]:
        self.calls.append((method, path, private))
        if path == "/v5/market/time":
            return {"retCode": 0, "result": {}, "time": 1000000}
        if path == "/v5/account/info":
            return {"retCode": 0, "result": {"marginMode": self.margin_mode}}
        if path == "/v5/account/wallet-balance":
            return {
                "retCode": 0,
                "result": {
                    "list": [
                        {
                            "totalAvailableBalance": "",
                            "coin": [
                                {
                                    "coin": "USDT",
                                    "walletBalance": "1000",
                                    "totalPositionIM": "100",
                                    "totalOrderIM": "20",
                                    "locked": "5",
                                    "bonus": "25",
                                }
                            ],
                        }
                    ]
                },
            }
        if path == "/v5/market/instruments-info":
            return {
                "retCode": 0,
                "result": {
                    "list": [
                        {
                            "priceFilter": {"tickSize": "0.1"},
                            "lotSizeFilter": {
                                "qtyStep": "0.001",
                                "minOrderQty": "0.001",
                                "maxOrderQty": "100",
                                "minNotionalValue": "5",
                            },
                        }
                    ]
                },
            }
        if path == "/v5/position/list":
            return {
                "retCode": 0,
                "result": {
                    "list": [{"positionIdx": self.position_idx, "size": "0"}]
                },
            }
        raise AssertionError(path)


class BybitDemoPreflightTests(unittest.TestCase):
    def test_preflight_verifies_all_read_only_safety_metadata(self) -> None:
        client = FakePreflightClient()
        preflight = BybitDemoPreflight(
            client,  # type: ignore[arg-type]
            clock_ms=lambda: 1000000,
        )

        report = preflight.run("btcusdt")

        self.assertTrue(report.isolated_margin)
        self.assertTrue(report.one_way_mode)
        self.assertEqual(str(report.available_balance), "850")
        self.assertEqual(report.fee_verification.value, "unavailable")
        self.assertEqual(str(report.instrument.quantity_step), "0.001")
        self.assertTrue(all(method == "GET" for method, _, _ in client.calls))
        self.assertNotIn(
            "/v5/account/fee-rate",
            {path for _, path, _ in client.calls},
        )

    def test_preflight_fails_closed_on_cross_margin(self) -> None:
        preflight = BybitDemoPreflight(
            FakePreflightClient("REGULAR_MARGIN"),  # type: ignore[arg-type]
            clock_ms=lambda: 1000000,
        )

        with self.assertRaises(DemoExecutionError) as raised:
            preflight.run("BTCUSDT")
        self.assertEqual(raised.exception.code, "margin_mode")

    def test_preflight_fails_closed_on_clock_drift(self) -> None:
        preflight = BybitDemoPreflight(
            FakePreflightClient(),  # type: ignore[arg-type]
            clock_ms=lambda: 990000,
        )

        with self.assertRaises(DemoExecutionError) as raised:
            preflight.run("BTCUSDT")
        self.assertEqual(raised.exception.code, "time_unsynchronized")

    def test_preflight_fails_closed_on_hedge_position_mode(self) -> None:
        preflight = BybitDemoPreflight(
            FakePreflightClient(position_idx=1),  # type: ignore[arg-type]
            clock_ms=lambda: 1000000,
        )

        with self.assertRaises(DemoExecutionError) as raised:
            preflight.run("BTCUSDT")
        self.assertEqual(raised.exception.code, "position_mode")


class DemoAvailableBalanceTests(unittest.TestCase):
    def test_isolated_margin_derives_available_usdt_from_coin_fields(self) -> None:
        wallet = {
            "result": {
                "list": [
                    {
                        "totalAvailableBalance": "",
                        "coin": [
                            {
                                "coin": "USDT",
                                "walletBalance": "200",
                                "totalPositionIM": "40.25",
                                "totalOrderIM": "10.5",
                                "locked": "4",
                                "bonus": "5.25",
                            }
                        ],
                    }
                ]
            }
        }

        available = available_usdt_balance(wallet, "ISOLATED_MARGIN")

        self.assertEqual(available, Decimal("140"))

    def test_cross_margin_uses_account_wide_available_balance(self) -> None:
        wallet = {
            "result": {
                "list": [
                    {
                        "totalAvailableBalance": "321.125",
                        "coin": [],
                    }
                ]
            }
        }

        available = available_usdt_balance(wallet, "REGULAR_MARGIN")

        self.assertEqual(available, Decimal("321.125"))

    def test_isolated_margin_rejects_missing_usdt_without_raw_payload(self) -> None:
        wallet = {
            "result": {
                "list": [
                    {
                        "coin": [
                            {
                                "coin": "BTC",
                                "walletBalance": "raw-secret-marker",
                            }
                        ]
                    }
                ]
            }
        }

        with self.assertRaises(DemoExecutionError) as raised:
            available_usdt_balance(wallet, "ISOLATED_MARGIN")

        self.assertEqual(raised.exception.code, "balance_unavailable")


class FakeSnapshotClient:
    def __init__(
        self,
        wallet_balance: str = "1000",
        upl: str = "25",
        paginated_orders: bool = False,
    ) -> None:
        self.wallet_balance = wallet_balance
        self.upl = upl
        self.paginated_orders = paginated_orders
        self.calls: list[tuple[str, str, bool]] = []

    def request(
        self,
        method: str,
        path: str,
        params: dict[str, str] | None = None,
        body: dict[str, object] | None = None,
        private: bool = True,
    ) -> dict[str, object]:
        self.calls.append((method, path, private))
        result: dict[str, object]
        if path == "/v5/market/time":
            result = {}
        elif path == "/v5/account/info":
            result = {"marginMode": "ISOLATED_MARGIN"}
        elif path == "/v5/account/wallet-balance":
            result = {
                "list": [
                    {
                        "coin": [
                            {
                                "coin": "USDT",
                                "walletBalance": self.wallet_balance,
                                "unrealisedPnl": self.upl,
                                "equity": str(
                                    Decimal(self.wallet_balance) + Decimal(self.upl)
                                ),
                            }
                        ]
                    }
                ]
            }
        elif path == "/v5/position/list":
            result = {"list": []}
        elif path == "/v5/order/realtime":
            result = {
                "list": [],
                "nextPageCursor": "more" if self.paginated_orders else "",
            }
        elif path == "/v5/market/instruments-info":
            result = {
                "list": [
                    {
                        "priceFilter": {"tickSize": "0.1"},
                        "lotSizeFilter": {
                            "qtyStep": "0.001",
                            "minOrderQty": "0.001",
                            "maxOrderQty": "1000",
                            "minNotionalValue": "5",
                        },
                    }
                ]
            }
        else:
            raise AssertionError(path)
        return {"retCode": 0, "result": result, "time": 1000000}


class DemoAccountSnapshotTests(unittest.TestCase):
    def test_accepts_unlinked_bybit_take_profit_child_with_exact_metadata(self) -> None:
        class ProtectiveChildClient(FakeSnapshotClient):
            def request(self, method, path, params=None, body=None, private=True):
                payload = super().request(method, path, params, body, private)
                if path == "/v5/order/realtime":
                    payload["result"]["list"] = [
                        {
                            "symbol": "UNIUSDT",
                            "orderLinkId": "",
                            "reduceOnly": True,
                            "closeOnTrigger": True,
                            "stopOrderType": "TakeProfit",
                            "createType": "CreateByTakeProfit",
                        }
                    ]
                return payload

        provider = BybitDemoAccountSnapshotProvider(
            ProtectiveChildClient(),  # type: ignore[arg-type]
            clock_ms=lambda: 1000000,
        )

        snapshot, _ = provider.fetch("UNIUSDT")

        self.assertEqual(len(snapshot.orders), 1)
        self.assertIsNone(snapshot.orders[0].order_link_id)
        self.assertTrue(snapshot.orders[0].exchange_protective_child)

    def test_rejects_unlinked_order_without_exact_bybit_protection_metadata(self) -> None:
        class UnlinkedOrderClient(FakeSnapshotClient):
            def request(self, method, path, params=None, body=None, private=True):
                payload = super().request(method, path, params, body, private)
                if path == "/v5/order/realtime":
                    payload["result"]["list"] = [
                        {
                            "symbol": "BNBUSDT",
                            "orderLinkId": "",
                            "reduceOnly": True,
                            "closeOnTrigger": True,
                            "stopOrderType": "TakeProfit",
                            "createType": "CreateByUser",
                        }
                    ]
                return payload

        provider = BybitDemoAccountSnapshotProvider(
            UnlinkedOrderClient(),  # type: ignore[arg-type]
            clock_ms=lambda: 1000000,
        )

        with self.assertRaises(DemoExecutionError) as raised:
            provider.fetch("BNBUSDT")

        self.assertEqual(raised.exception.code, "malformed_response")

    def test_paginates_account_orders_with_bounded_cursor(self) -> None:
        class OneCursorClient(FakeSnapshotClient):
            def __init__(self) -> None:
                super().__init__()
                self.order_page = 0

            def request(self, method, path, params=None, body=None, private=True):
                payload = super().request(method, path, params, body, private)
                if path == "/v5/order/realtime":
                    self.order_page += 1
                    if self.order_page == 1:
                        payload["result"]["list"] = [
                            {
                                "orderLinkId": "trd-demo-e-1",
                                "symbol": "BTCUSDT",
                            }
                        ]
                        payload["result"]["nextPageCursor"] = "next"
                    else:
                        payload["result"]["list"] = []
                        payload["result"]["nextPageCursor"] = ""
                return payload

        provider = BybitDemoAccountSnapshotProvider(
            OneCursorClient(),  # type: ignore[arg-type]
            clock_ms=lambda: 1000000,
        )

        snapshot, _ = provider.fetch("BTCUSDT")

        self.assertEqual(len(snapshot.orders), 1)

    def test_accepts_demo_account_info_without_response_time(self) -> None:
        class AccountInfoWithoutTimeClient(FakeSnapshotClient):
            def request(self, method, path, params=None, body=None, private=True):
                payload = super().request(method, path, params, body, private)
                if path == "/v5/account/info":
                    payload.pop("time", None)
                return payload

        provider = BybitDemoAccountSnapshotProvider(
            AccountInfoWithoutTimeClient(),  # type: ignore[arg-type]
            clock_ms=lambda: 1000000,
        )

        snapshot, _ = provider.fetch("BTCUSDT")

        self.assertEqual(snapshot.equity, Decimal("1025"))

    def test_uses_wallet_plus_unrealized_pnl_as_mark_to_market_equity(self) -> None:
        client = FakeSnapshotClient("1000", "-25.5")
        provider = BybitDemoAccountSnapshotProvider(
            client,  # type: ignore[arg-type]
            clock_ms=lambda: 1000000,
        )

        snapshot, rules = provider.fetch("btcusdt")

        self.assertEqual(snapshot.equity, Decimal("974.5"))
        self.assertEqual(rules.symbol, "BTCUSDT")
        self.assertEqual(snapshot.positions, ())
        self.assertEqual(snapshot.orders, ())
        self.assertTrue(all(method == "GET" for method, _, _ in client.calls))

    def test_rejects_inconsistent_reported_equity_without_payload_details(self) -> None:
        wallet = {
            "result": {
                "list": [
                    {
                        "coin": [
                            {
                                "coin": "USDT",
                                "walletBalance": "1000",
                                "unrealisedPnl": "50",
                                "equity": "999",
                                "private_marker": "do-not-report",
                            }
                        ]
                    }
                ]
            }
        }

        with self.assertRaises(DemoExecutionError) as raised:
            mark_to_market_usdt_equity(wallet)

        self.assertEqual(raised.exception.code, "equity_unavailable")
        self.assertNotIn("do-not-report", str(raised.exception))
        self.assertNotIn("raw-secret-marker", str(raised.exception))

    def test_rejects_partial_paginated_account_snapshot(self) -> None:
        provider = BybitDemoAccountSnapshotProvider(
            FakeSnapshotClient(paginated_orders=True),  # type: ignore[arg-type]
            clock_ms=lambda: 1000000,
        )

        with self.assertRaises(DemoExecutionError) as raised:
            provider.fetch("BTCUSDT")

        self.assertEqual(raised.exception.code, "account_snapshot_incomplete")

    def test_isolated_margin_rejects_negative_derived_balance(self) -> None:
        wallet = {
            "result": {
                "list": [
                    {
                        "coin": [
                            {
                                "coin": "USDT",
                                "walletBalance": "10",
                                "totalPositionIM": "7",
                                "totalOrderIM": "4",
                                "locked": "0",
                                "bonus": "0",
                            }
                        ]
                    }
                ]
            }
        }

        with self.assertRaises(DemoExecutionError) as raised:
            available_usdt_balance(wallet, "ISOLATED_MARGIN")

        self.assertEqual(raised.exception.code, "balance_unavailable")

    def test_isolated_margin_rejects_missing_or_malformed_fields(self) -> None:
        wallet = {
            "result": {
                "list": [
                    {
                        "coin": [
                            {
                                "coin": "USDT",
                                "walletBalance": "not-a-decimal",
                                "totalPositionIM": "0",
                                "totalOrderIM": "0",
                                "locked": "0",
                            }
                        ]
                    }
                ]
            }
        }

        with self.assertRaises(DemoExecutionError) as raised:
            available_usdt_balance(wallet, "ISOLATED_MARGIN")

        self.assertEqual(raised.exception.code, "malformed_response")
        self.assertNotIn("not-a-decimal", str(raised.exception))


if __name__ == "__main__":
    unittest.main()
