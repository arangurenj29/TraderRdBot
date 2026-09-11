from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import hmac
import json
import time
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlparse
from urllib.request import Request, urlopen

from traderrd.domain.bridge import (
    DemoAccountOrder,
    DemoAccountPosition,
    DemoStrategyAccountSnapshot,
)
from traderrd.domain.execution import (
    DemoPreflightReport,
    FeeVerificationStatus,
    InstrumentRules,
)
from traderrd.domain.models import Direction


DEMO_BASE_URL = "https://api-demo.bybit.com"


class DemoExecutionError(RuntimeError):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


@dataclass(frozen=True, slots=True)
class BybitDemoCredentials:
    api_key: str = field(repr=False)
    api_secret: str = field(repr=False)

    def __post_init__(self) -> None:
        if not self.api_key or not self.api_secret:
            raise ValueError("Bybit Demo Trading API credentials are required")


@dataclass(frozen=True, slots=True)
class BybitDemoConfig:
    credentials: BybitDemoCredentials
    base_url: str = DEMO_BASE_URL
    recv_window_ms: int = 5000
    timeout_seconds: float = 5.0
    read_retries: int = 2

    def __post_init__(self) -> None:
        parsed = urlparse(self.base_url)
        if self.base_url != DEMO_BASE_URL or parsed.path not in ("", "/"):
            raise ValueError("Private execution is restricted to Bybit Demo Trading")
        if not 1000 <= self.recv_window_ms <= 10000:
            raise ValueError("Bybit receive window must be between 1000 and 10000 ms")
        if not 0 < self.timeout_seconds <= 30:
            raise ValueError("Bybit timeout must be positive and at most 30 seconds")
        if not 0 <= self.read_retries <= 3:
            raise ValueError("Bybit read retries must be between zero and three")


class BybitV5DemoClient:
    _MAX_RESPONSE_BYTES = 2_000_000
    _SUPPORTED_REQUESTS = {
        ("GET", "/v5/market/time"): "market_time_unavailable",
        ("GET", "/v5/market/instruments-info"): "instrument_info_unavailable",
        ("GET", "/v5/account/info"): "account_info_unavailable",
        ("GET", "/v5/account/wallet-balance"): "wallet_balance_unavailable",
        ("GET", "/v5/position/list"): "position_list_unavailable",
        ("GET", "/v5/order/realtime"): "order_realtime_unavailable",
        ("GET", "/v5/order/history"): "order_history_unavailable",
        ("GET", "/v5/execution/list"): "executions_unavailable",
        ("GET", "/v5/position/closed-pnl"): "closed_pnl_unavailable",
        ("POST", "/v5/order/create"): "order_create_unavailable",
        ("POST", "/v5/order/cancel"): "order_cancel_unavailable",
        ("POST", "/v5/position/trading-stop"): "trading_stop_unavailable",
    }

    def __init__(
        self,
        config: BybitDemoConfig,
        opener: Callable[..., Any] = urlopen,
        clock_ms: Callable[[], int] | None = None,
        sleeper: Callable[[float], None] = time.sleep,
    ) -> None:
        self.config = config
        self._opener = opener
        self._clock_ms = clock_ms or (lambda: int(time.time() * 1000))
        self._sleeper = sleeper

    @staticmethod
    def signature(
        api_secret: str,
        timestamp_ms: int,
        api_key: str,
        recv_window_ms: int,
        canonical_payload: str,
    ) -> str:
        message = (
            f"{timestamp_ms}{api_key}{recv_window_ms}{canonical_payload}"
        ).encode("utf-8")
        return hmac.new(
            api_secret.encode("utf-8"), message, hashlib.sha256
        ).hexdigest()

    def request(
        self,
        method: str,
        path: str,
        params: dict[str, str] | None = None,
        body: dict[str, Any] | None = None,
        private: bool = True,
    ) -> dict[str, Any]:
        normalized_method = method.upper()
        operation_code = self._SUPPORTED_REQUESTS.get((normalized_method, path))
        if operation_code is None:
            raise ValueError("Unsupported Bybit V5 request")
        query = urlencode(sorted((params or {}).items()))
        body_text = (
            json.dumps(body or {}, sort_keys=True, separators=(",", ":"))
            if normalized_method == "POST"
            else ""
        )
        canonical = query if normalized_method == "GET" else body_text
        attempts = self.config.read_retries + 1 if normalized_method == "GET" else 1
        last_error: DemoExecutionError | None = None
        for attempt in range(attempts):
            try:
                return self._send(
                    normalized_method,
                    path,
                    query,
                    body_text,
                    canonical,
                    private,
                    operation_code,
                )
            except DemoExecutionError as exc:
                last_error = exc
                if exc.code not in {"transport_error", "rate_limited"}:
                    raise
                if attempt + 1 >= attempts:
                    raise
                self._sleeper(float(Decimal("0.1") * (attempt + 1)))
        if last_error is None:
            raise RuntimeError("Bybit request did not execute")
        raise last_error

    def _send(
        self,
        method: str,
        path: str,
        query: str,
        body_text: str,
        canonical: str,
        private: bool,
        operation_code: str,
    ) -> dict[str, Any]:
        timestamp = self._clock_ms()
        url = f"{self.config.base_url}{path}"
        if query:
            url = f"{url}?{query}"
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "User-Agent": "TraderRd-Demo/0.1",
        }
        if private:
            headers.update(
                {
                    "X-BAPI-API-KEY": self.config.credentials.api_key,
                    "X-BAPI-TIMESTAMP": str(timestamp),
                    "X-BAPI-RECV-WINDOW": str(self.config.recv_window_ms),
                    "X-BAPI-SIGN": self.signature(
                        self.config.credentials.api_secret,
                        timestamp,
                        self.config.credentials.api_key,
                        self.config.recv_window_ms,
                        canonical,
                    ),
                }
            )
        request = Request(
            url,
            data=body_text.encode("utf-8") if method == "POST" else None,
            headers=headers,
            method=method,
        )
        try:
            with self._opener(
                request, timeout=self.config.timeout_seconds
            ) as response:
                status = getattr(response, "status", None) or response.getcode()
                if status == 429:
                    raise DemoExecutionError(
                        "rate_limited",
                        f"Demo Trading operation rate limited: {operation_code}",
                    )
                if status != 200:
                    raise DemoExecutionError(
                        "http_error",
                        f"Demo Trading operation failed: {operation_code}",
                    )
                raw = response.read(self._MAX_RESPONSE_BYTES + 1)
        except HTTPError as exc:
            code = "rate_limited" if exc.code == 429 else "http_error"
            raise DemoExecutionError(
                code,
                f"Demo Trading operation failed: {operation_code}",
            ) from exc
        except (URLError, TimeoutError, OSError) as exc:
            raise DemoExecutionError(
                "transport_error",
                f"Demo Trading transport unavailable: {operation_code}",
            ) from exc
        if len(raw) > self._MAX_RESPONSE_BYTES:
            raise DemoExecutionError(
                "response_too_large", "Demo Trading response too large"
            )
        try:
            payload = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise DemoExecutionError(
                "malformed_response", "Malformed Demo Trading response"
            ) from exc
        if not isinstance(payload, dict) or type(payload.get("retCode")) is not int:
            raise DemoExecutionError(
                "malformed_response", "Malformed Demo Trading response"
            )
        if payload["retCode"] != 0:
            if payload["retCode"] == 10006:
                raise DemoExecutionError(
                    "rate_limited",
                    f"Demo Trading operation rate limited: {operation_code}",
                )
            raise DemoExecutionError(
                operation_code,
                f"Demo Trading operation unavailable: {operation_code}",
            )
        result = payload.get("result")
        if not isinstance(result, dict):
            raise DemoExecutionError(
                "malformed_response", "Malformed Demo Trading response"
            )
        return payload


class BybitDemoPreflight:
    def __init__(
        self,
        client: BybitV5DemoClient,
        clock_ms: Callable[[], int] | None = None,
        max_time_offset_ms: int = 3000,
    ) -> None:
        self._client = client
        self._clock_ms = clock_ms or (lambda: int(time.time() * 1000))
        self._max_time_offset_ms = max_time_offset_ms

    def run(self, symbol: str) -> DemoPreflightReport:
        normalized = symbol.upper()
        server = self._client.request("GET", "/v5/market/time", private=False)
        server_time = _positive_int(server.get("time"), "server time")
        offset = server_time - self._clock_ms()
        if abs(offset) > self._max_time_offset_ms:
            raise DemoExecutionError(
                "time_unsynchronized", "System clock is not synchronized"
            )

        account = self._client.request("GET", "/v5/account/info")
        margin_mode = account["result"].get("marginMode")
        if margin_mode != "ISOLATED_MARGIN":
            raise DemoExecutionError(
                "margin_mode", "Account must use isolated margin"
            )

        wallet = self._client.request(
            "GET",
            "/v5/account/wallet-balance",
            {"accountType": "UNIFIED", "coin": "USDT"},
        )
        available = available_usdt_balance(wallet, margin_mode)
        if available <= 0:
            raise DemoExecutionError(
                "balance_unavailable", "No available Demo Trading balance"
            )

        instrument_payload = self._client.request(
            "GET",
            "/v5/market/instruments-info",
            {"category": "linear", "symbol": normalized},
            private=False,
        )
        instrument_row = _rows(instrument_payload)[0]
        price_filter = instrument_row.get("priceFilter")
        lot_filter = instrument_row.get("lotSizeFilter")
        if not isinstance(price_filter, dict) or not isinstance(lot_filter, dict):
            raise DemoExecutionError(
                "instrument_rules", "Instrument rules unavailable"
            )
        rules = InstrumentRules(
            symbol=normalized,
            tick_size=_decimal(price_filter, "tickSize"),
            quantity_step=_decimal(lot_filter, "qtyStep"),
            min_quantity=_decimal(lot_filter, "minOrderQty"),
            max_quantity=_decimal(lot_filter, "maxOrderQty"),
            min_notional=_decimal(lot_filter, "minNotionalValue"),
        )

        positions = self._client.request(
            "GET",
            "/v5/position/list",
            {"category": "linear", "symbol": normalized},
        )
        position_rows = _rows(positions)
        if not position_rows or any(
            row.get("positionIdx") != 0 for row in position_rows
        ):
            raise DemoExecutionError(
                "position_mode", "Account must use one-way mode"
            )
        return DemoPreflightReport(
            symbol=normalized,
            isolated_margin=True,
            one_way_mode=True,
            available_balance=available,
            fee_verification=FeeVerificationStatus.UNAVAILABLE,
            instrument=rules,
            server_time_offset_ms=offset,
            checked_at=datetime.fromtimestamp(self._clock_ms() / 1000, tz=timezone.utc),
        )


class BybitDemoAccountSnapshotProvider:
    """Read a complete, short-lived Demo strategy-account snapshot."""

    def __init__(
        self,
        client: BybitV5DemoClient,
        clock_ms: Callable[[], int] | None = None,
        max_time_offset_ms: int = 3000,
        max_payload_age_ms: int = 10_000,
    ) -> None:
        self._client = client
        self._clock_ms = clock_ms or (lambda: int(time.time() * 1000))
        self._max_time_offset_ms = max_time_offset_ms
        self._max_payload_age_ms = max_payload_age_ms

    def fetch(
        self, symbol: str
    ) -> tuple[DemoStrategyAccountSnapshot, InstrumentRules]:
        normalized = symbol.upper()
        server = self._client.request("GET", "/v5/market/time", private=False)
        server_time = _positive_int(server.get("time"), "server time")
        if abs(server_time - self._clock_ms()) > self._max_time_offset_ms:
            raise DemoExecutionError(
                "time_unsynchronized", "System clock is not synchronized"
            )

        # Demo account-info responses do not include the top-level `time`
        # field. Freshness is still enforced by the market-time response and
        # every timestamped account snapshot endpoint below.
        account = self._fresh_request(
            "GET", "/v5/account/info", require_time=False
        )
        if account["result"].get("marginMode") != "ISOLATED_MARGIN":
            raise DemoExecutionError(
                "margin_mode", "Account must use isolated margin"
            )
        wallet = self._fresh_request(
            "GET",
            "/v5/account/wallet-balance",
            {"accountType": "UNIFIED", "coin": "USDT"},
        )
        equity = mark_to_market_usdt_equity(wallet)
        wallet_metrics = _usdt_wallet_metrics(wallet)
        positions_payload = self._fresh_paginated_request(
            "GET",
            "/v5/position/list",
            {"category": "linear", "settleCoin": "USDT", "limit": "200"},
        )
        orders_payload = self._fresh_paginated_request(
            "GET",
            "/v5/order/realtime",
            {
                "category": "linear",
                "settleCoin": "USDT",
                "openOnly": "0",
                "limit": "50",
            },
        )
        instrument_payload = self._fresh_request(
            "GET",
            "/v5/market/instruments-info",
            {"category": "linear", "symbol": normalized},
            private=False,
        )
        instrument_row = _rows(instrument_payload)[0]
        price_filter = instrument_row.get("priceFilter")
        lot_filter = instrument_row.get("lotSizeFilter")
        if not isinstance(price_filter, dict) or not isinstance(lot_filter, dict):
            raise DemoExecutionError(
                "instrument_rules", "Instrument rules unavailable"
            )
        rules = InstrumentRules(
            symbol=normalized,
            tick_size=_decimal(price_filter, "tickSize"),
            quantity_step=_decimal(lot_filter, "qtyStep"),
            min_quantity=_decimal(lot_filter, "minOrderQty"),
            max_quantity=_decimal(lot_filter, "maxOrderQty"),
            min_notional=_decimal(lot_filter, "minNotionalValue"),
        )
        position_rows = _optional_rows(positions_payload)
        if any(row.get("positionIdx") != 0 for row in position_rows):
            raise DemoExecutionError(
                "position_mode", "Account must use one-way mode"
            )
        positions = tuple(
            self._position(row)
            for row in position_rows
            if _decimal(row, "size", allow_zero=True) > 0
        )
        orders = tuple(self._order(row) for row in _optional_rows(orders_payload))
        captured_at = datetime.fromtimestamp(server_time / 1000, tz=timezone.utc)
        return (
            DemoStrategyAccountSnapshot(
                equity=equity,
                captured_at=captured_at,
                positions=positions,
                orders=orders,
                wallet_balance=wallet_metrics["wallet_balance"],
                unrealised_pnl=wallet_metrics["unrealised_pnl"],
                available_balance=wallet_metrics["available_balance"],
                position_initial_margin=wallet_metrics["position_initial_margin"],
                order_initial_margin=wallet_metrics["order_initial_margin"],
            ),
            rules,
        )

    def _fresh_request(
        self,
        method: str,
        path: str,
        params: dict[str, str] | None = None,
        private: bool = True,
        require_time: bool = True,
    ) -> dict[str, Any]:
        payload = self._client.request(method, path, params, private=private)
        if not require_time:
            return payload
        payload_time = _positive_int(payload.get("time"), "response time")
        age = self._clock_ms() - payload_time
        if age < -self._max_time_offset_ms or age > self._max_payload_age_ms:
            raise DemoExecutionError(
                "account_snapshot_stale", "Demo account snapshot is stale"
            )
        return payload

    def _fresh_paginated_request(
        self,
        method: str,
        path: str,
        params: dict[str, str],
        max_pages: int = 5,
    ) -> dict[str, Any]:
        """Read a bounded account collection without accepting partial data."""
        collected: list[dict[str, Any]] = []
        current = dict(params)
        seen_cursors: set[str] = set()
        for _ in range(max_pages):
            payload = self._fresh_request(method, path, current)
            result = payload.get("result")
            if not isinstance(result, dict):
                raise DemoExecutionError(
                    "malformed_response", "Malformed Demo account collection"
                )
            rows = result.get("list")
            if not isinstance(rows, list) or not all(
                isinstance(row, dict) for row in rows
            ):
                raise DemoExecutionError(
                    "malformed_response", "Malformed Demo account collection"
                )
            collected.extend(rows)
            cursor = result.get("nextPageCursor")
            if cursor in {None, ""}:
                merged = dict(payload)
                merged_result = dict(result)
                merged_result["list"] = collected
                merged_result["nextPageCursor"] = ""
                merged["result"] = merged_result
                return merged
            if not isinstance(cursor, str) or cursor in seen_cursors:
                break
            seen_cursors.add(cursor)
            current["cursor"] = cursor
        raise DemoExecutionError(
            "account_snapshot_incomplete",
            "Demo account snapshot exceeds the supported safety bound",
        )

    @staticmethod
    def _position(row: dict[str, Any]) -> DemoAccountPosition:
        if row.get("positionIdx") != 0:
            raise DemoExecutionError(
                "position_mode", "Account must use one-way mode"
            )
        symbol = row.get("symbol")
        side = row.get("side")
        if not isinstance(symbol, str) or symbol != symbol.upper():
            raise DemoExecutionError(
                "malformed_response", "Malformed Demo position metadata"
            )
        if side not in {"Buy", "Sell"}:
            raise DemoExecutionError(
                "malformed_response", "Malformed Demo position metadata"
            )
        return DemoAccountPosition(
            symbol=symbol,
            direction=Direction.LONG if side == "Buy" else Direction.SHORT,
            quantity=_decimal(row, "size"),
            average_price=_optional_metric(row, "avgPrice"),
            mark_price=_optional_metric(row, "markPrice"),
            liquidation_price=_optional_metric(row, "liqPrice", zero_is_none=True),
            unrealised_pnl=_optional_metric(row, "unrealisedPnl", signed=True),
            leverage=_optional_metric(row, "leverage"),
            position_margin=_optional_metric(row, "positionIM", zero_is_none=True),
            take_profit=_optional_metric(row, "takeProfit", zero_is_none=True),
            stop_loss=_optional_metric(row, "stopLoss", zero_is_none=True),
        )

    @staticmethod
    def _order(row: dict[str, Any]) -> DemoAccountOrder:
        symbol = row.get("symbol")
        order_link_id = row.get("orderLinkId")
        if not isinstance(symbol, str) or symbol != symbol.upper():
            raise DemoExecutionError(
                "malformed_response", "Malformed Demo order metadata"
            )
        if isinstance(order_link_id, str) and order_link_id:
            return DemoAccountOrder(symbol=symbol, order_link_id=order_link_id)
        if _is_exchange_protective_child(row):
            return DemoAccountOrder(
                symbol=symbol,
                order_link_id=None,
                exchange_protective_child=True,
            )
        raise DemoExecutionError(
            "malformed_response", "Malformed Demo order metadata"
        )


def _is_exchange_protective_child(row: dict[str, Any]) -> bool:
    """Recognize only the unlinked TP/SL children created by Bybit itself.

    An empty ``orderLinkId`` alone is never trusted: the child must be
    reduce-only, close-on-trigger, and consistently identify itself as either
    a take-profit or stop-loss order in both Bybit metadata fields.
    """
    stop_order_type = row.get("stopOrderType")
    create_type = row.get("createType")
    return (
        row.get("reduceOnly") is True
        and row.get("closeOnTrigger") is True
        and (stop_order_type, create_type)
        in {
            ("TakeProfit", "CreateByTakeProfit"),
            ("StopLoss", "CreateByStopLoss"),
        }
    )

def available_usdt_balance(
    wallet_payload: dict[str, Any],
    margin_mode: str,
) -> Decimal:
    account = _rows(wallet_payload)[0]
    if margin_mode != "ISOLATED_MARGIN":
        return _decimal(account, "totalAvailableBalance", allow_zero=True)

    coins = account.get("coin")
    if not isinstance(coins, list) or not all(
        isinstance(coin, dict) for coin in coins
    ):
        raise DemoExecutionError(
            "malformed_response", "Malformed Demo Trading coin balances"
        )
    usdt_rows = [coin for coin in coins if coin.get("coin") == "USDT"]
    if len(usdt_rows) != 1:
        raise DemoExecutionError(
            "balance_unavailable", "USDT Demo Trading balance is unavailable"
        )
    usdt = usdt_rows[0]
    available = (
        _decimal(usdt, "walletBalance", allow_zero=True)
        - _decimal(usdt, "totalPositionIM", allow_zero=True)
        - _decimal(usdt, "totalOrderIM", allow_zero=True)
        - _decimal(usdt, "locked", allow_zero=True)
        - _decimal(usdt, "bonus", allow_zero=True)
    )
    if available < 0:
        raise DemoExecutionError(
            "balance_unavailable", "Available Demo Trading balance is negative"
        )
    return available


def mark_to_market_usdt_equity(wallet_payload: dict[str, Any]) -> Decimal:
    account = _rows(wallet_payload)[0]
    coins = account.get("coin")
    if not isinstance(coins, list) or not all(
        isinstance(coin, dict) for coin in coins
    ):
        raise DemoExecutionError(
            "malformed_response", "Malformed Demo Trading coin balances"
        )
    usdt_rows = [coin for coin in coins if coin.get("coin") == "USDT"]
    if len(usdt_rows) != 1:
        raise DemoExecutionError(
            "balance_unavailable", "USDT Demo Trading balance is unavailable"
        )
    usdt = usdt_rows[0]
    wallet_balance = _decimal(usdt, "walletBalance", allow_zero=True)
    unrealized_pnl = _signed_decimal(usdt, "unrealisedPnl")
    reported_equity = _decimal(usdt, "equity", allow_zero=True)
    derived_equity = wallet_balance + unrealized_pnl
    if derived_equity <= 0 or abs(derived_equity - reported_equity) > Decimal("1e-8"):
        raise DemoExecutionError(
            "equity_unavailable", "Mark-to-market Demo equity is unavailable"
        )
    return derived_equity


def _usdt_wallet_metrics(wallet_payload: dict[str, Any]) -> dict[str, Decimal | None]:
    account = _rows(wallet_payload)[0]
    coins = account.get("coin")
    if not isinstance(coins, list):
        raise DemoExecutionError("malformed_response", "Malformed Demo Trading coin balances")
    rows = [coin for coin in coins if isinstance(coin, dict) and coin.get("coin") == "USDT"]
    if len(rows) != 1:
        raise DemoExecutionError("balance_unavailable", "USDT Demo Trading balance is unavailable")
    row = rows[0]
    wallet_balance = _decimal(row, "walletBalance", allow_zero=True)
    unrealised_pnl = _signed_decimal(row, "unrealisedPnl")
    margin_fields = ("totalPositionIM", "totalOrderIM", "locked", "bonus")
    if all(key in row for key in margin_fields):
        position_margin = _decimal(row, "totalPositionIM", allow_zero=True)
        order_margin = _decimal(row, "totalOrderIM", allow_zero=True)
        locked = _decimal(row, "locked", allow_zero=True)
        bonus = _decimal(row, "bonus", allow_zero=True)
        available = wallet_balance - position_margin - order_margin - locked - bonus
        if available < 0:
            raise DemoExecutionError("balance_unavailable", "Available Demo Trading balance is negative")
    else:
        position_margin = order_margin = available = None
    return {
        "wallet_balance": wallet_balance,
        "unrealised_pnl": unrealised_pnl,
        "available_balance": available,
        "position_initial_margin": position_margin,
        "order_initial_margin": order_margin,
    }


def _optional_metric(
    payload: dict[str, Any], key: str, *, signed: bool = False, zero_is_none: bool = False
) -> Decimal | None:
    raw = payload.get(key)
    if raw is None or raw == "":
        return None
    try:
        value = Decimal(str(raw))
    except (InvalidOperation, TypeError, ValueError):
        return None
    if not value.is_finite() or (not signed and value < 0):
        return None
    if zero_is_none and value == 0:
        return None
    return value


def _rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
    rows = payload["result"].get("list")
    if (
        not isinstance(rows, list)
        or not rows
        or not all(isinstance(row, dict) for row in rows)
    ):
        raise DemoExecutionError(
            "malformed_response", "Expected Demo Trading rows are missing"
        )
    return rows


def _optional_rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
    rows = payload["result"].get("list")
    if not isinstance(rows, list) or not all(isinstance(row, dict) for row in rows):
        raise DemoExecutionError(
            "malformed_response", "Expected Demo Trading rows are malformed"
        )
    return rows


def _decimal(
    payload: dict[str, Any], key: str, allow_zero: bool = False
) -> Decimal:
    try:
        value = Decimal(str(payload[key]))
    except (KeyError, InvalidOperation) as exc:
        raise DemoExecutionError(
            "malformed_response", "Invalid Demo Trading decimal"
        ) from exc
    if not value.is_finite() or value < 0 or (value == 0 and not allow_zero):
        raise DemoExecutionError("malformed_response", "Invalid Demo Trading decimal")
    return value


def _signed_decimal(payload: dict[str, Any], key: str) -> Decimal:
    try:
        value = Decimal(str(payload[key]))
    except (KeyError, InvalidOperation) as exc:
        raise DemoExecutionError(
            "malformed_response", "Invalid Demo Trading decimal"
        ) from exc
    if not value.is_finite():
        raise DemoExecutionError("malformed_response", "Invalid Demo Trading decimal")
    return value


def _positive_int(value: Any, label: str) -> int:
    if type(value) is not int or value <= 0:
        raise DemoExecutionError("malformed_response", f"Invalid {label}")
    return value
