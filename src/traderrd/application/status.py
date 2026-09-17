from __future__ import annotations

from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from decimal import Decimal, InvalidOperation
import json
from pathlib import Path
import re
import sqlite3
from typing import Any, Callable, Iterable

from traderrd.domain.bridge import VALIDATED_SOURCE_CHAT_ID, VALIDATED_SOURCE_TOPIC_ID


STATUS_SCHEMA_VERSION = "1"
VALIDATED_SOURCE_SEED_ID = 231906
VALIDATED_SOURCE_SENDER_ID = 8003985182
HEARTBEAT_COMPONENTS = ("observer", "worker", "monitor")
ACTIVATION_COMMANDS = {
    "observer": "./.venv/bin/traderrd demo-run",
    "worker": "./.venv/bin/traderrd demo-run",
    "monitor": "./.venv/bin/traderrd demo-run",
}
_SAFE_TOKEN = re.compile(r"[^A-Za-z0-9_.:-]+")
_KNOWN_INTENT_STATES = (
    "planned",
    "acknowledged",
    "working",
    "filled",
    "cancelled",
    "rejected",
    "close_confirmed",
    "protection_verified",
    "position_closed_pending",
    "position_closed",
    "reconciliation_required",
)
_KNOWN_RESERVATION_STATES = (
    "pending",
    "filled",
    "close_requested",
    "cancelled",
    "expired",
    "closed",
)
_ACTIVE_RESERVATION_STATES = ("pending", "filled", "close_requested")
_KNOWN_RISK_MODES = ("active", "entry_paused", "killed")


class TraderStatusReader:
    """Read-only operational snapshot for the local TraderRd database.

    This reader intentionally does not initialize schemas, load configuration,
    open credentials, or use any exchange/Telegram client. A missing database
    is a valid first-run state and is reported as ``not_started``.
    """

    def __init__(
        self,
        database_path: str | Path,
        *,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._database_path = Path(database_path)
        self._clock = clock or (lambda: datetime.now(timezone.utc))

    def read(self) -> dict[str, Any]:
        now = _aware_utc(self._clock())
        components = _empty_components()
        payload = {
            "schema_version": STATUS_SCHEMA_VERSION,
            "generated_at": now.isoformat(),
            "overall_status": "not_started",
            "database": {
                "available": self._database_path.is_file(),
                "read_only": True,
                "read_error": False,
            },
            "components": components,
            "source": {
                "chat_id": VALIDATED_SOURCE_CHAT_ID,
                "topic_id": VALIDATED_SOURCE_TOPIC_ID,
                "seed_message_id": VALIDATED_SOURCE_SEED_ID,
                "sender_id": VALIDATED_SOURCE_SENDER_ID,
                "latest_signal": None,
            },
            "worker": {
                "cursor_message_id": None,
                "cursor_initialized": False,
                "latest_signal_message_id": None,
                "cursor_lag_messages": None,
                "unprocessed_signal_count": None,
            },
            "execution": {
                "intent_counts": {state: 0 for state in _KNOWN_INTENT_STATES},
                "expiry": {
                    "next_entry_expiry_at": None,
                    "expired_entry_count": 0,
                    "active_entry_count": 0,
                },
                "reconciliation_required": [],
                "active_intents": [],
            },
            "risk": _empty_risk(),
            "account": _empty_account(),
            "performance": _empty_performance(),
            "operations": _empty_operations(),
            "freshness": _empty_freshness(),
            "recent_errors": [],
        }

        if not self._database_path.is_file():
            payload["freshness"] = _freshness_snapshot(payload, now)
            payload["operations"] = _operations_snapshot(payload)
            return payload

        try:
            with self._connection() as connection:
                heartbeat_events = self._heartbeat_events(connection)
                payload["components"] = _component_snapshot(heartbeat_events, now)
                payload["source"]["latest_signal"] = self._latest_signal(connection)
                self._worker_snapshot(connection, payload)
                payload["execution"] = self._execution_snapshot(connection, now)
                payload["risk"] = self._risk_snapshot(connection)
                payload["account"] = self._account_snapshot(connection)
                payload["performance"] = self._performance_snapshot(connection)
                payload["recent_errors"] = _recent_errors(heartbeat_events)
        except (OSError, sqlite3.Error, ValueError, TypeError, json.JSONDecodeError):
            # Status must remain useful even when an older/corrupt DB cannot be
            # fully read. Never expose SQLite details or arbitrary stored text.
            payload["database"]["read_error"] = True
            payload["overall_status"] = "degraded"
            payload["freshness"] = _freshness_snapshot(payload, now)
            payload["operations"] = _operations_snapshot(payload)
            return payload

        payload["overall_status"] = _overall_status(payload["components"].values())
        _attach_risk_to_intents(payload)
        payload["freshness"] = _freshness_snapshot(payload, now)
        payload["operations"] = _operations_snapshot(payload)
        return payload

    def _connection(self) -> sqlite3.Connection:
        database = f"{self._database_path.resolve().as_uri()}?mode=ro"
        connection = sqlite3.connect(database, uri=True, timeout=2)
        connection.row_factory = sqlite3.Row
        return connection

    @staticmethod
    def _heartbeat_events(connection: sqlite3.Connection) -> list[sqlite3.Row]:
        if not _table_exists(connection, "component_heartbeats"):
            return []
        # Consumers need only latest/latest-healthy/latest-error per component
        # and the newest 20 global errors. Select their IDs in one snapshot;
        # never transfer the append-only history into every TUI refresh.
        selections: list[str] = []
        parameters: list[str] = []
        for component in HEARTBEAT_COMPONENTS:
            for state in (None, "healthy", "error"):
                predicate = "component = ?"
                parameters.append(component)
                if state is not None:
                    predicate += " AND state = ?"
                    parameters.append(state)
                selections.append(
                    "SELECT id FROM (SELECT id FROM component_heartbeats WHERE "
                    + predicate + " ORDER BY id DESC LIMIT 1)"
                )
        selections.append(
            "SELECT id FROM (SELECT id FROM component_heartbeats "
            "WHERE state = 'error' ORDER BY id DESC LIMIT 20)"
        )
        return connection.execute(
            "WITH selected_ids AS (" + " UNION ".join(selections) + ") "
            "SELECT component, state, observed_at, operation, error_code "
            "FROM component_heartbeats WHERE id IN (SELECT id FROM selected_ids) "
            "ORDER BY id ASC",
            parameters,
        ).fetchall()

    @staticmethod
    def _latest_signal(connection: sqlite3.Connection) -> dict[str, Any] | None:
        if not _table_exists(connection, "signals"):
            return None
        columns = _columns(connection, "signals")
        required = {
            "source_chat_id",
            "source_topic_id",
            "source_sender_id",
            "source_message_id",
            "direction",
            "symbol",
            "telegram_received_at",
            "fingerprint",
        }
        if not required.issubset(columns):
            return None
        row = connection.execute(
            """
            SELECT source_message_id, direction, symbol,
                   telegram_received_at, fingerprint
            FROM signals
            WHERE source_chat_id = ? AND source_topic_id = ?
              AND source_sender_id = ?
            ORDER BY source_message_id DESC
            LIMIT 1
            """,
            (
                VALIDATED_SOURCE_CHAT_ID,
                VALIDATED_SOURCE_TOPIC_ID,
                VALIDATED_SOURCE_SENDER_ID,
            ),
        ).fetchone()
        if row is None:
            return None
        received_at = _parse_timestamp(row["telegram_received_at"])
        return {
            "message_id": int(row["source_message_id"]),
            "symbol": _safe_token(row["symbol"]),
            "direction": _safe_token(row["direction"]),
            "telegram_received_at": received_at.isoformat() if received_at else None,
            "fingerprint": _safe_token(row["fingerprint"]),
        }

    @staticmethod
    def _worker_snapshot(
        connection: sqlite3.Connection,
        payload: dict[str, Any],
    ) -> None:
        worker = payload["worker"]
        if not _table_exists(connection, "demo_signal_worker_cursors"):
            return
        row = connection.execute(
            """
            SELECT last_message_id
            FROM demo_signal_worker_cursors
            WHERE source_chat_id = ? AND source_topic_id = ?
              AND source_sender_id = ?
            """,
            (
                VALIDATED_SOURCE_CHAT_ID,
                VALIDATED_SOURCE_TOPIC_ID,
                VALIDATED_SOURCE_SENDER_ID,
            ),
        ).fetchone()
        if row is None:
            return
        cursor = int(row["last_message_id"])
        worker["cursor_message_id"] = cursor
        worker["cursor_initialized"] = True
        latest = payload["source"]["latest_signal"]
        latest_id = int(latest["message_id"]) if latest is not None else None
        worker["latest_signal_message_id"] = latest_id
        worker["cursor_lag_messages"] = (
            max(0, latest_id - cursor) if latest_id is not None else 0
        )
        if not _table_exists(connection, "signals"):
            worker["unprocessed_signal_count"] = 0
            return
        columns = _columns(connection, "signals")
        if not {
            "source_chat_id",
            "source_topic_id",
            "source_sender_id",
            "source_message_id",
        }.issubset(columns):
            worker["unprocessed_signal_count"] = 0
            return
        pending = connection.execute(
            """
            SELECT COUNT(*) AS count
            FROM signals
            WHERE source_chat_id = ? AND source_topic_id = ?
              AND source_sender_id = ? AND source_message_id > ?
            """,
            (
                VALIDATED_SOURCE_CHAT_ID,
                VALIDATED_SOURCE_TOPIC_ID,
                VALIDATED_SOURCE_SENDER_ID,
                cursor,
            ),
        ).fetchone()
        worker["unprocessed_signal_count"] = int(pending["count"])

    @staticmethod
    def _execution_snapshot(
        connection: sqlite3.Connection,
        now: datetime,
    ) -> dict[str, Any]:
        result = {
            "intent_counts": {state: 0 for state in _KNOWN_INTENT_STATES},
            "expiry": {
                "next_entry_expiry_at": None,
                "expired_entry_count": 0,
                "active_entry_count": 0,
            },
            "reconciliation_required": [],
            "active_intents": [],
        }
        if not _table_exists(connection, "demo_execution_intents"):
            return result
        counts = connection.execute(
            "SELECT state, COUNT(*) AS count FROM demo_execution_intents GROUP BY state"
        ).fetchall()
        for row in counts:
            state = _safe_token(row["state"])
            if state not in result["intent_counts"]:
                result["intent_counts"][state] = 0
            result["intent_counts"][state] += int(row["count"])

        rows = connection.execute(
            """
            SELECT intent_id, symbol, kind, state, expires_at, updated_at
            FROM demo_execution_intents
            WHERE kind = 'entry' AND state IN ('planned', 'acknowledged', 'working')
            ORDER BY expires_at ASC
            """
        ).fetchall()
        expiries: list[tuple[datetime, str]] = []
        for row in rows:
            result["expiry"]["active_entry_count"] += 1
            expires_at = _parse_timestamp(row["expires_at"])
            if expires_at is None:
                continue
            expiries.append((expires_at, _safe_timestamp_text(row["expires_at"])))
            if expires_at <= now:
                result["expiry"]["expired_entry_count"] += 1
        if expiries:
            future = [item for item in expiries if item[0] > now]
            if future:
                result["expiry"]["next_entry_expiry_at"] = min(
                    future, key=lambda item: item[0]
                )[1]

        columns = _columns(connection, "demo_execution_intents")
        filled_quantity = "filled_quantity" if "filled_quantity" in columns else "NULL AS filled_quantity"
        active_intents = connection.execute(
            f"""
            SELECT intent_id, risk_reservation_id, symbol, kind, state, direction,
                   quantity, {filled_quantity}, price, take_profit, stop_loss,
                   expires_at, updated_at
            FROM demo_execution_intents
            WHERE state IN ('planned', 'acknowledged', 'working', 'filled',
                            'protection_verified', 'position_closed_pending')
            ORDER BY updated_at DESC
            LIMIT 20
            """
        ).fetchall()
        result["active_intents"] = [
            {
                "intent_id": _safe_token(row["intent_id"]),
                "risk_reservation_id": _safe_token(row["risk_reservation_id"]),
                "symbol": _safe_token(row["symbol"]),
                "kind": _safe_token(row["kind"]),
                "state": _safe_token(row["state"]),
                "direction": _safe_token(row["direction"]),
                "quantity": _safe_decimal_text(row["quantity"]),
                "filled_quantity": _safe_decimal_text(row["filled_quantity"]),
                "price": _safe_decimal_text(row["price"]),
                "take_profit": _safe_decimal_text(row["take_profit"]),
                "stop_loss": _safe_decimal_text(row["stop_loss"]),
                "expires_at": _safe_timestamp_text(row["expires_at"]),
                "updated_at": _safe_timestamp_text(row["updated_at"]),
            }
            for row in active_intents
        ]

        reconciliation = connection.execute(
            """
            SELECT intent_id, symbol, kind, updated_at
            FROM demo_execution_intents
            WHERE state = 'reconciliation_required'
            ORDER BY updated_at DESC
            LIMIT 20
            """
        ).fetchall()
        result["reconciliation_required"] = [
            {
                "intent_id": _safe_token(row["intent_id"]),
                "symbol": _safe_token(row["symbol"]),
                "kind": _safe_token(row["kind"]),
                "updated_at": _safe_timestamp_text(row["updated_at"]),
            }
            for row in reconciliation
        ]
        return result

    @staticmethod
    def _account_snapshot(connection: sqlite3.Connection) -> dict[str, Any]:
        result = _empty_account()
        if not _table_exists(connection, "demo_account_snapshots"):
            return result
        row = connection.execute(
            "SELECT * FROM demo_account_snapshots WHERE singleton_id = 1"
        ).fetchone()
        if row is None:
            return result
        for key in (
            "equity", "wallet_balance", "unrealised_pnl", "available_balance",
            "position_initial_margin", "order_initial_margin",
        ):
            result[key] = _safe_decimal_text(row[key])
        result["captured_at"] = _safe_timestamp_text(row["captured_at"])
        if _table_exists(connection, "demo_position_snapshots"):
            rows = connection.execute(
                "SELECT * FROM demo_position_snapshots ORDER BY symbol, direction"
            ).fetchall()
            result["positions"] = [
                {
                    "symbol": _safe_token(item["symbol"]),
                    "direction": _safe_token(item["direction"]),
                    **{
                        key: _safe_decimal_text(item[key])
                        for key in (
                            "quantity", "average_price", "mark_price",
                            "liquidation_price", "unrealised_pnl", "leverage",
                            "position_margin", "take_profit", "stop_loss",
                        )
                    },
                    "captured_at": _safe_timestamp_text(item["captured_at"]),
                }
                for item in rows
            ]
        return result

    @staticmethod
    def _performance_snapshot(connection: sqlite3.Connection) -> dict[str, Any]:
        result = _empty_performance()
        if not _table_exists(connection, "demo_performance_outcomes"):
            return result
        rows = connection.execute(
            """
            SELECT symbol, closed_at, closed_pnl, open_fee, close_fee
            FROM demo_performance_outcomes
            WHERE status = 'attributed'
            ORDER BY symbol, closed_at, risk_reservation_id
            """
        ).fetchall()
        unresolved = connection.execute(
            "SELECT COUNT(*) AS count FROM demo_performance_outcomes WHERE status = 'unresolved'"
        ).fetchone()
        result["unresolved_exit_count"] = int(unresolved["count"])
        overall = _empty_performance_totals()
        closed_dates: list[datetime] = []
        pairs: dict[str, dict[str, Any]] = {}
        for row in rows:
            pnl = _decimal_value(row["closed_pnl"])
            open_fee = _decimal_value(row["open_fee"])
            close_fee = _decimal_value(row["close_fee"])
            if pnl is None or open_fee is None or close_fee is None:
                # A malformed row must never turn into invented performance.
                result["unresolved_exit_count"] += 1
                continue
            pair = pairs.setdefault(str(row["symbol"]), _empty_performance_totals())
            _add_performance_outcome(overall, pnl, open_fee + close_fee)
            _add_performance_outcome(pair, pnl, open_fee + close_fee)
            closed_at = _parse_timestamp(row["closed_at"])
            if closed_at is not None:
                closed_dates.append(closed_at)
        result["pairs"] = [
            {"symbol": symbol, **_finalize_performance_totals(totals)}
            for symbol, totals in sorted(pairs.items())
        ]
        result["overall"] = _finalize_performance_totals(overall)
        result["coverage"] = {
            "first_attributed_close_at": min(closed_dates).isoformat() if closed_dates else None,
            "latest_attributed_close_at": max(closed_dates).isoformat() if closed_dates else None,
            "attributed_exit_count": result["overall"]["closed_trades"],
            "unresolved_exit_count": result["unresolved_exit_count"],
            "complete": result["unresolved_exit_count"] == 0,
        }
        return result

    @staticmethod
    def _risk_snapshot(connection: sqlite3.Connection) -> dict[str, Any]:
        result = _empty_risk()
        if not _table_exists(connection, "risk_engine_state"):
            return result
        row = connection.execute(
            "SELECT state_json FROM risk_engine_state WHERE singleton_id = 1"
        ).fetchone()
        if row is None:
            return result
        state = json.loads(str(row["state_json"]))
        result["initialized"] = True
        mode = state.get("mode")
        result["mode"] = _safe_token(mode) if mode in _KNOWN_RISK_MODES else None
        result["as_of"] = _safe_timestamp_text(state.get("as_of"))
        result["equity"] = _safe_decimal_text(state.get("equity"))
        result["high_watermark"] = _safe_decimal_text(state.get("high_watermark"))
        result["daily_halted"] = _optional_bool(state.get("daily_halted"))
        result["weekly_halted"] = _optional_bool(state.get("weekly_halted"))
        result["drawdown"]["circuit_breaker_latched"] = (
            mode == "killed" if mode in _KNOWN_RISK_MODES else None
        )
        equity = _decimal_value(state.get("equity"))
        high_watermark = _decimal_value(state.get("high_watermark"))
        if (
            equity is not None
            and high_watermark is not None
            and high_watermark > 0
            and equity >= 0
        ):
            result["drawdown"]["current_loss_fraction"] = _canonical_decimal(
                max(Decimal("0"), (high_watermark - equity) / high_watermark)
            )
        reservations = state.get("reservations", {})
        if isinstance(reservations, dict):
            counts = {status: 0 for status in _KNOWN_RESERVATION_STATES}
            active_risk = Decimal("0")
            active_risk_valid = True
            active_count = 0
            for reservation in reservations.values():
                if not isinstance(reservation, dict):
                    continue
                status = _safe_token(reservation.get("status"))
                if status not in counts:
                    counts[status] = 0
                counts[status] += 1
                if status in _ACTIVE_RESERVATION_STATES:
                    active_count += 1
                    reserved_risk = _decimal_value(reservation.get("reserved_risk"))
                    if reserved_risk is None or reserved_risk < 0:
                        active_risk_valid = False
                    elif active_risk_valid:
                        active_risk += reserved_risk
                    proposal = reservation.get("proposal")
                    result["active_reservation_details"].append({
                        "reservation_id": _safe_token(reservation.get("reservation_id")),
                        "status": status,
                        "symbol": _safe_token(proposal.get("symbol")) if isinstance(proposal, dict) else "",
                        "direction": _safe_token(proposal.get("direction")) if isinstance(proposal, dict) else "",
                        "reserved_risk": _safe_decimal_text(reservation.get("reserved_risk")),
                    })
            result["reservation_counts"] = counts
            result["active_reservations"] = active_count
            result["reserved_risk"] = (
                _canonical_decimal(active_risk) if active_risk_valid else None
            )
        policy = state.get("policy")
        if isinstance(policy, dict):
            result["policy"] = {
                "leverage": _safe_decimal_text(policy.get("leverage")),
                "target_trade_risk_fraction": _safe_decimal_text(
                    policy.get("target_trade_risk_fraction")
                ),
                "max_reservations": _optional_int(policy.get("max_reservations")),
                "max_reserved_risk_fraction": _safe_decimal_text(
                    policy.get("max_reserved_risk_fraction")
                ),
                "daily_loss_fraction": _safe_decimal_text(
                    policy.get("daily_loss_fraction")
                ),
                "weekly_loss_fraction": _safe_decimal_text(
                    policy.get("weekly_loss_fraction")
                ),
                "max_drawdown_fraction": _safe_decimal_text(
                    policy.get("max_drawdown_fraction")
                ),
                "pending_entry_ttl_seconds": _optional_int(
                    policy.get("pending_entry_ttl_seconds")
                ),
            }
        return result


class _Ansi:
    """Small ANSI palette for the interactive status view, with no dependency."""

    RESET = "\033[0m"
    BOLD = "\033[1m"
    DIM = "\033[2m"
    RED = "\033[31m"
    GREEN = "\033[32m"
    YELLOW = "\033[33m"
    BLUE = "\033[34m"
    MAGENTA = "\033[35m"
    CYAN = "\033[36m"


def render_status(payload: dict[str, Any], *, color: bool = False) -> str:
    """Render a scan-friendly local operator summary without secrets.

    ``color`` is deliberately opt-in for programmatic callers. The CLI enables
    it only for an interactive TTY, unless ``NO_COLOR`` or ``--no-color`` is set.
    """
    paint = _StatusPalette(color)
    database = payload["database"]
    overall = str(payload["overall_status"])
    lines = [
        f"{paint.heading('TraderRd')} · {paint.muted('Demo Trading')} · {paint.health(overall.upper(), overall)}",
        (
            f"{paint.muted('Snapshot:')} {_display_timestamp(payload['generated_at'])} · "
            f"{paint.muted('database:')} {_database_label(database)}"
        ),
        "",
        paint.section("SYSTEMS"),
    ]
    for component in HEARTBEAT_COMPONENTS:
        snapshot = payload["components"][component]
        status = str(snapshot["status"])
        marker = {"healthy": "OK", "degraded": "WARN", "stopped": "STOP", "not_started": "OFF"}.get(
            status, "UNKNOWN"
        )
        line = (
            f"  {paint.health(f'[{marker}]', status)} {component.title():<8} "
            f"{paint.health(f'{status:<11}', status)} "
            f"{paint.muted('last activity')} {_display_age(snapshot['age_seconds'])}"
        )
        if status in {"not_started", "stopped"}:
            line += f" · {paint.muted('start:')} {snapshot['activation_command']}"
        elif status == "degraded":
            line += f" · {paint.warning('investigate before restarting')}"
        lines.append(line)

    operations = payload.get("operations", _empty_operations())
    lines.extend(["", paint.section("OPERATIONS")])
    operation_status = str(operations.get("status", "not_ready"))
    lines.append(
        "  Trading: " + (
            paint.danger(operation_status.upper())
            if operation_status == "blocked"
            else paint.warning(operation_status.upper())
            if operation_status != "ready"
            else paint.health("READY", "healthy")
        )
    )
    for item in operations.get("attention", [])[:5]:
        lines.append("  " + paint.warning(f"ACTION REQUIRED: {item['message']}"))

    freshness = payload.get("freshness", _empty_freshness())
    lines.append(
        "  Freshness: risk " + _display_age(freshness.get("risk_age_seconds"))
        + " · signal " + _display_age(freshness.get("signal_age_seconds"))
        + " · reconciliation " + _display_age(freshness.get("monitor_age_seconds"))
        + " · market " + _display_age(freshness.get("account_age_seconds"))
    )

    lines.extend(["", paint.section("SIGNAL FLOW")])
    source = payload["source"]["latest_signal"]
    if source is None:
        lines.append("  Latest signal: none stored from the approved Telegram source")
    else:
        lines.append(
            "  Latest signal: "
            f"#{source['message_id']} · {source['symbol']} {source['direction']} · "
            f"received {_display_timestamp(source['telegram_received_at'])}"
        )
    worker = payload["worker"]
    cursor = "not initialized" if not worker["cursor_initialized"] else f"#{worker['cursor_message_id']}"
    lines.append(
        "  Worker cursor: "
        f"{cursor} · lag { _display_count(worker['cursor_lag_messages'], 'message') } · "
        f"queued { _display_count(worker['unprocessed_signal_count'], 'signal') }"
    )

    execution = payload["execution"]
    expiry = execution["expiry"]
    active_states = _active_intent_summary(execution["intent_counts"])
    lines.extend(["", paint.section("EXECUTION")])
    lines.append(
        "  Active: "
        f"{active_states} · pending entries {expiry['active_entry_count']} · "
        f"expired {expiry['expired_entry_count']}"
    )
    if expiry["next_entry_expiry_at"]:
        lines.append(
            "  Next entry expiry: "
            f"{_display_timestamp(expiry['next_entry_expiry_at'])}"
        )
    if execution["reconciliation_required"]:
        lines.append(
            "  " + paint.warning(
                f"ACTION REQUIRED: {len(execution['reconciliation_required'])} item(s) need reconciliation"
            )
        )

    risk = payload["risk"]
    lines.extend(["", paint.section("RISK")])
    if not risk["initialized"]:
        lines.append("  Risk engine: not initialized")
    else:
        lines.append(
            "  Equity: "
            f"{_display_decimal(risk['equity'])} · reserved risk: "
            f"{_display_decimal(risk['reserved_risk'])} · active slots: "
            f"{risk['active_reservations']}"
        )
        controls = (
            f"mode {risk['mode']} · daily halt {_yes_no(risk['daily_halted'])} · "
            f"weekly halt {_yes_no(risk['weekly_halted'])} · drawdown "
            f"{_display_percent(risk['drawdown']['current_loss_fraction'])} · "
            f"circuit breaker {_yes_no(risk['drawdown']['circuit_breaker_latched'])}"
        )
        danger = risk["mode"] == "killed" or bool(risk["daily_halted"]) or bool(risk["weekly_halted"])
        lines.append("  Controls: " + (paint.danger(controls) if danger else controls))

    account = payload.get("account", _empty_account())
    if account.get("captured_at"):
        lines.append(
            "  Account: wallet " + _display_decimal(account.get("wallet_balance"))
            + " USDT · uPnL " + _display_decimal(account.get("unrealised_pnl"))
            + " USDT · available " + _display_decimal(account.get("available_balance"))
            + " USDT · position margin " + _display_decimal(account.get("position_initial_margin"))
            + " USDT"
        )

    performance = payload["performance"]
    total = performance["overall"]
    lines.extend(["", paint.section("PERFORMANCE · Demo · Ledger rollout onward")])
    lines.append(
        "  Net P&L: "
        f"{paint.pnl(_display_decimal(total['net_pnl']), total['net_pnl'])} USDT · exchange closed P&L: "
        f"{paint.pnl(_display_decimal(total['exchange_reported_realized_pnl']), total['exchange_reported_realized_pnl'])} USDT · "
        f"trading fees: {_display_decimal(total['trading_fees'])} USDT"
    )
    lines.append(
        "  Closed trades: "
        f"{total['closed_trades']} · wins {total['wins']} · losses {total['losses']} · "
        f"breakeven {total['breakeven']} · win rate "
        f"{_display_percent(total['win_rate'])}"
    )
    if performance["unresolved_exit_count"]:
        lines.append(
            "  " + paint.warning(
                f"ACTION REQUIRED: {performance['unresolved_exit_count']} closed exit(s) lack unique complete Bybit P&L evidence"
            )
        )
    pairs = performance["pairs"]
    if not pairs:
        lines.append(
            "  Per pair: no uniquely attributed closed trades yet · "
            "rows appear after the first verified Bybit close"
        )
    else:
        lines.append("  Per-pair results:")
        for pair in pairs:
            lines.append(
                f"    {pair['symbol']:<10} {pair['closed_trades']:>3} trades · "
                f"W {pair['wins']} / L {pair['losses']} / BE {pair['breakeven']} · "
                f"win rate {_display_percent(pair['win_rate']):>7} · net "
                f"{paint.pnl(_display_decimal(pair['net_pnl']), pair['net_pnl'])} USDT"
            )

    current, historical = _classify_errors(payload)
    if current:
        lines.extend(["", paint.section("CURRENT INCIDENTS")])
        lines.extend(f"  {paint.warning('[WARN]')} {_render_error(item)}" for item in current[:5])
    if historical:
        lines.extend(["", paint.section("RECENT HISTORY")])
        lines.append(
            f"  Resolved incidents: {len(historical)} · latest: "
            f"{_render_error(historical[0])}"
        )
    return "\n".join(lines)


class _StatusPalette:
    def __init__(self, enabled: bool) -> None:
        self.enabled = enabled

    def _style(self, value: str, *codes: str) -> str:
        if not self.enabled:
            return value
        return "".join(codes) + value + _Ansi.RESET

    def heading(self, value: str) -> str:
        return self._style(value, _Ansi.BOLD, _Ansi.CYAN)

    def section(self, value: str) -> str:
        return self._style(value, _Ansi.BOLD, _Ansi.BLUE)

    def muted(self, value: str) -> str:
        return self._style(value, _Ansi.DIM)

    def health(self, value: str, status: str) -> str:
        code = {
            "healthy": _Ansi.GREEN,
            "degraded": _Ansi.YELLOW,
            "stopped": _Ansi.RED,
            "not_started": _Ansi.MAGENTA,
        }.get(status, _Ansi.YELLOW)
        return self._style(value, code)

    def pnl(self, value: str, raw: Any) -> str:
        decimal = _decimal_value(raw)
        if decimal is None or decimal == 0:
            return value
        return self._style(value, _Ansi.GREEN if decimal > 0 else _Ansi.RED)

    def warning(self, value: str) -> str:
        return self._style(value, _Ansi.YELLOW)

    def danger(self, value: str) -> str:
        return self._style(value, _Ansi.RED)

def _database_label(database: dict[str, Any]) -> str:
    if database["read_error"]:
        return "unreadable"
    return "available" if database["available"] else "not created yet"


def _display_age(seconds: Any) -> str:
    if seconds is None:
        return "unknown"
    try:
        value = max(0, int(float(seconds)))
    except (TypeError, ValueError):
        return "unknown"
    if value < 60:
        return f"{value}s ago"
    minutes, remainder = divmod(value, 60)
    if minutes < 60:
        return f"{minutes}m ago" if remainder == 0 else f"{minutes}m {remainder}s ago"
    hours, minutes = divmod(minutes, 60)
    return f"{hours}h ago" if minutes == 0 else f"{hours}h {minutes}m ago"


def _display_timestamp(value: Any) -> str:
    parsed = _parse_timestamp(value)
    if parsed is None:
        return "unknown"
    local = parsed.astimezone(ZoneInfo("America/Lima"))
    return local.strftime("%Y-%m-%d %H:%M:%S PET")


def _display_count(value: Any, singular: str) -> str:
    if value is None:
        return "unknown"
    try:
        count = int(value)
    except (TypeError, ValueError):
        return "unknown"
    return f"{count} {singular}" + ("" if count == 1 else "s")


def _display_decimal(value: Any) -> str:
    return str(value) if value is not None else "unknown"


def _display_percent(value: Any) -> str:
    decimal = _decimal_value(value)
    if decimal is None:
        return "unknown"
    return f"{(decimal * Decimal('100')):.2f}%"


def _yes_no(value: Any) -> str:
    if value is None:
        return "unknown"
    return "YES" if value else "no"


def _active_intent_summary(counts: dict[str, Any]) -> str:
    active = [
        f"{state.replace('_', ' ')} {count}"
        for state, count in counts.items()
        if count and state in {"planned", "acknowledged", "working", "filled", "protection_verified", "position_closed_pending", "reconciliation_required"}
    ]
    return ", ".join(active) if active else "none"


def _classify_errors(payload: dict[str, Any]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    current: list[dict[str, Any]] = []
    historical: list[dict[str, Any]] = []
    components = payload["components"]
    for item in payload["recent_errors"]:
        component = components.get(item["component"])
        last_error = component.get("last_error") if component else None
        is_latest_error = last_error and last_error.get("at") == item["observed_at"]
        if component and component["status"] == "degraded" and is_latest_error:
            current.append(item)
        else:
            historical.append(item)
    return current, historical


def _render_error(item: dict[str, Any]) -> str:
    return (
        f"{item['component']} · {item['code']} · "
        f"{_display_timestamp(item['observed_at'])}"
    )


def status_json(payload: dict[str, Any]) -> str:
    """Serialize the stable v1 status contract deterministically."""
    return json.dumps(payload, sort_keys=True, separators=(",", ":"))


def _empty_components() -> dict[str, dict[str, Any]]:
    return {
        component: {
            "status": "not_started",
            "latest_state": None,
            "last_heartbeat_at": None,
            "last_success_at": None,
            "last_error": None,
            "age_seconds": None,
            "activation_command": ACTIVATION_COMMANDS[component],
        }
        for component in HEARTBEAT_COMPONENTS
    }


def _empty_risk() -> dict[str, Any]:
    return {
        "initialized": False,
        "mode": None,
        "as_of": None,
        "equity": None,
        "high_watermark": None,
        "daily_halted": None,
        "weekly_halted": None,
        "active_reservations": 0,
        "reserved_risk": None,
        "reservation_counts": {status: 0 for status in _KNOWN_RESERVATION_STATES},
        "active_reservation_details": [],
        "drawdown": {
            "current_loss_fraction": None,
            "circuit_breaker_latched": None,
        },
        "policy": None,
    }


def _empty_account() -> dict[str, Any]:
    return {
        "captured_at": None,
        "equity": None,
        "wallet_balance": None,
        "unrealised_pnl": None,
        "available_balance": None,
        "position_initial_margin": None,
        "order_initial_margin": None,
        "positions": [],
    }


def _empty_operations() -> dict[str, Any]:
    return {"status": "not_ready", "can_open_new_positions": None, "attention": []}


def _empty_freshness() -> dict[str, Any]:
    return {
        "risk_age_seconds": None,
        "signal_age_seconds": None,
        "monitor_age_seconds": None,
        "account_age_seconds": None,
    }


def _empty_performance() -> dict[str, Any]:
    return {
        "reporting_currency": "USDT",
        "scope": "demo_ledger_rollout_forward_only",
        # Bybit documents Closed P&L as the final amount after opening/closing
        # fees and funding.  Fees remain a breakdown, never a second deduction.
        "exchange_closed_pnl_includes_trading_fees": True,
        "win_rate_excludes_breakeven": True,
        "overall": _finalize_performance_totals(_empty_performance_totals()),
        "pairs": [],
        "unresolved_exit_count": 0,
        "coverage": {
            "first_attributed_close_at": None,
            "latest_attributed_close_at": None,
            "attributed_exit_count": 0,
            "unresolved_exit_count": 0,
            "complete": True,
        },
    }


def _empty_performance_totals() -> dict[str, Any]:
    return {
        "closed_trades": 0,
        "wins": 0,
        "losses": 0,
        "breakeven": 0,
        "exchange_reported_realized_pnl": Decimal("0"),
        "trading_fees": Decimal("0"),
    }


def _add_performance_outcome(
    totals: dict[str, Any], pnl: Decimal, fees: Decimal
) -> None:
    totals["closed_trades"] += 1
    totals["exchange_reported_realized_pnl"] += pnl
    totals["trading_fees"] += fees
    if pnl > 0:
        totals["wins"] += 1
    elif pnl < 0:
        totals["losses"] += 1
    else:
        totals["breakeven"] += 1


def _finalize_performance_totals(totals: dict[str, Any]) -> dict[str, Any]:
    resolved = totals["wins"] + totals["losses"]
    win_rate = (
        _canonical_decimal(Decimal(totals["wins"]) / Decimal(resolved))
        if resolved else None
    )
    closed_pnl = totals["exchange_reported_realized_pnl"]
    return {
        "closed_trades": int(totals["closed_trades"]),
        "wins": int(totals["wins"]),
        "losses": int(totals["losses"]),
        "breakeven": int(totals["breakeven"]),
        "win_rate": win_rate,
        "exchange_reported_realized_pnl": _canonical_decimal(closed_pnl),
        "trading_fees": _canonical_decimal(totals["trading_fees"]),
        # Closed P&L is already net of Bybit's recorded costs.  This is not
        # closed P&L minus fees, which would double count them.
        "net_pnl": _canonical_decimal(closed_pnl),
    }


def _component_snapshot(
    rows: Iterable[sqlite3.Row],
    now: datetime,
) -> dict[str, dict[str, Any]]:
    events: dict[str, list[sqlite3.Row]] = {component: [] for component in HEARTBEAT_COMPONENTS}
    for row in rows:
        component = str(row["component"])
        if component in events:
            events[component].append(row)
    result = _empty_components()
    for component, component_events in events.items():
        if not component_events:
            continue
        latest = component_events[-1]
        latest_at = _parse_timestamp(latest["observed_at"])
        age = max(0.0, (now - latest_at).total_seconds()) if latest_at else None
        successes = [
            row for row in component_events if str(row["state"]) == "healthy"
        ]
        errors = [row for row in component_events if str(row["state"]) == "error"]
        last_success = successes[-1] if successes else None
        last_error = errors[-1] if errors else None
        health = _component_health(latest, latest_at, last_success, last_error, now)
        result[component] = {
            "status": health,
            "latest_state": _safe_token(latest["state"]),
            "last_heartbeat_at": _safe_timestamp_text(latest["observed_at"]),
            "last_success_at": (
                _safe_timestamp_text(last_success["observed_at"])
                if last_success
                else None
            ),
            "last_error": (
                {
                    "at": _safe_timestamp_text(last_error["observed_at"]),
                    "code": _safe_token(last_error["error_code"]),
                    "operation": _safe_token(last_error["operation"]),
                }
                if last_error
                else None
            ),
            "age_seconds": age,
            "activation_command": _activation_command(component, health),
        }
    return result


def _activation_command(component: str, status: str) -> str | None:
    if status in {"not_started", "stopped"}:
        return ACTIVATION_COMMANDS[component]
    return None


def _component_health(
    latest: sqlite3.Row,
    latest_at: datetime | None,
    last_success: sqlite3.Row | None,
    last_error: sqlite3.Row | None,
    now: datetime,
) -> str:
    if latest_at is None:
        return "degraded"
    age = max(0.0, (now - latest_at).total_seconds())
    if str(latest["state"]) == "stopped":
        return "stopped"
    if age > 300:
        return "stopped"
    if str(latest["state"]) in {"starting", "error"}:
        return "degraded"
    if last_error is not None and last_success is not None:
        error_at = _parse_timestamp(last_error["observed_at"])
        success_at = _parse_timestamp(last_success["observed_at"])
        if error_at is not None and success_at is not None and error_at > success_at:
            return "degraded"
    return "healthy" if age <= 90 else "degraded"


def _overall_status(statuses: Iterable[dict[str, Any]]) -> str:
    values = [str(item["status"]) for item in statuses]
    if not values or all(value == "not_started" for value in values):
        return "not_started"
    if "stopped" in values:
        return "stopped"
    if "degraded" in values or "not_started" in values:
        return "degraded"
    return "healthy"


def _attach_risk_to_intents(payload: dict[str, Any]) -> None:
    details = {
        item.get("reservation_id"): item
        for item in payload["risk"].get("active_reservation_details", [])
    }
    positions = {
        (item.get("symbol"), item.get("direction")): item
        for item in payload["account"].get("positions", [])
    }
    for intent in payload["execution"].get("active_intents", []):
        reservation = details.get(intent.get("risk_reservation_id"), {})
        intent["reserved_risk"] = reservation.get("reserved_risk")
        intent["risk_status"] = reservation.get("status")
        position = positions.get((intent.get("symbol"), intent.get("direction")), {})
        intent["position"] = {
            key: position.get(key)
            for key in (
                "quantity", "average_price", "mark_price", "liquidation_price",
                "unrealised_pnl", "leverage", "position_margin", "take_profit",
                "stop_loss", "captured_at",
            )
        } if position else None


def _freshness_snapshot(payload: dict[str, Any], now: datetime) -> dict[str, Any]:
    signal = payload["source"].get("latest_signal") or {}
    return {
        "risk_age_seconds": _timestamp_age(payload["risk"].get("as_of"), now),
        "signal_age_seconds": _timestamp_age(signal.get("telegram_received_at"), now),
        "monitor_age_seconds": payload["components"]["monitor"].get("age_seconds"),
        "account_age_seconds": _timestamp_age(payload["account"].get("captured_at"), now),
    }


def _timestamp_age(value: Any, now: datetime) -> float | None:
    parsed = _parse_timestamp(value)
    return max(0.0, (now - parsed).total_seconds()) if parsed is not None else None


def _operations_snapshot(payload: dict[str, Any]) -> dict[str, Any]:
    attention: list[dict[str, str]] = []
    hard_block = False

    def add(code: str, message: str, *, severity: str = "warning", blocks: bool = False) -> None:
        nonlocal hard_block
        attention.append({"code": code, "severity": severity, "message": message})
        hard_block = hard_block or blocks

    if payload["database"].get("read_error"):
        add("database_unreadable", "Local status database is unreadable", severity="critical", blocks=True)
    unhealthy = [
        name for name, item in payload["components"].items()
        if item.get("status") != "healthy"
    ]
    if unhealthy:
        add("components_unhealthy", "Runtime component health requires attention", blocks=True)
    risk = payload["risk"]
    if not risk.get("initialized"):
        add("risk_uninitialized", "Risk engine is not initialized", blocks=True)
    elif risk.get("mode") != "active":
        add("risk_mode_blocked", f"Risk mode is {_safe_token(risk.get('mode')) or 'unknown'}", severity="critical", blocks=True)
    if risk.get("daily_halted"):
        add("daily_halt", "Daily loss halt is active", severity="critical", blocks=True)
    if risk.get("weekly_halted"):
        add("weekly_halt", "Weekly loss halt is active", severity="critical", blocks=True)
    execution = payload["execution"]
    reconciliation_count = len(execution.get("reconciliation_required", []))
    if reconciliation_count:
        add("reconciliation_required", f"{reconciliation_count} execution item(s) require reconciliation", severity="critical", blocks=True)
    expired = int(execution.get("expiry", {}).get("expired_entry_count") or 0)
    if expired:
        add("expired_entries", f"{expired} pending entry order(s) are expired", severity="critical", blocks=True)
    unresolved = int(payload["performance"].get("unresolved_exit_count") or 0)
    if unresolved:
        add("unattributed_closes", f"{unresolved} closed exit(s) lack complete P&L attribution")
    pending_closes = sum(
        item.get("state") == "position_closed_pending"
        for item in execution.get("active_intents", [])
    )
    if pending_closes:
        add("close_attribution_pending", f"{pending_closes} closed position(s) await P&L attribution")
    account_age = payload["freshness"].get("account_age_seconds")
    if payload["account"].get("positions") and (account_age is None or account_age > 90):
        add("position_snapshot_stale", "Persisted position metrics are stale", blocks=False)

    if not payload["database"].get("available") or not risk.get("initialized"):
        status = "not_ready"
    elif hard_block:
        status = "blocked"
    elif attention:
        status = "attention"
    else:
        status = "ready"
    attention.sort(key=lambda item: 0 if item["severity"] == "critical" else 1)
    return {
        "status": status,
        "can_open_new_positions": status in {"ready", "attention"},
        "attention": attention,
    }


def _recent_errors(rows: Iterable[sqlite3.Row]) -> list[dict[str, Any]]:
    errors = [row for row in rows if str(row["state"]) == "error"]
    errors.reverse()
    return [
        {
            "component": _safe_token(row["component"]),
            "observed_at": _safe_timestamp_text(row["observed_at"]),
            "operation": _safe_token(row["operation"]),
            "code": _safe_token(row["error_code"]),
        }
        for row in errors[:20]
    ]


def _table_exists(connection: sqlite3.Connection, table: str) -> bool:
    row = connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table,),
    ).fetchone()
    return row is not None


def _columns(connection: sqlite3.Connection, table: str) -> set[str]:
    return {str(row["name"]) for row in connection.execute(f"PRAGMA table_info({table})")}


def _parse_timestamp(value: Any) -> datetime | None:
    if value is None:
        return None
    try:
        parsed = datetime.fromisoformat(str(value))
    except (TypeError, ValueError):
        return None
    return _aware_utc(parsed)


def _aware_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _safe_token(value: Any) -> str:
    if value is None:
        return "unknown"
    return _SAFE_TOKEN.sub("_", str(value).strip())[:96] or "unknown"


def _safe_timestamp_text(value: Any) -> str | None:
    parsed = _parse_timestamp(value)
    return parsed.isoformat() if parsed else None


def _safe_decimal_text(value: Any) -> str | None:
    parsed = _decimal_value(value)
    return _canonical_decimal(parsed) if parsed is not None else None


def _decimal_value(value: Any) -> Decimal | None:
    if value is None:
        return None
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError, ArithmeticError):
        return None
    return parsed if parsed.is_finite() else None


def _canonical_decimal(value: Decimal) -> str:
    if value == 0:
        return "0"
    return format(value.normalize(), "f")


def _optional_bool(value: Any) -> bool | None:
    return value if isinstance(value, bool) else None


def _optional_int(value: Any) -> int | None:
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None
