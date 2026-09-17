from __future__ import annotations

from contextlib import closing
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import json
from pathlib import Path
import sqlite3
from typing import Any, Callable

from traderrd.application.risk_engine import decision_from_dict
from traderrd.domain.execution import (
    ExecutionIntent,
    ExecutionIntentKind,
    ExecutionIntentState,
    InstrumentRules,
)
from traderrd.domain.models import Direction, canonical_decimal
from traderrd.domain.risk import EngineMode, PortfolioState, ReservationStatus, RiskActionType, RiskDecision
from traderrd.infrastructure.bybit_demo import (
    BybitV5DemoClient,
    DemoExecutionError,
)
from traderrd.infrastructure.execution_repository import (
    SQLiteDemoExecutionRepository,
)
from traderrd.infrastructure.risk_repository import SQLiteRiskStateRepository


ORDER_LINK_PREFIX = "trd-demo-"


class DemoExecutionPlanner:
    def __init__(self, database_path: str | Path) -> None:
        self._database_path = Path(database_path)

    def action_count(self, risk_command_id: str | None = None) -> int:
        return sum(
            len(decision.actions)
            for command_id, decision in self._risk_decisions()
            if risk_command_id is None or command_id == risk_command_id
        )

    def preview(
        self,
        rules: InstrumentRules,
        risk_command_id: str | None = None,
    ) -> list[ExecutionIntent]:
        risk_state = SQLiteRiskStateRepository(self._database_path).load_state()
        if risk_state is None:
            raise ValueError("Risk engine state is not initialized")
        intents: list[ExecutionIntent] = []
        for command_id, decision in self._risk_decisions():
            if risk_command_id is not None and command_id != risk_command_id:
                continue
            intents.extend(
                self.plan_decision(rules, command_id, decision, risk_state)
            )
        unique = {intent.intent_id: intent for intent in intents}
        return list(unique.values())

    @staticmethod
    def plan_decision(
        rules: InstrumentRules,
        risk_command_id: str,
        decision: RiskDecision,
        risk_state: PortfolioState,
    ) -> list[ExecutionIntent]:
        intents: list[ExecutionIntent] = []
        for action in decision.actions:
            reservation = risk_state.reservations.get(action.reservation_id)
            if reservation is None or reservation.proposal.symbol != rules.symbol:
                continue
            kind = _kind(action.action_type)
            quantity = rules.normalize_quantity(
                action.quantity, reservation.proposal.entry
            )
            price = None
            if kind is ExecutionIntentKind.ENTRY:
                price = rules.normalize_entry_price(
                    reservation.proposal.entry,
                    reservation.proposal.direction,
                )
            intents.append(
                ExecutionIntent(
                    intent_id=_intent_id(
                        risk_command_id, kind, action.reservation_id
                    ),
                    order_link_id=_order_link_id(kind, action.reservation_id),
                    risk_command_id=risk_command_id,
                    risk_reservation_id=action.reservation_id,
                    kind=kind,
                    state=ExecutionIntentState.PLANNED,
                    symbol=reservation.proposal.symbol,
                    direction=reservation.proposal.direction,
                    quantity=quantity,
                    price=price,
                    take_profit=reservation.proposal.take_profit,
                    stop_loss=reservation.proposal.stop_loss,
                    expires_at=action.expires_at,
                )
            )
        return intents

    def _risk_decisions(self) -> list[tuple[str, Any]]:
        database = f"{self._database_path.resolve().as_uri()}?mode=ro"
        with closing(sqlite3.connect(database, uri=True)) as connection:
            connection.row_factory = sqlite3.Row
            rows = connection.execute(
                """
                SELECT command_id, decision_json FROM risk_engine_commands
                ORDER BY id
                """
            ).fetchall()
        return [
            (row["command_id"], decision_from_dict(json.loads(row["decision_json"])))
            for row in rows
        ]


class DemoExecutionService:
    def __init__(
        self,
        client: BybitV5DemoClient,
        repository: SQLiteDemoExecutionRepository,
        rules: InstrumentRules,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._client = client
        self._repository = repository
        self._rules = rules
        self._clock = clock or (lambda: datetime.now(timezone.utc))

    def submit(self, intent: ExecutionIntent) -> ExecutionIntent:
        self._validate_namespace(intent.order_link_id)
        stored = self._repository.save_planned(intent)
        if stored.state is not ExecutionIntentState.PLANNED:
            return stored
        if intent.kind is ExecutionIntentKind.ENTRY:
            now = self._clock()
            if now.tzinfo is None:
                raise ValueError("Execution clock must be timezone-aware")
            if self._repository.has_submission_attempt(intent.intent_id):
                # An uncertain POST must never be repeated or assumed absent.
                try:
                    return self.reconcile(intent.intent_id)
                except DemoExecutionError:
                    self._unresolved(intent, "submission_outcome_unknown")
                    raise
            if intent.expires_at is None or intent.expires_at <= now:
                return self._repository.transition(
                    intent.intent_id, ExecutionIntentState.CANCELLED,
                    "entry_expired_before_submission", "never_submitted_no_exposure",
                )
            risk = SQLiteRiskStateRepository(self._repository.database_path).load_state()
            reservation = risk.reservations.get(intent.risk_reservation_id) if risk else None
            if reservation is None or reservation.status is not ReservationStatus.PENDING:
                self._unresolved(intent, "risk_submission_guard")
                raise DemoExecutionError("risk_submission_guard", "Entry requires an active pending reservation; reconcile owned state first")
            reversal = risk.reversal_intents.get(intent.symbol)
            if risk.mode is not EngineMode.ACTIVE or (
                reversal is not None and reversal.closing_reservation_id == intent.risk_reservation_id
            ):
                return self._repository.transition(
                    intent.intent_id, ExecutionIntentState.CANCELLED,
                    "entry_cancelled_before_submission", "risk_policy_blocks_unsent_entry",
                )
            self._repository.mark_submission_attempt(intent.intent_id)
            payload = self._entry_payload(intent)
            response = self._client.request("POST", "/v5/order/create", body=payload)
        elif intent.kind is ExecutionIntentKind.CANCEL_ENTRY:
            entry = self._owned_entry(intent.risk_reservation_id)
            if entry.state in {ExecutionIntentState.PLANNED, ExecutionIntentState.CANCELLED} and not self._repository.has_submission_attempt(entry.intent_id):
                self._repository.transition(entry.intent_id, ExecutionIntentState.CANCELLED,
                    "entry_cancelled_before_submission", "never_submitted_no_exposure")
                return self._repository.transition(intent.intent_id, ExecutionIntentState.CANCELLED,
                    "cancel_confirmed", "never_submitted_owned_entry")
            if entry.state in {ExecutionIntentState.CANCELLED, ExecutionIntentState.REJECTED, ExecutionIntentState.POSITION_CLOSED}:
                return self._retire_completed_cancel(intent, entry)
            response = self._client.request(
                "POST",
                "/v5/order/cancel",
                body={
                    "category": "linear",
                    "symbol": entry.symbol,
                    "orderLinkId": entry.order_link_id,
                },
            )
        elif intent.kind is ExecutionIntentKind.CLOSE_POSITION:
            if self._repository.has_submission_attempt(intent.intent_id):
                return self.reconcile(intent.intent_id)
            entry = self._owned_entry(intent.risk_reservation_id)
            if entry.state not in {
                ExecutionIntentState.FILLED,
                ExecutionIntentState.PROTECTION_VERIFIED,
            }:
                raise DemoExecutionError(
                    "close_guard", "Only an owned filled position can be closed"
                )
            if entry.state is ExecutionIntentState.FILLED and self._find_order(entry).get("orderStatus") not in {"Filled", "Cancelled", "Deactivated", "PartiallyFilledCanceled"}:
                raise DemoExecutionError("close_guard", "Confirm entry remainder cancelled before closing owned exposure")
            self._repository.mark_submission_attempt(intent.intent_id)
            response = self._client.request(
                "POST",
                "/v5/order/create",
                body=self._close_payload(replace(intent, quantity=entry.filled_quantity or intent.quantity)),
            )
        else:
            raise ValueError("Trading-stop intents are internal only")
        result = response["result"]
        exchange_order_id = result.get("orderId")
        if intent.kind is not ExecutionIntentKind.CANCEL_ENTRY and (
            not isinstance(exchange_order_id, str) or not exchange_order_id
        ):
            raise DemoExecutionError(
                "malformed_response", "Missing Demo Trading order ID"
            )
        return self._repository.transition(
            intent.intent_id,
            ExecutionIntentState.ACKNOWLEDGED,
            "request_acknowledged",
            "acknowledgement_is_not_fill",
            exchange_order_id,
        )

    def reconcile(self, intent_id: str) -> ExecutionIntent:
        intent = self._repository.get(intent_id)
        if intent is None:
            raise ValueError("Execution intent does not exist")
        self._validate_namespace(intent.order_link_id)
        if intent.state in {
            ExecutionIntentState.PROTECTION_VERIFIED,
            ExecutionIntentState.POSITION_CLOSED_PENDING,
        }:
            return self._reconcile_protected_position(intent)
        if intent.state in {
            ExecutionIntentState.POSITION_CLOSED_PENDING,
            ExecutionIntentState.POSITION_CLOSED,
            ExecutionIntentState.CLOSE_CONFIRMED,
            ExecutionIntentState.CANCELLED,
            ExecutionIntentState.REJECTED,
        }:
            return intent
        if intent.kind is ExecutionIntentKind.CANCEL_ENTRY:
            entry = self._owned_entry(intent.risk_reservation_id)
            if entry.state in {ExecutionIntentState.CANCELLED, ExecutionIntentState.REJECTED, ExecutionIntentState.POSITION_CLOSED}:
                return self._retire_completed_cancel(intent, entry)
            reconciled = self.reconcile(entry.intent_id)
            if reconciled.state in {ExecutionIntentState.FILLED, ExecutionIntentState.PROTECTION_VERIFIED}:
                return self._repository.transition(
                    intent.intent_id, ExecutionIntentState.FILLED,
                    "cancel_raced_fill", "owned_entry_has_exposure",
                )
            if reconciled.state not in {ExecutionIntentState.CANCELLED, ExecutionIntentState.POSITION_CLOSED}:
                return self._repository.transition(
                    intent.intent_id, ExecutionIntentState.RECONCILIATION_REQUIRED,
                    "cancel_not_confirmed", "owned_entry_not_confirmed_cancelled_flat",
                )
            return self._repository.transition(
                intent.intent_id, ExecutionIntentState.CANCELLED,
                "cancel_confirmed", "owned_order_cancelled_flat",
            )
        order = self._find_order(intent)
        status = order.get("orderStatus")
        if intent.kind is ExecutionIntentKind.ENTRY and status in {
            "PartiallyFilled", "Filled", "Cancelled", "Deactivated", "Rejected",
            "PartiallyFilledCanceled",
        }:
            return self._reconcile_entry_order(intent, order)
        if status in {"New", "Untriggered"}:
            return self._repository.transition(
                intent.intent_id,
                ExecutionIntentState.WORKING,
                "order_working",
                str(status),
            )
        if status in {"Cancelled", "Deactivated"}:
            return self._repository.transition(
                intent.intent_id,
                ExecutionIntentState.CANCELLED,
                "order_cancelled",
                str(status),
            )
        if status == "Rejected":
            return self._repository.transition(
                intent.intent_id,
                ExecutionIntentState.REJECTED,
                "order_rejected",
                "opaque_exchange_rejection",
            )
        if status != "Filled":
            return self._repository.transition(
                intent.intent_id,
                ExecutionIntentState.RECONCILIATION_REQUIRED,
                "unknown_order_state",
                "manual_demo_reconciliation_required",
            )
        rows = self._execution_rows(intent)
        entry = self._owned_entry(intent.risk_reservation_id)
        expected_quantity = entry.filled_quantity
        expected_side = "Sell" if intent.direction is Direction.LONG else "Buy"
        order_id = order.get("orderId")
        identifiers = [row.get("execId") for row in rows]
        if (
            not rows or expected_quantity is None or expected_quantity <= 0
            or not isinstance(order_id, str) or not order_id
            or (intent.exchange_order_id is not None and order_id != intent.exchange_order_id)
            or any(not isinstance(value, str) or not value for value in identifiers)
            or len(set(identifiers)) != len(identifiers)
            or any(row.get("orderLinkId") != intent.order_link_id
                   or row.get("orderId") != order_id
                   or row.get("symbol") != intent.symbol
                   or row.get("side") != expected_side
                   or _safe_decimal(row.get("execQty")) <= 0 for row in rows)
            or sum((_safe_decimal(row.get("execQty")) for row in rows), Decimal("0")) != expected_quantity
            or _safe_decimal(order.get("cumExecQty")) != expected_quantity
        ):
            return self._unresolved(intent, "close_execution_evidence_mismatch")
        position = self._position(intent.symbol)
        if "size" not in position:
            raise DemoExecutionError(
                "position_reconciliation", "One-way Demo Trading position size unavailable"
            )
        size = _safe_decimal(position["size"])
        if intent.kind is ExecutionIntentKind.CLOSE_POSITION:
            if size != 0:
                return self._repository.transition(
                    intent.intent_id,
                    ExecutionIntentState.RECONCILIATION_REQUIRED,
                    "close_not_flat",
                    "position_size_nonzero",
                )
            return self._repository.transition(
                intent.intent_id,
                ExecutionIntentState.CLOSE_CONFIRMED,
                "close_confirmed",
                "order_execution_and_flat_position",
            )
        return self._unresolved(intent, "unexpected_filled_intent_kind")

    def _reconcile_entry_order(self, intent: ExecutionIntent, order: dict[str, Any], *, protect: bool = True) -> ExecutionIntent:
        status = order.get("orderStatus")
        quantity = _safe_decimal(order.get("cumExecQty"))
        if quantity == 0 and status in {"Cancelled", "Deactivated", "Rejected"}:
            position = self._position(intent.symbol)
            size = _safe_decimal(position.get("size"))
            if size != 0 or intent.filled_quantity is not None:
                return self._unresolved(intent, "cancel_position_mismatch")
            return self._repository.transition(
                intent.intent_id,
                ExecutionIntentState.REJECTED if status == "Rejected" else ExecutionIntentState.CANCELLED,
                "entry_terminal_flat", "zero_execution_and_flat_position",
            )
        rows = self._execution_rows(intent)
        if not rows:
            return self._unresolved(intent, "incomplete_execution_evidence")
        identifiers = [row.get("execId") for row in rows]
        if (any(not isinstance(value, str) or not value for value in identifiers)
            or len(set(identifiers)) != len(identifiers)
            or any(row.get("orderLinkId", intent.order_link_id) != intent.order_link_id
                   or row.get("symbol", intent.symbol) != intent.symbol
                   or (intent.exchange_order_id is not None and row.get("orderId", intent.exchange_order_id) != intent.exchange_order_id)
                   for row in rows)
            or sum((_safe_decimal(row.get("execQty")) for row in rows), Decimal("0")) != quantity):
            return self._unresolved(intent, "execution_quantity_mismatch")
        if not 0 < quantity <= intent.quantity or (intent.filled_quantity is not None and quantity < intent.filled_quantity):
            return self._unresolved(intent, "execution_quantity_mismatch")
        position = self._position(intent.symbol)
        size = _safe_decimal(position.get("size"))
        if (size == 0 and intent.filled_quantity == quantity
            and status in {"Filled", "Cancelled", "Deactivated", "PartiallyFilledCanceled"}):
            return self._repository.transition(
                intent.intent_id, ExecutionIntentState.POSITION_CLOSED_PENDING,
                "position_closed_detected", "owned_position_flat; exit_reason_unknown",
            )
        if size != quantity or not _position_side_matches(position, intent.direction):
            return self._unresolved(intent, "fill_position_mismatch")
        self._repository.record_fill_quantity(intent.intent_id, quantity)
        filled = self._repository.transition(
            intent.intent_id, ExecutionIntentState.FILLED, "fill_confirmed",
            "cumulative_execution_and_position_confirmed",
        )
        if not protect:
            return filled
        self._set_and_verify_protection(filled)
        if status == "PartiallyFilled":
            self._client.request("POST", "/v5/order/cancel", body={
                "category": "linear", "symbol": intent.symbol, "orderLinkId": intent.order_link_id,
            })
            final = self._find_order(intent)
            if final.get("orderStatus") not in {"Filled", "Cancelled", "Deactivated", "PartiallyFilledCanceled"}:
                return self._unresolved(filled, "partial_remainder_not_cancelled")
            # Re-read cumulative fills and position after cancellation: another
            # fill may have raced with the cancellation request.
            return self._reconcile_entry_order(filled, final)
        return self._repository.transition(
            intent.intent_id, ExecutionIntentState.PROTECTION_VERIFIED,
            "protection_verified", "exchange_side_tp_sl_no_live_remainder",
        )

    def reconcile_for_operator_close(self, intent_id: str) -> ExecutionIntent:
        """Prove terminal owned entry exposure without installing stale TP/SL."""
        entry = self._repository.get(intent_id)
        if entry is None or entry.kind is not ExecutionIntentKind.ENTRY:
            raise DemoExecutionError("close_guard", "Operator close requires an owned entry intent")
        self._validate_namespace(entry.order_link_id)
        if entry.state in {ExecutionIntentState.CANCELLED, ExecutionIntentState.REJECTED}:
            raise DemoExecutionError("close_guard", "Inactive entry requires explicit reconciliation")
        if entry.state is ExecutionIntentState.POSITION_CLOSED:
            return entry
        order = self._find_order(entry)
        if order.get("orderStatus") not in {"Filled", "Cancelled", "Deactivated", "PartiallyFilledCanceled"}:
            raise DemoExecutionError("close_guard", "Confirm entry remainder cancelled before operator closure")
        reconciled = self._reconcile_entry_order(entry, order, protect=False)
        if reconciled.state not in {ExecutionIntentState.FILLED, ExecutionIntentState.POSITION_CLOSED_PENDING}:
            raise DemoExecutionError("close_guard", "Owned execution and position evidence do not agree")
        return reconciled

    def _execution_rows(self, intent: ExecutionIntent) -> list[dict[str, Any]]:
        return self._paginated_rows("/v5/execution/list", {
            "category": "linear", "symbol": intent.symbol,
            "orderLinkId": intent.order_link_id,
        }, page_size=100)

    def _paginated_rows(
        self, path: str, params: dict[str, str], *, page_size: int
    ) -> list[dict[str, Any]]:
        """Collect bounded complete evidence; never interpret a cursor as a row."""
        params = {**params, "limit": str(page_size)}
        rows: list[dict[str, Any]] = []
        cursors: set[str] = set()
        for _ in range(10):
            response = self._client.request("GET", path, dict(params))
            page = _result_rows(response)
            if len(page) > page_size:
                raise DemoExecutionError("execution_pagination", "Evidence page exceeded safety bound")
            rows.extend(page)
            cursor = response["result"].get("nextPageCursor")
            if cursor in (None, ""):
                return rows
            if not isinstance(cursor, str) or cursor in cursors:
                raise DemoExecutionError("execution_pagination", "Evidence pagination is ambiguous")
            cursors.add(cursor)
            params["cursor"] = cursor
        raise DemoExecutionError("execution_pagination", "Evidence pagination exceeded safety bound")

    def _retire_completed_cancel(
        self, intent: ExecutionIntent, entry: ExecutionIntent
    ) -> ExecutionIntent:
        """Retire a redundant request, never repost or resurrect its parent.

        Old confirmation decisions could emit a second cancellation action.
        Durable terminal parent plus current flat/open-order evidence is sufficient
        even after the historical order disappears from exchange history. Closed
        parents additionally require completed risk and attributed outcome records.
        """
        def refuse(reason: str) -> None:
            self._unresolved(intent, reason)
            raise DemoExecutionError(
                "cancel_reconciliation", "Completed cancellation conflicts with current exchange evidence; reconcile before new entries"
            )

        closed = entry.state is ExecutionIntentState.POSITION_CLOSED
        risk = SQLiteRiskStateRepository(self._repository.database_path).load_state()
        reservation = risk.reservations.get(entry.risk_reservation_id) if risk else None
        if closed and (reservation is None or reservation.status is not ReservationStatus.CLOSED
                       or not self._repository.outcome_is_attributed(entry.risk_reservation_id)):
            refuse("completed_close_accounting_incomplete")
        try:
            rows = self._paginated_rows("/v5/order/realtime", {
                "category": "linear", "symbol": entry.symbol,
                "orderLinkId": entry.order_link_id, "openOnly": "0",
            }, page_size=50)
        except DemoExecutionError:
            self._unresolved(intent, "completed_cancel_order_pages_incomplete")
            raise
        if len(rows) > 1 or any(
            row.get("orderLinkId") != entry.order_link_id
            or row.get("symbol") != entry.symbol
            or (entry.exchange_order_id is not None
                and row.get("orderId") != entry.exchange_order_id)
            or row.get("orderStatus") not in ({"Filled", "Cancelled", "Deactivated", "PartiallyFilledCanceled"} if closed else {"Cancelled", "Deactivated", "Rejected"})
            or _safe_decimal(row.get("cumExecQty")) != (entry.filled_quantity if closed else Decimal("0"))
            for row in rows
        ):
            refuse("completed_cancel_order_not_terminal")
        # Check current exposure after the complete order walk, not before it.
        position = self._position(entry.symbol)
        if _safe_decimal(position.get("size")) != 0:
            refuse("completed_cancel_position_not_flat")
        if not closed and entry.filled_quantity is not None:
            refuse("completed_cancel_has_fill_evidence")
        retired = self._repository.transition(
            intent.intent_id, ExecutionIntentState.CANCELLED,
            "redundant_cancel_retired", "parent_terminal_current_flat_no_live_order",
        )
        risk = SQLiteRiskStateRepository(self._repository.database_path).load_state()
        reservation = risk.reservations.get(entry.risk_reservation_id) if risk else None
        if reservation is not None and reservation.status in {
            ReservationStatus.CANCELLED, ReservationStatus.EXPIRED, ReservationStatus.CLOSED,
        }:
            self._repository.mark_risk_sync_completed(intent.intent_id)
        return retired

    def _unresolved(self, intent: ExecutionIntent, reason: str) -> ExecutionIntent:
        return self._repository.transition(
            intent.intent_id, ExecutionIntentState.RECONCILIATION_REQUIRED,
            reason, "manual_demo_reconciliation_required",
        )

    def attribute_closed_outcome(
        self, intent: ExecutionIntent, observed_at: datetime
    ) -> bool:
        """Store closed-PnL only when Bybit supplies one complete unique match.

        ``closedPnl`` is Bybit's final closed P&L (already inclusive of its
        reported fees/funding).  We keep the fees as a breakdown and never
        subtract them again.
        """
        if observed_at.tzinfo is None:
            raise ValueError("Close observation timestamp must be timezone-aware")
        entry = intent if intent.kind is ExecutionIntentKind.ENTRY else self._owned_entry(
            intent.risk_reservation_id
        )
        if self._repository.outcome_is_attributed(entry.risk_reservation_id):
            return True
        if entry.expires_at is None:
            self._repository.record_unresolved_outcome(entry, "missing_entry_time")
            return False
        opened_after = entry.expires_at - timedelta(hours=3)
        if observed_at < opened_after or observed_at - opened_after > timedelta(days=7):
            self._repository.record_unresolved_outcome(entry, "closed_pnl_time_window")
            return False
        rows = self._closed_pnl_rows(
            entry.symbol,
            int(opened_after.timestamp() * 1000),
            int(observed_at.timestamp() * 1000),
        )
        match, reason = _unique_closed_pnl_match(
            rows, entry, opened_after, observed_at
        )
        if match is None:
            self._repository.record_unresolved_outcome(entry, reason)
            return False
        self._repository.record_attributed_outcome(
            entry,
            exchange_order_id=match["order_id"],
            closed_at=match["closed_at"],
            closed_pnl=match["closed_pnl"],
            open_fee=match["open_fee"],
            close_fee=match["close_fee"],
        )
        return True

    def _closed_pnl_rows(
        self, symbol: str, start_time_ms: int, end_time_ms: int
    ) -> list[dict[str, Any]]:
        """Read a bounded complete result set instead of silently trusting page 1."""
        params = {
            "category": "linear",
            "symbol": symbol,
            "startTime": str(start_time_ms),
            "endTime": str(end_time_ms),
            "limit": "100",
        }
        rows: list[dict[str, Any]] = []
        cursor: str | None = None
        for _ in range(10):
            request_params = dict(params)
            if cursor is not None:
                request_params["cursor"] = cursor
            payload = self._client.request(
                "GET", "/v5/position/closed-pnl", request_params
            )
            rows.extend(_result_rows(payload))
            next_cursor = payload["result"].get("nextPageCursor")
            if next_cursor in (None, ""):
                return rows
            if not isinstance(next_cursor, str) or next_cursor == cursor:
                raise DemoExecutionError(
                    "closed_pnl_pagination", "Closed PnL pagination is ambiguous"
                )
            cursor = next_cursor
        raise DemoExecutionError(
            "closed_pnl_pagination", "Closed PnL pagination exceeded safety bound"
        )

    def _reconcile_protected_position(self, intent: ExecutionIntent) -> ExecutionIntent:
        """Re-check a protected entry until its exchange position is flat.

        A flat position proves only that the position is closed. It does not
        identify whether the exchange-side TP or SL caused the closure, so the
        local event deliberately records an unknown exit reason.
        """
        if intent.kind is not ExecutionIntentKind.ENTRY:
            raise DemoExecutionError(
                "protected_kind", "Only entry intents can hold protection"
            )
        position = self._position(intent.symbol)
        if "size" not in position:
            raise DemoExecutionError(
                "position_reconciliation", "One-way Demo Trading position size unavailable"
            )
        size = _safe_decimal(position["size"])
        if size == 0:
            return self._repository.transition(
                intent.intent_id,
                ExecutionIntentState.POSITION_CLOSED_PENDING,
                "position_closed_detected",
                "owned_position_flat; exit_reason_unknown",
            )
        if not _position_side_matches(position, intent.direction):
            return self._repository.transition(
                intent.intent_id,
                ExecutionIntentState.RECONCILIATION_REQUIRED,
                "protected_position_mismatch",
                "position_side_mismatch",
            )
        if intent.state is ExecutionIntentState.POSITION_CLOSED_PENDING:
            return self._repository.transition(
                intent.intent_id,
                ExecutionIntentState.RECONCILIATION_REQUIRED,
                "position_reopened",
                "position_size_nonzero_after_flat_confirmation",
            )
        expected_quantity = intent.filled_quantity or intent.quantity
        if (size != expected_quantity
            or _safe_decimal(position.get("takeProfit", "0")) != self._rules.normalize_entry_price(intent.take_profit, intent.direction)
            or _safe_decimal(position.get("stopLoss", "0")) != self._rules.normalize_entry_price(intent.stop_loss, intent.direction)):
            return self._unresolved(intent, "protected_position_or_tp_sl_mismatch")
        return intent

    def cancel_expired(self, now: datetime) -> int:
        if now.tzinfo is None:
            raise ValueError("Expiry timestamp must be timezone-aware")
        cancelled = 0
        for entry in self._repository.expirable_entries(now):
            if entry.symbol != self._rules.symbol:
                continue
            self._validate_namespace(entry.order_link_id)
            self._client.request(
                "POST",
                "/v5/order/cancel",
                body={
                    "category": "linear",
                    "symbol": entry.symbol,
                    "orderLinkId": entry.order_link_id,
                },
            )
            reconciled = self.reconcile(entry.intent_id)
            cancelled += int(reconciled.state is ExecutionIntentState.CANCELLED)
        return cancelled

    def _find_order(self, intent: ExecutionIntent) -> dict[str, Any]:
        params = {
            "category": "linear",
            "symbol": intent.symbol,
            "orderLinkId": intent.order_link_id,
        }
        realtime = self._client.request("GET", "/v5/order/realtime", params)
        rows = _result_rows(realtime)
        if not rows:
            history = self._client.request("GET", "/v5/order/history", params)
            rows = _result_rows(history)
        if len(rows) != 1 or rows[0].get("orderLinkId") != intent.order_link_id:
            raise DemoExecutionError(
                "order_reconciliation", "Owned Demo Trading order not uniquely resolved"
            )
        return rows[0]

    def _position(self, symbol: str) -> dict[str, Any]:
        payload = self._client.request(
            "GET",
            "/v5/position/list",
            {"category": "linear", "symbol": symbol},
        )
        rows = _result_rows(payload)
        matches = [row for row in rows if row.get("positionIdx") == 0]
        if len(matches) != 1 or (
            matches[0].get("symbol") is not None
            and matches[0].get("symbol") != symbol
        ):
            raise DemoExecutionError(
                "position_reconciliation", "One-way Demo Trading position unavailable"
            )
        return matches[0]

    def _set_and_verify_protection(self, intent: ExecutionIntent) -> None:
        if intent.take_profit is None or intent.stop_loss is None:
            raise DemoExecutionError("protection_missing", "TP and SL are required")
        take_profit = self._rules.normalize_entry_price(
            intent.take_profit, intent.direction
        )
        stop_loss = self._rules.normalize_entry_price(
            intent.stop_loss, intent.direction
        )
        self._client.request(
            "POST",
            "/v5/position/trading-stop",
            body={
                "category": "linear",
                "symbol": intent.symbol,
                "takeProfit": canonical_decimal(take_profit),
                "stopLoss": canonical_decimal(stop_loss),
                "tpslMode": "Full",
                "tpOrderType": "Market",
                "slOrderType": "Market",
                "positionIdx": 0,
            },
        )
        position = self._position(intent.symbol)
        if (
            _safe_decimal(position.get("size")) != (intent.filled_quantity or intent.quantity)
            or not _position_side_matches(position, intent.direction)
            or _safe_decimal(position.get("takeProfit", "0")) != take_profit
            or _safe_decimal(position.get("stopLoss", "0")) != stop_loss
        ):
            raise DemoExecutionError(
                "protection_verification", "Exchange-side TP or SL was not verified"
            )

    def _owned_entry(self, reservation_id: str) -> ExecutionIntent:
        entry = self._repository.find_entry(reservation_id)
        if entry is None:
            raise DemoExecutionError(
                "ownership_guard", "Owned entry intent not found"
            )
        self._validate_namespace(entry.order_link_id)
        return entry

    @staticmethod
    def _entry_payload(intent: ExecutionIntent) -> dict[str, Any]:
        if intent.price is None:
            raise ValueError("Entry intent requires a price")
        return {
            "category": "linear",
            "symbol": intent.symbol,
            "side": "Buy" if intent.direction is Direction.LONG else "Sell",
            "orderType": "Limit",
            "qty": canonical_decimal(intent.quantity),
            "price": canonical_decimal(intent.price),
            "timeInForce": "PostOnly",
            "orderLinkId": intent.order_link_id,
            "positionIdx": 0,
        }

    @staticmethod
    def _close_payload(intent: ExecutionIntent) -> dict[str, Any]:
        return {
            "category": "linear",
            "symbol": intent.symbol,
            "side": "Sell" if intent.direction is Direction.LONG else "Buy",
            "orderType": "Market",
            "qty": canonical_decimal(intent.quantity),
            "reduceOnly": True,
            "orderLinkId": intent.order_link_id,
            "positionIdx": 0,
        }

    @staticmethod
    def _validate_namespace(order_link_id: str) -> None:
        if not order_link_id.startswith(ORDER_LINK_PREFIX):
            raise DemoExecutionError(
                "ownership_guard", "Order is outside the TraderRd namespace"
            )


def _kind(action_type: RiskActionType) -> ExecutionIntentKind:
    mapping = {
        RiskActionType.CREATE_PENDING_POST_ONLY: ExecutionIntentKind.ENTRY,
        RiskActionType.CANCEL_PENDING: ExecutionIntentKind.CANCEL_ENTRY,
        RiskActionType.REQUEST_CLOSE_POSITION: ExecutionIntentKind.CLOSE_POSITION,
    }
    return mapping[action_type]


def _intent_id(command_id: str, kind: ExecutionIntentKind, reservation_id: str) -> str:
    raw = f"{command_id}:{kind.value}:{reservation_id}".encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _order_link_id(kind: ExecutionIntentKind, reservation_id: str) -> str:
    code = {
        ExecutionIntentKind.ENTRY: "e",
        ExecutionIntentKind.CANCEL_ENTRY: "x",
        ExecutionIntentKind.CLOSE_POSITION: "c",
        ExecutionIntentKind.SET_TRADING_STOP: "p",
    }[kind]
    digest = hashlib.sha256(reservation_id.encode("utf-8")).hexdigest()[:24]
    return f"{ORDER_LINK_PREFIX}{code}-{digest}"


def _result_rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
    rows = payload["result"].get("list")
    if not isinstance(rows, list) or not all(
        isinstance(row, dict) for row in rows
    ):
        raise DemoExecutionError(
            "malformed_response", "Malformed reconciliation rows"
        )
    return rows


def _position_side_matches(row: dict[str, Any], direction: Direction) -> bool:
    expected = "Buy" if direction is Direction.LONG else "Sell"
    return row.get("side") == expected


def _safe_decimal(value: Any) -> Decimal:
    try:
        parsed = Decimal(str(value))
    except InvalidOperation as exc:
        raise DemoExecutionError(
            "malformed_response", "Malformed reconciliation decimal"
        ) from exc
    if not parsed.is_finite() or parsed < 0:
        raise DemoExecutionError(
            "malformed_response", "Malformed reconciliation decimal"
        )
    return parsed


def _unique_closed_pnl_match(
    rows: list[dict[str, Any]],
    entry: ExecutionIntent,
    opened_after: datetime,
    observed_at: datetime,
) -> tuple[dict[str, Any] | None, str]:
    """Return one complete close record, never an inferred or partial result."""
    expected_side = "Sell" if entry.direction is Direction.LONG else "Buy"
    candidates: list[dict[str, Any]] = []
    incomplete = False
    for row in rows:
        if (
            row.get("symbol") != entry.symbol
            or row.get("side") != expected_side
            or row.get("execType") != "Trade"
        ):
            continue
        try:
            closed_size = _signed_decimal(row.get("closedSize"))
        except DemoExecutionError:
            incomplete = True
            continue
        if closed_size != (entry.filled_quantity or entry.quantity):
            continue
        try:
            order_id = _nonempty_text(row.get("orderId"))
            closed_at = _millisecond_timestamp(row.get("updatedTime"))
            # createdTime is required as independent timing evidence, even
            # though updatedTime is the close time retained in the ledger.
            created_at = _millisecond_timestamp(row.get("createdTime"))
            closed_pnl = _signed_decimal(row.get("closedPnl"))
            open_fee = _nonnegative_decimal(row.get("openFee"))
            close_fee = _nonnegative_decimal(row.get("closeFee"))
        except DemoExecutionError:
            incomplete = True
            continue
        if (
            created_at > closed_at
            or closed_at < opened_after
            or closed_at > observed_at
        ):
            incomplete = True
            continue
        candidates.append(
            {
                "order_id": order_id,
                "closed_at": closed_at,
                "closed_pnl": closed_pnl,
                "open_fee": open_fee,
                "close_fee": close_fee,
            }
        )
    if len(candidates) == 1:
        return candidates[0], "none"
    if len(candidates) > 1:
        return None, "ambiguous_closed_pnl"
    return None, "incomplete_closed_pnl" if incomplete else "missing_closed_pnl"


def _nonempty_text(value: Any) -> str:
    if not isinstance(value, str) or not value:
        raise DemoExecutionError("malformed_response", "Malformed closed PnL evidence")
    return value


def _millisecond_timestamp(value: Any) -> datetime:
    try:
        milliseconds = int(str(value))
    except (TypeError, ValueError) as exc:
        raise DemoExecutionError("malformed_response", "Malformed closed PnL timestamp") from exc
    if milliseconds < 0:
        raise DemoExecutionError("malformed_response", "Malformed closed PnL timestamp")
    return datetime.fromtimestamp(milliseconds / 1000, tz=timezone.utc)


def _signed_decimal(value: Any) -> Decimal:
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise DemoExecutionError("malformed_response", "Malformed closed PnL decimal") from exc
    if not parsed.is_finite():
        raise DemoExecutionError("malformed_response", "Malformed closed PnL decimal")
    return parsed


def _nonnegative_decimal(value: Any) -> Decimal:
    parsed = _signed_decimal(value)
    if parsed < 0:
        raise DemoExecutionError("malformed_response", "Malformed closed PnL fee")
    return parsed
