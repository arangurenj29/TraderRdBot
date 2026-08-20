from contextlib import closing
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path
import sqlite3
import tempfile
import unittest
from zoneinfo import ZoneInfo

from traderrd.application.risk_engine import RiskEngineService
from traderrd.domain.models import Direction
from traderrd.domain.risk import (
    ApplyEquitySnapshot,
    ConfirmPendingFill,
    ConfirmPositionClosed,
    EngineMode,
    InitializeRiskEngine,
    ManualRearm,
    PortfolioRiskEngine,
    ProposeRiskReservation,
    ReservationStatus,
    RiskActionType,
    TradeProposal,
)
from traderrd.infrastructure.risk_repository import (
    IdempotencyConflict,
    SQLiteRiskStateRepository,
)


LIMA = ZoneInfo("America/Lima")
START = datetime(2026, 8, 17, 9, 0, tzinfo=LIMA)


def proposal(
    signal_id: str,
    symbol: str = "BTCUSDT",
    direction: Direction = Direction.LONG,
    stop_percent: str = "0.03",
) -> TradeProposal:
    entry = Decimal("100")
    fraction = Decimal(stop_percent)
    if direction is Direction.LONG:
        take_profit = Decimal("100.8")
        stop_loss = entry * (Decimal("1") - fraction)
    else:
        take_profit = Decimal("99.2")
        stop_loss = entry * (Decimal("1") + fraction)
    return TradeProposal(
        signal_id=signal_id,
        symbol=symbol,
        direction=direction,
        entry=entry,
        take_profit=take_profit,
        stop_loss=stop_loss,
    )


class PortfolioRiskEngineTests(unittest.TestCase):
    def setUp(self) -> None:
        self.engine = PortfolioRiskEngine()
        initialized = self.engine.process(
            None,
            InitializeRiskEngine("init", START, Decimal("1000")),
        )
        self.state = initialized.state

    def propose(
        self,
        signal_id: str,
        at: datetime,
        equity: str = "1000",
        symbol: str = "BTCUSDT",
        direction: Direction = Direction.LONG,
        stop_percent: str = "0.03",
    ):
        return self.engine.process(
            self.state,
            ProposeRiskReservation(
                f"propose-{signal_id}",
                at,
                Decimal(equity),
                proposal(signal_id, symbol, direction, stop_percent),
            ),
        )

    def test_precise_dynamic_sizing_includes_estimated_costs(self) -> None:
        transition = self.propose("signal-1", START + timedelta(minutes=1))

        reservation = self.state.reservations["signal-1"]
        expected_notional = Decimal("15") / Decimal("0.031")
        self.assertEqual(transition.decision.status, "accepted")
        self.assertEqual(reservation.notional, expected_notional)
        self.assertEqual(reservation.reserved_risk, Decimal("15"))
        self.assertEqual(reservation.isolated_margin, expected_notional / Decimal("10"))
        self.assertEqual(
            reservation.estimated_cost,
            expected_notional * Decimal("0.001"),
        )
        self.assertEqual(reservation.quantity, expected_notional / Decimal("100"))

    def test_notional_is_capped_at_half_equity_for_tight_stop(self) -> None:
        self.propose(
            "signal-1",
            START + timedelta(minutes=1),
            stop_percent="0.01",
        )

        reservation = self.state.reservations["signal-1"]
        self.assertEqual(reservation.notional, Decimal("500"))
        self.assertEqual(reservation.reserved_risk, Decimal("5.5"))

    def test_pending_and_filled_reservations_share_three_slot_limit(self) -> None:
        for index, symbol in enumerate(("BTCUSDT", "ETHUSDT", "XRPUSDT"), start=1):
            self.propose(
                f"signal-{index}",
                START + timedelta(minutes=index),
                symbol=symbol,
            )
        self.engine.process(
            self.state,
            ConfirmPendingFill(
                "fill-1", START + timedelta(minutes=4), "signal-1"
            ),
        )

        fourth = self.propose(
            "signal-4",
            START + timedelta(minutes=5),
            symbol="SOLUSDT",
        )

        self.assertEqual(fourth.decision.status, "rejected")
        self.assertEqual(fourth.decision.reason, "max_reservations")
        self.assertEqual(len(self.state.active_reservations), 3)
        self.assertEqual(self.state.total_reserved_risk, Decimal("45"))

    def test_total_reserved_risk_blocks_before_five_percent(self) -> None:
        self.propose("one", START + timedelta(minutes=1), symbol="BTCUSDT")
        self.propose("two", START + timedelta(minutes=2), symbol="ETHUSDT")
        self.engine.process(
            self.state,
            ConfirmPendingFill("fill-one", START + timedelta(minutes=3), "one"),
        )
        self.engine.process(
            self.state,
            ConfirmPendingFill("fill-two", START + timedelta(minutes=4), "two"),
        )
        next_week = START + timedelta(days=7)
        self.engine.process(
            self.state,
            ApplyEquitySnapshot("mark", next_week, Decimal("851")),
        )

        decision = self.propose(
            "three",
            next_week + timedelta(minutes=1),
            equity="851",
            symbol="SOLUSDT",
        ).decision

        self.assertEqual(decision.status, "rejected")
        self.assertEqual(decision.reason, "max_total_reserved_risk")

    def test_projected_daily_loss_blocks_before_six_percent_boundary(self) -> None:
        snapshot = self.engine.process(
            self.state,
            ApplyEquitySnapshot(
                "snapshot", START + timedelta(minutes=1), Decimal("954")
            ),
        )
        self.assertEqual(snapshot.state.mode, EngineMode.ACTIVE)

        decision = self.propose(
            "signal-1",
            START + timedelta(minutes=2),
            equity="954",
        ).decision

        self.assertEqual(decision.status, "rejected")
        self.assertEqual(decision.reason, "projected_daily_loss_limit")

    def test_projected_weekly_and_drawdown_limits_are_checked(self) -> None:
        next_monday = START + timedelta(days=7)
        self.engine.process(
            self.state,
            ApplyEquitySnapshot("new-week", next_monday, Decimal("860")),
        )

        decision = self.propose(
            "signal-1",
            next_monday + timedelta(minutes=1),
            equity="860",
        ).decision

        self.assertEqual(decision.status, "rejected")
        self.assertEqual(decision.reason, "projected_drawdown_limit")

    def test_daily_trip_cancels_pending_but_keeps_filled_hard_exits(self) -> None:
        self.propose("pending", START + timedelta(minutes=1), symbol="BTCUSDT")
        self.propose("filled", START + timedelta(minutes=2), symbol="ETHUSDT")
        self.engine.process(
            self.state,
            ConfirmPendingFill("fill", START + timedelta(minutes=3), "filled"),
        )

        transition = self.engine.process(
            self.state,
            ApplyEquitySnapshot("loss", START + timedelta(minutes=4), Decimal("940")),
        )

        self.assertEqual(self.state.mode, EngineMode.ENTRY_PAUSED)
        self.assertEqual(
            self.state.reservations["pending"].status,
            ReservationStatus.CANCELLED,
        )
        self.assertEqual(
            self.state.reservations["filled"].status,
            ReservationStatus.FILLED,
        )
        self.assertEqual(
            [action.action_type for action in transition.decision.actions],
            [RiskActionType.CANCEL_PENDING],
        )

    def test_lima_period_resets_release_only_expired_halts(self) -> None:
        monday = START
        self.engine.process(
            self.state,
            ApplyEquitySnapshot("weekly-loss", monday, Decimal("900")),
        )
        self.assertTrue(self.state.weekly_halted)

        next_day = monday + timedelta(days=1)
        self.engine.process(
            self.state,
            ApplyEquitySnapshot("next-day", next_day, Decimal("900")),
        )
        self.assertEqual(self.state.mode, EngineMode.ENTRY_PAUSED)
        self.assertFalse(self.state.daily_halted)
        self.assertTrue(self.state.weekly_halted)

        next_week = monday + timedelta(days=7)
        self.engine.process(
            self.state,
            ApplyEquitySnapshot("next-week", next_week, Decimal("900")),
        )
        self.assertEqual(self.state.mode, EngineMode.ACTIVE)
        self.assertFalse(self.state.weekly_halted)

    def test_three_hour_expiry_releases_reservation_across_signal_gap(self) -> None:
        self.propose("old", START + timedelta(minutes=1))

        transition = self.propose(
            "new",
            START + timedelta(hours=3, minutes=2),
            symbol="ETHUSDT",
        )

        self.assertEqual(
            self.state.reservations["old"].status,
            ReservationStatus.EXPIRED,
        )
        self.assertEqual(
            self.state.reservations["new"].status,
            ReservationStatus.PENDING,
        )
        self.assertEqual(
            transition.decision.actions[0].action_type,
            RiskActionType.CANCEL_PENDING,
        )

    def test_original_signal_receipt_can_shorten_pending_entry_window(self) -> None:
        expires_at = START + timedelta(hours=1)
        custom = proposal("aged-signal")
        custom = TradeProposal(
            signal_id=custom.signal_id,
            symbol=custom.symbol,
            direction=custom.direction,
            entry=custom.entry,
            take_profit=custom.take_profit,
            stop_loss=custom.stop_loss,
            entry_expires_at=expires_at,
        )

        transition = self.engine.process(
            self.state,
            ProposeRiskReservation(
                "aged-proposal",
                START + timedelta(minutes=30),
                Decimal("1000"),
                custom,
            ),
        )

        self.assertEqual(transition.decision.status, "accepted")
        self.assertEqual(
            self.state.reservations["aged-signal"].expires_at,
            expires_at,
        )

    def test_inverse_pending_is_cancelled_before_new_reservation(self) -> None:
        self.propose("long", START + timedelta(minutes=1))

        transition = self.propose(
            "short",
            START + timedelta(minutes=2),
            direction=Direction.SHORT,
        )

        self.assertEqual(
            self.state.reservations["long"].status,
            ReservationStatus.CANCELLED,
        )
        self.assertEqual(
            self.state.reservations["short"].status,
            ReservationStatus.PENDING,
        )
        self.assertEqual(
            [action.action_type for action in transition.decision.actions],
            [
                RiskActionType.CANCEL_PENDING,
                RiskActionType.CREATE_PENDING_POST_ONLY,
            ],
        )

    def test_filled_inverse_requires_close_confirmation_before_new_entry(self) -> None:
        self.propose("long", START + timedelta(minutes=1))
        self.engine.process(
            self.state,
            ConfirmPendingFill("fill", START + timedelta(minutes=2), "long"),
        )

        staged = self.propose(
            "short",
            START + timedelta(minutes=3),
            direction=Direction.SHORT,
        )

        self.assertEqual(staged.decision.status, "staged")
        self.assertNotIn("short", self.state.reservations)
        self.assertEqual(
            self.state.reservations["long"].status,
            ReservationStatus.CLOSE_REQUESTED,
        )

        confirmed = self.engine.process(
            self.state,
            ConfirmPositionClosed(
                "close", START + timedelta(minutes=4), "long", Decimal("1000")
            ),
        )
        self.assertEqual(confirmed.decision.status, "accepted")
        self.assertEqual(
            self.state.reservations["long"].status,
            ReservationStatus.CLOSED,
        )
        self.assertEqual(
            self.state.reservations["short"].status,
            ReservationStatus.PENDING,
        )

    def test_drawdown_trip_closes_filled_and_never_auto_rearms(self) -> None:
        self.propose("pending", START + timedelta(minutes=1), symbol="BTCUSDT")
        self.propose("filled", START + timedelta(minutes=2), symbol="ETHUSDT")
        self.engine.process(
            self.state,
            ConfirmPendingFill("fill", START + timedelta(minutes=3), "filled"),
        )

        killed = self.engine.process(
            self.state,
            ApplyEquitySnapshot("kill", START + timedelta(minutes=4), Decimal("850")),
        )

        self.assertEqual(self.state.mode, EngineMode.KILLED)
        self.assertEqual(
            self.state.reservations["filled"].status,
            ReservationStatus.CLOSE_REQUESTED,
        )
        self.assertEqual(len(killed.decision.actions), 2)

        self.engine.process(
            self.state,
            ApplyEquitySnapshot(
                "recovery", START + timedelta(minutes=5), Decimal("1100")
            ),
        )
        self.assertEqual(self.state.mode, EngineMode.KILLED)

        blocked_rearm = self.engine.process(
            self.state,
            ManualRearm(
                "early-rearm",
                START + timedelta(minutes=6),
                Decimal("1100"),
                "operator-ticket",
            ),
        )
        self.assertEqual(blocked_rearm.decision.status, "rejected")

        self.engine.process(
            self.state,
            ConfirmPositionClosed(
                "closed",
                START + timedelta(minutes=7),
                "filled",
                Decimal("1100"),
            ),
        )
        rearmed = self.engine.process(
            self.state,
            ManualRearm(
                "manual-rearm",
                START + timedelta(minutes=8),
                Decimal("1100"),
                "operator-ticket",
            ),
        )
        self.assertEqual(rearmed.decision.status, "accepted")
        self.assertEqual(self.state.mode, EngineMode.ACTIVE)
        self.assertEqual(self.state.high_watermark, Decimal("1100"))


class RiskRepositoryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.database_path = Path(self.directory.name) / "risk.sqlite3"
        self.repository = SQLiteRiskStateRepository(self.database_path)
        self.repository.initialize()
        self.service = RiskEngineService(self.repository)

    def tearDown(self) -> None:
        self.directory.cleanup()

    def test_duplicate_command_is_idempotent_and_audited_once(self) -> None:
        command = InitializeRiskEngine("init", START, Decimal("1000"))

        first = self.service.execute(command)
        second = self.service.execute(command)

        self.assertFalse(first.duplicate_command)
        self.assertTrue(second.duplicate_command)
        commands, events, revision = self.repository.counts()
        self.assertEqual((commands, events, revision), (1, 1, 1))

    def test_reused_command_id_with_different_input_is_rejected(self) -> None:
        self.service.execute(InitializeRiskEngine("init", START, Decimal("1000")))

        with self.assertRaises(IdempotencyConflict):
            self.service.execute(InitializeRiskEngine("init", START, Decimal("999")))

    def test_state_and_transition_audit_survive_repository_reload(self) -> None:
        self.service.execute(InitializeRiskEngine("init", START, Decimal("1000")))
        result = self.service.execute(
            ProposeRiskReservation(
                "propose",
                START + timedelta(minutes=1),
                Decimal("1000"),
                proposal("signal-1"),
            )
        )

        reloaded = SQLiteRiskStateRepository(self.database_path).load_state()

        self.assertEqual(result.decision.status, "accepted")
        self.assertIsNotNone(reloaded)
        self.assertEqual(reloaded.total_reserved_risk, Decimal("15"))
        with closing(sqlite3.connect(self.database_path)) as connection:
            command_count = connection.execute(
                "SELECT COUNT(*) FROM risk_engine_commands"
            ).fetchone()[0]
            event_count = connection.execute(
                "SELECT COUNT(*) FROM risk_engine_events"
            ).fetchone()[0]
        self.assertEqual(command_count, 2)
        self.assertEqual(event_count, 2)


if __name__ == "__main__":
    unittest.main()
