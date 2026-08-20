from __future__ import annotations

import argparse
from decimal import Decimal, InvalidOperation
import logging
import os
from pathlib import Path

from traderrd.config import ConfigurationError, load_config, load_dotenv
from traderrd.infrastructure.sqlite_repository import SQLiteSignalRepository


def positive_integer(raw: str) -> int:
    try:
        value = int(raw)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be a positive integer") from exc
    if value <= 0:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return value


def positive_float(raw: str) -> float:
    try:
        value = float(raw)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be a positive number") from exc
    if value <= 0:
        raise argparse.ArgumentTypeError("must be a positive number")
    return value


def nonzero_integer(raw: str) -> int:
    try:
        value = int(raw)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be a non-zero integer") from exc
    if value == 0:
        raise argparse.ArgumentTypeError("must be a non-zero integer")
    return value


def cost_rate(raw: str) -> Decimal:
    try:
        value = Decimal(raw)
    except InvalidOperation as exc:
        raise argparse.ArgumentTypeError("must be a decimal from 0 to less than 1") from exc
    if not value.is_finite() or value < 0 or value >= 1:
        raise argparse.ArgumentTypeError("must be a decimal from 0 to less than 1")
    return value


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="traderrd", description="Observe and audit Telegram trading signals"
    )
    parser.add_argument(
        "command",
        choices=(
            "init-db",
            "run",
            "backfill",
            "inspect-source",
            "reprocess-audit",
            "simulate-history",
            "risk-replay",
            "risk-report",
            "demo-preflight",
            "demo-bridge",
            "demo-execute",
            "demo-reconcile",
            "demo-monitor",
            "demo-worker",
            "demo-run",
            "tui",
            "status",
            "healthcheck",
            "telegram-auth",
            "testnet-preflight",
            "testnet-execute",
            "testnet-reconcile",
        ),
        help=(
            "Action to perform; testnet-* names are retired aliases that "
            "always fail with migration guidance"
        ),
    )
    parser.add_argument("--env-file", default=".env", help="Environment file path")
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit stable status JSON v1 (status only)",
    )
    parser.add_argument(
        "--no-tui",
        action="store_true",
        help="Use prefixed logs instead of the full-screen dashboard (demo-run only)",
    )
    parser.add_argument(
        "--no-color",
        action="store_true",
        help="Disable ANSI color in the human status view (status only)",
    )
    parser.add_argument(
        "--limit",
        type=positive_integer,
        help=(
            "Maximum records to process (backfill, reprocess-audit, or "
            "simulate-history; default: all)"
        ),
    )
    parser.add_argument(
        "--database-path",
        default=None,
        help=(
            "SQLite path (reprocess, simulation, risk, status, or Demo Trading; "
            "default: data/traderrd.sqlite3)"
        ),
    )
    parser.add_argument(
        "--fetch-public-data",
        action="store_true",
        help="Allow simulate-history to request public Bybit kline data",
    )
    parser.add_argument(
        "--source-chat-id",
        type=positive_integer,
        default=2180632014,
        help="Validated Telegram parent chat ID (simulate-history only)",
    )
    parser.add_argument(
        "--source-topic-id",
        type=positive_integer,
        default=231508,
        help="Validated Telegram topic ID (simulate-history only)",
    )
    parser.add_argument(
        "--exit-horizon-hours",
        type=positive_integer,
        default=24,
        help="Post-entry evaluation horizon in hours (simulate-history only)",
    )
    parser.add_argument(
        "--bybit-base-url",
        default="https://api.bybit.com",
        help="Public Bybit HTTPS base URL (simulate-history only)",
    )
    parser.add_argument(
        "--bybit-timeout-seconds",
        type=positive_float,
        default=5.0,
        help="Public Bybit request timeout (simulate-history only)",
    )
    parser.add_argument(
        "--commands-file",
        default=None,
        help="JSON Lines risk command file (risk-replay only)",
    )
    parser.add_argument(
        "--apply-risk-state",
        action="store_true",
        help="Persist risk-replay decisions; never executes exchange actions",
    )
    parser.add_argument(
        "--symbol",
        default=None,
        help="Uppercase linear symbol for a Demo Trading command",
    )
    parser.add_argument(
        "--risk-command-id",
        default=None,
        help="One audited risk command to adapt (demo-execute only)",
    )
    parser.add_argument(
        "--submit-demo",
        action="store_true",
        help="Explicitly permit demo-execute private mutation requests",
    )
    parser.add_argument(
        "--intent-id",
        default=None,
        help="Stored execution intent to reconcile (demo-reconcile only)",
    )
    parser.add_argument(
        "--apply-demo-reconciliation",
        action="store_true",
        help="Permit private reconciliation and TP/SL setup on Demo Trading",
    )
    parser.add_argument(
        "--apply-demo-monitor",
        action="store_true",
        help="Permit the Demo lifecycle monitor to reconcile and cancel owned intents",
    )
    parser.add_argument(
        "--apply-demo-worker",
        action="store_true",
        help="Permit the fresh-signal worker to persist risk and submit Demo orders",
    )
    parser.add_argument(
        "--watch",
        action="store_true",
        help="Keep the Demo monitor or fresh-signal worker running until interrupted",
    )
    parser.add_argument(
        "--interval-seconds",
        type=positive_float,
        default=30.0,
        help="Seconds between Demo monitor/worker cycles when --watch is set",
    )
    parser.add_argument(
        "--message-id",
        type=positive_integer,
        help="Stored Telegram message ID to evaluate (demo-bridge only)",
    )
    parser.add_argument(
        "--source-sender-id",
        type=nonzero_integer,
        help="Expected numeric Telegram sender ID (demo-bridge only)",
    )
    parser.add_argument(
        "--persist-risk",
        action="store_true",
        help="Persist the supervised risk proposal and Demo intent plan",
    )
    parser.add_argument(
        "--estimated-cost-rate",
        type=cost_rate,
        default=Decimal("0.001"),
        help="Explicit estimated round-trip cost rate (default: 0.001)",
    )
    parser.add_argument(
        "--submit-testnet",
        action="store_true",
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--apply-testnet-reconciliation",
        action="store_true",
        help=argparse.SUPPRESS,
    )
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    retired_testnet_commands = {
        "testnet-preflight",
        "testnet-execute",
        "testnet-reconcile",
    }
    if args.command in retired_testnet_commands:
        parser.error(
            "testnet commands are retired; use demo-preflight, demo-execute, "
            "or demo-reconcile with dedicated BYBIT_DEMO_* credentials"
        )
    if args.submit_testnet or args.apply_testnet_reconciliation:
        parser.error(
            "testnet flags are retired; use --submit-demo or "
            "--apply-demo-reconciliation with the matching demo command"
        )
    simulation_only = (
        args.fetch_public_data
        or args.source_chat_id != 2180632014
        or args.source_topic_id != 231508
        or args.exit_horizon_hours != 24
        or args.bybit_base_url != "https://api.bybit.com"
        or args.bybit_timeout_seconds != 5.0
    )
    if args.command not in {"backfill", "reprocess-audit", "simulate-history"} and (
        args.limit is not None
    ):
        parser.error(
            "--limit is only valid with backfill, reprocess-audit, or simulate-history"
        )
    if args.command != "simulate-history" and simulation_only:
        parser.error("historical simulation options require simulate-history")
    if args.command == "simulate-history" and (
        args.source_chat_id != 2180632014 or args.source_topic_id != 231508
    ):
        parser.error(
            "simulate-history is restricted to validated chat 2180632014 "
            "and topic 231508"
        )
    if args.command != "risk-replay" and (
        args.commands_file is not None or args.apply_risk_state
    ):
        parser.error("risk replay options require risk-replay")
    if args.command == "risk-replay" and args.commands_file is None:
        parser.error("risk-replay requires --commands-file")
    if args.command in {
        "risk-replay",
        "risk-report",
    } and args.env_file != ".env":
        parser.error("risk commands do not load --env-file")
    if args.command not in {
        "reprocess-audit",
        "simulate-history",
        "risk-replay",
        "risk-report",
        "demo-bridge",
        "demo-execute",
        "demo-reconcile",
        "demo-monitor",
        "demo-worker",
        "demo-run",
        "tui",
        "status",
        "healthcheck",
    } and args.database_path is not None:
        parser.error(
            "--database-path is only valid with reprocess-audit, "
            "simulate-history, risk-replay, risk-report, demo-execute, "
            "demo-bridge, demo-reconcile, demo-monitor, demo-worker, status, or healthcheck"
        )
    if args.command != "demo-run" and args.no_tui:
        parser.error("--no-tui is only valid with demo-run")
    if args.command != "status" and args.json:
        parser.error("--json is only valid with status")
    if args.command != "status" and args.no_color:
        parser.error("--no-color is only valid with status")
    if args.json and args.no_color:
        parser.error("--no-color cannot be used with --json")
    if args.command == "healthcheck":
        from traderrd.health_cli import run_healthcheck

        database_path = args.database_path or "data/traderrd.sqlite3"
        return run_healthcheck(database_path)
    if args.command == "telegram-auth":
        if args.database_path is not None:
            parser.error("--database-path is not valid with telegram-auth")
        try:
            config = load_config(args.env_file)
        except ConfigurationError as exc:
            raise SystemExit(f"Configuration error: {exc}") from exc
        from traderrd.telegram_auth import run_telegram_auth

        return run_telegram_auth(config)

    if args.command in {"risk-replay", "risk-report"}:
        from traderrd.risk_cli import run_risk_replay, run_risk_report

        database_path = args.database_path or "data/traderrd.sqlite3"
        if args.command == "risk-report":
            return run_risk_report(database_path)
        return run_risk_replay(
            args.commands_file,
            database_path,
            args.apply_risk_state,
        )
    demo_commands = {
        "demo-preflight",
        "demo-bridge",
        "demo-execute",
        "demo-reconcile",
        "demo-monitor",
        "demo-worker",
        "demo-run",
    }
    demo_options = (
        args.symbol is not None
        or args.risk_command_id is not None
        or args.submit_demo
        or args.intent_id is not None
        or args.apply_demo_reconciliation
        or args.apply_demo_monitor
        or args.apply_demo_worker
        or args.watch
        or args.interval_seconds != 30.0
        or args.message_id is not None
        or args.source_sender_id is not None
        or args.persist_risk
        or args.estimated_cost_rate != Decimal("0.001")
    )
    if args.command not in demo_commands and demo_options:
        parser.error("Demo Trading options require a demo command")
    if args.command in demo_commands:
        from traderrd.demo_cli import (
            run_demo_execute,
            run_demo_preflight,
            run_demo_reconcile,
            run_demo_monitor,
            run_demo_runtime,
            run_demo_worker,
        )

        bridge_options = (
            args.message_id is not None
            or args.source_sender_id is not None
            or args.persist_risk
            or args.estimated_cost_rate != Decimal("0.001")
        )
        if args.command != "demo-bridge" and bridge_options:
            parser.error("bridge options require demo-bridge")

        monitor_options = (
            args.apply_demo_monitor
            or args.watch
            or args.interval_seconds != 30.0
        )
        worker_options = args.apply_demo_worker
        if args.command not in {"demo-monitor", "demo-worker", "demo-run"} and monitor_options:
            parser.error("monitor options require demo-monitor or demo-worker")
        if args.command != "demo-worker" and worker_options:
            parser.error("--apply-demo-worker requires demo-worker")

        if args.command == "demo-run":
            if args.watch:
                parser.error("demo-run always watches; omit --watch")
            if args.apply_demo_monitor or args.apply_demo_worker:
                parser.error("demo-run applies the Demo-only worker and monitor itself")
            if any(
                value is not None
                for value in (args.symbol, args.risk_command_id, args.intent_id)
            ) or args.submit_demo or args.apply_demo_reconciliation or bridge_options:
                parser.error("execution and bridge options do not apply to demo-run")
            database_path = args.database_path or "data/traderrd.sqlite3"
            if args.no_tui:
                return run_demo_runtime(
                    database_path, args.env_file, args.interval_seconds, use_tui=False
                )
            return run_demo_runtime(database_path, args.env_file, args.interval_seconds)

        if args.command == "demo-bridge":
            if args.message_id is None or args.source_sender_id is None:
                parser.error("demo-bridge requires --message-id and --source-sender-id")
            if (
                args.symbol
                or args.risk_command_id
                or args.submit_demo
                or args.intent_id
                or args.apply_demo_reconciliation
            ):
                parser.error("execution and reconciliation options do not apply to demo-bridge")
            from traderrd.demo_bridge_cli import run_demo_bridge

            database_path = args.database_path or "data/traderrd.sqlite3"
            return run_demo_bridge(
                database_path,
                args.message_id,
                args.source_sender_id,
                args.persist_risk,
                args.estimated_cost_rate,
                args.env_file,
            )

        if args.command == "demo-monitor":
            if any(
                value is not None
                for value in (args.symbol, args.risk_command_id, args.intent_id)
            ) or args.submit_demo or args.apply_demo_reconciliation:
                parser.error("execution options do not apply to demo-monitor")
            database_path = args.database_path or "data/traderrd.sqlite3"
            return run_demo_monitor(
                database_path,
                args.apply_demo_monitor,
                args.watch,
                args.interval_seconds,
                args.env_file,
            )

        if args.command == "demo-worker":
            if any(
                value is not None
                for value in (args.symbol, args.risk_command_id, args.intent_id)
            ) or args.submit_demo or args.apply_demo_reconciliation or args.apply_demo_monitor:
                parser.error("execution and reconciliation options do not apply to demo-worker")
            database_path = args.database_path or "data/traderrd.sqlite3"
            return run_demo_worker(
                database_path,
                args.apply_demo_worker,
                args.watch,
                args.interval_seconds,
                args.env_file,
            )

        if args.command != "demo-reconcile" and not args.symbol:
            parser.error("demo-preflight and demo-execute require --symbol")
        if args.command == "demo-preflight":
            if args.submit_demo or args.apply_demo_reconciliation:
                parser.error("demo-preflight is read-only")
            return run_demo_preflight(args.symbol, args.env_file)
        database_path = args.database_path or "data/traderrd.sqlite3"
        if args.command == "demo-execute":
            if args.apply_demo_reconciliation or args.intent_id:
                parser.error("reconciliation options require demo-reconcile")
            return run_demo_execute(
                database_path,
                args.symbol,
                args.risk_command_id,
                args.submit_demo,
                args.env_file,
            )
        if not args.intent_id:
            parser.error("demo-reconcile requires --intent-id")
        if args.submit_demo or args.risk_command_id or args.symbol:
            parser.error("execution options require demo-execute")
        return run_demo_reconcile(
            database_path,
            args.intent_id,
            args.apply_demo_reconciliation,
            args.env_file,
        )
    if args.command == "reprocess-audit":
        from traderrd.audit_reprocess import run_audit_reprocessing

        database_path = args.database_path or "data/traderrd.sqlite3"
        return run_audit_reprocessing(database_path, limit=args.limit)
    if args.command == "simulate-history":
        from traderrd.historical_simulation_cli import run_historical_simulation

        database_path = args.database_path or "data/traderrd.sqlite3"
        try:
            return run_historical_simulation(
                database_path=database_path,
                source_chat_id=args.source_chat_id,
                source_topic_id=args.source_topic_id,
                exit_horizon_hours=args.exit_horizon_hours,
                limit=args.limit,
                fetch_public_data=args.fetch_public_data,
                bybit_base_url=args.bybit_base_url,
                bybit_timeout_seconds=args.bybit_timeout_seconds,
            )
        except KeyboardInterrupt:
            print("Historical simulation interrupted")
            return 130
    if args.command == "tui":
        from traderrd.tui import run_tui

        database_path = args.database_path or "data/traderrd.sqlite3"
        return run_tui(database_path)

    if args.command == "status":
        from traderrd.status_cli import run_status

        database_path = args.database_path or "data/traderrd.sqlite3"
        return run_status(database_path, args.json, args.no_color)
    if args.command == "init-db":
        load_dotenv(args.env_file)
        database_path = Path(os.getenv("TRADERRD_DB_PATH", "data/traderrd.sqlite3"))
        SQLiteSignalRepository(database_path).initialize()
        print(f"Database initialized at {database_path}")
        return 0

    try:
        config = load_config(args.env_file)
    except ConfigurationError as exc:
        raise SystemExit(f"Configuration error: {exc}") from exc

    logging.basicConfig(
        level=getattr(logging, config.log_level, logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    if args.command == "backfill":
        from traderrd.telegram_backfill import run_backfill

        return run_backfill(config, limit=args.limit)
    if args.command == "inspect-source":
        from traderrd.telegram_preflight import run_source_inspection

        return run_source_inspection(config)

    from traderrd.telegram_listener import run

    return run(config)


if __name__ == "__main__":
    raise SystemExit(main())
