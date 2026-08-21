# TraderRd Maintainer Guide

## Project purpose

TraderRd observes one Telegram forum topic, parses and audits trading signals,
and can execute the approved strategy in **Bybit Demo Trading only**. The
Telegram observer is separate from the Demo signal worker and lifecycle
monitor. This separation keeps ingestion, risk admission, order submission,
and exchange reconciliation independently visible and restartable.

## Non-negotiable invariants

- Telegram source scope is exact: parent chat `2180632014`, topic `231508`,
  seed message `231906`, numeric sender `8003985182`.
- Bybit private execution uses only `https://api-demo.bybit.com`. Mainnet and
  testnet are never valid fallbacks.
- The worker sizes from full mark-to-market strategy equity
  (`walletBalance + unrealisedPnl`), with 10x isolated margin, 50% notional
  cap, approximately 1.5% target risk/trade, max 3 reservations, 5% total
  reserved risk, daily -6%, weekly -10%, and -15% drawdown circuit breaker.
- A pending entry expires after three hours and is cancelled without a market
  fallback. An inverse pending signal cancels first; an inverse filled position
  closes reduce-only and must be confirmed flat before the opposite entry.
- Exchange acknowledgement is not a fill. Fill, position, and exchange-side
  TP/SL evidence must be reconciled before risk state advances.
- Protected entries remain monitorable after TP/SL verification. A verified
  flat one-way position moves through durable `position_closed_pending` and
  then terminal `position_closed` only after one complete, uniquely attributed
  Bybit closed-PnL record is persisted and one idempotent risk close command
  releases the reservation with fresh equity. Missing, partial, or ambiguous
  closed-PnL evidence remains unresolved and reserves risk. The exit reason remains
  `exit_reason_unknown` unless concrete exchange evidence proves TP or SL;
  reopened, mismatched, or ambiguous positions fail closed.
- The worker's first applied cursor is a live boundary at the newest existing
  source-attested signal. Historical/backfill rows must never be replayed.
- Unknown, stale, unmanaged, contradictory, or incomplete state fails closed.
  Never bypass a guard by editing SQLite or creating a manual external order.

## Directory map

- `src/traderrd/domain/` — immutable signal, risk, execution, market-data, and
  simulation models.
- `src/traderrd/application/` — ingestion, risk, historical simulation, Demo
  bridge, execution, worker, and lifecycle orchestration.
- `src/traderrd/infrastructure/` — SQLite repositories, additive heartbeat
  telemetry, and Bybit adapters.
- `src/traderrd/telegram_*.py` — Telethon source inspection, backfill, and
  reconnecting observer runtime.
- `src/traderrd/cli.py`, `demo_cli.py` — command routing and safe mutation
  flags.
- `tests/` — standard-library unit tests with mocked network/exchange edges.
- `README.md` — architecture, operating runbook, safety policy, and limits.

## Runtime commands

For normal Demo-only operation, use the one visible supervised command:

```bash
./.venv/bin/traderrd demo-run
```

It starts observer, worker, and monitor in one terminal. In an interactive TTY
it opens a read-only full-screen dashboard with dominant health/risk, signal
flow, orders, per-pair performance, and hypothetical TP/SL scenarios. Use curses
color pairs only with graceful monochrome fallback; never
write raw ANSI sequences from the TUI. `q` or
`Ctrl-C` stops all children cleanly; `--no-tui` retains prefixed logs. The TUI
reads SQLite only and must never make exchange/Telegram calls or write state.
If TTY/TERM capabilities are absent, `demo-run` states the exact reason and
falls back visibly to prefixed logs. A render exception must cleanly stop all
children and be surfaced to the operator. It owns a per-database lock and must
not be run alongside the individual commands below. `traderrd tui` is a
standalone read-only observer; it does not start components. A future real
account must use a separately designed launcher and must never be enabled by
this Demo command or dashboard. `demo-run` remains Demo
Trading only; it is not a mainnet or future real-account switch.

For diagnostics, run the three visible processes in this order:

```bash
./.venv/bin/traderrd run
./.venv/bin/traderrd demo-worker --apply-demo-worker --watch --interval-seconds 30
./.venv/bin/traderrd demo-monitor --apply-demo-monitor --watch --interval-seconds 30
```

Before applied execution, use `inspect-source`, `demo-preflight --symbol
SYMBOL`, `demo-worker`, and `demo-monitor` as read-only checks. Use
`risk-report` for local risk state. Use `status` or `status --json` for a
read-only aggregate of component heartbeats, exact-source freshness, cursor
lag, intents, expiry, risk halts, and reconciliation-required work; it never
loads credentials or writes SQLite. The risk section also reports active
reserved risk, current high-watermark drawdown, and whether the killed circuit
breaker is latched. Inactive components expose their exact activation command;
degraded components expose no start command and require investigation first.
Stop the worker before the monitor and the
observer; do not stop the monitor while owned positions or orders remain
unreconciled.

## Testing

From the repository root:

```bash
./.venv/bin/python -m compileall -q src tests
PYTHONPATH=src python3 -m unittest discover -s tests -v
```

Tests must not contact Telegram or Bybit. Add focused mock tests for every
order, cursor, risk, source-isolation, and restart/idempotency change.

## Maintainer change checklist

1. Read the relevant domain/application/infrastructure boundaries before
   editing; keep Demo-only behavior explicit.
2. Preserve exact source guards, risk limits, durable cursor semantics, and
   deterministic `trd-demo-*` ownership links.
3. Add or update focused tests, including failure/ambiguous exchange states.
4. Run compile checks and the full test suite.
5. Update `README.md` and this guide when commands, invariants, or recovery
   procedures change.
6. Review the diff for secret leakage, raw response logging, mainnet/testnet
   URLs, unsafe market fallbacks, accidental database writes in dry-run paths,
   and accidental writes or credential loads in `status`.
7. Never add AI attribution to commits; use conventional commit messages if a
   commit is requested.

## Docker server operations

- `.dockerignore` must exclude credentials, local runtime state, SQLite/WAL/SHM, Telethon sessions, Git metadata, and caches; only safe environment templates may be re-included.
- `Dockerfile` and `compose.yaml` are Demo-only deployment artifacts. They must
  never include credentials, `.env.server`, session files, SQLite data, a
  mainnet endpoint, or a testnet fallback.
- The persistent `/var/lib/traderrd` volume holds both the Telegram session and
  SQLite database. Preserve it across normal service restarts and updates.
- `telegram-auth` is an interactive bootstrap profile only. It may authorize
  Telegram but must never start observer/worker/monitor or create a Bybit
  client.
- `healthcheck` must remain strictly read-only: no dotenv loading, credentials,
  network operations, schema initialization, or SQLite writes. Docker owns the
  bounded startup grace; the command itself must be strict.
- The persistent `traderrd` Compose service must run `demo-run --no-tui` with
  `restart: unless-stopped`, log rotation, and a shared state volume. Do not
  add an automatic real-account mode.

- SQLite backups and restores must use `scripts/sqlite_snapshot.py`, which uses
  SQLite's backup API and verifies integrity; never copy a live `.sqlite3` file
  directly or restore without clearing stale WAL/SHM sidecars.
- Server prerequisites must link to Docker's official Ubuntu Engine and Compose
  installation documentation; never add Docker installation scripts to this repo.
