# TraderRd Observer

TraderRd Observer is a deliberately small, local-only MVP. It reads LONG and
SHORT signal messages from one Telegram forum topic through the authenticated
user's MTProto session, validates their risk parameters, deduplicates them, and
keeps an SQLite audit trail. For each newly stored signal, it also records a public
Bybit V5 USDT-linear ticker snapshot with best bid/ask, last price, and mark
price when available.

The Telegram observer itself **never places orders** and never uses private
Bybit endpoints or Bybit credentials. An explicit, separate Bybit Demo worker
can consume only new source-attested signals after the observer stores them.
The public quote snapshots are evidence for later analysis; they are not
execution prices, fills, or completed paper-trade outcomes.

## Architecture

- `domain`: immutable signal models, parsing rules, and financial validation.
- `application`: ingestion orchestration and explicit outcomes.
- `infrastructure`: SQLite persistence plus Telethon and public Bybit adapters.
- `cli`: database initialization and observer startup.

Every inbound event is retained in `inbound_revisions`. The corresponding
Telegram message has one current row in `inbound_messages`, keyed by chat and
message ID. Valid signals live separately in `signals` and are deduplicated by
a deterministic content fingerprint.
Successful and failed Bybit quote attempts are stored separately in
`market_quote_captures`, so market-data outages never remove an audited signal.

## Requirements

- Python 3.11 or newer
- A Telegram account that already belongs to the source chat
- Telegram `api_id` and `api_hash` from <https://my.telegram.org>

## Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -e .
cp .env.example .env
```

Replace the credential placeholders in `.env` locally. Never commit or share
the `.env` file, login codes, API hash, or generated `.session` files.

Initialize the database:

```bash
traderrd init-db
```

Run the observer from an interactive terminal:

```bash
traderrd run
```

On the first run, Telethon asks for account authorization in that terminal.
The observer resolves chat `2180632014` from the authenticated account's
dialogs and then accepts only forum topic `231508`. The configured seed message
is `231906`, derived from the private topic link structure. It does not guess or
require a public username. General-group messages and messages from every other
topic are rejected before parsing or persistence.

Telethon automatically retries short interruptions. If the connection still
fails, the observer restarts the Telegram client with bounded exponential
backoff (capped at 60 seconds). Network permission errors stop immediately with
an actionable message; source-isolation and configuration failures are never
retried automatically.

Validate the configured source before starting a listener or backfill:

```bash
./.venv/bin/traderrd inspect-source
```

Inspection requires an already authorized local session but never starts an
interactive login. It loads the configured seed message, verifies the numeric
chat/topic/message coordinates, tests parser eligibility, and prints metadata
only. It never prints message text, API credentials, session content, phone
numbers, or authorization codes. The report also exposes the numeric sender
peer ID when Telegram provides one. To pin the optional second guard, copy that
numeric value into `TELEGRAM_EXPECTED_SENDER_ID` and run inspection again.
Even when the variable is absent, the listener and backfill derive the numeric
sender from the validated seed for the current process and use it as a second
guard when available. Setting the variable additionally pins that identity and
makes a changed seed sender fail validation. Sender display names are never
trusted for source isolation.

The observer uses Bybit's public `GET /v5/market/tickers` endpoint with
`category=linear`. Requests have a bounded timeout, require no Bybit account,
and run outside the Telegram event loop. Restart the observer after upgrading
so the running process loads quote capture and creates the additive table.

## Historical backfill

Backfill imports only the configured Telegram topic from oldest to newest
through the same parser and SQLite audit path as live messages. It requests the
topic explicitly and independently validates every returned message before
parsing or persistence. It requires an already authorized local Telethon
session and never starts an interactive login.
Backfill enforces topic membership before inspecting content. An explicit topic
mismatch stops without advancing and directs the operator to run source
inspection. Within the correct topic, service metadata and parser-ineligible
comments are skipped without persistence. The numeric sender guard becomes a
hard blocker only after a body parses as a valid signal; a valid signal from a
different sender stops without advancing.
Topic creation metadata and other non-operational service messages are skipped
without persistence and advance the topic checkpoint normally. Source checks
remain strict for all parser-valid signals.
Older Telegram records may omit the `forum_topic` flag while still carrying an
explicit topic ID in their reply header; that numeric relationship is accepted,
while an explicit different topic ID remains rejected.

For a bounded first run:

```bash
traderrd backfill --limit 500
```

To scan all available history:

```bash
traderrd backfill
```

The unlimited form can take a long time for a large chat. Progress and final
output contain aggregate counters only, never raw message content. Re-running
either command is safe. The backfill stores a monotonic per-chat-and-topic checkpoint
after every handled message, including non-text messages, and the next run
requests only later message IDs. Telegram message IDs and signal fingerprints
remain the final idempotency constraints. If upgrading from a version without
checkpoints, the first upgraded run may re-scan the already audited prefix
once; later runs resume from the saved position.

Rows recorded by older parent-chat-only versions remain untouched as audit
history. The source-isolation upgrade prevents any new general-group or
other-topic message from entering the database; it does not destructively
rewrite existing SQLite data.

Stop the live observer before backfill so both processes do not share one
Telethon session concurrently, then restart `traderrd run` afterward. Backfill
automatically waits for Telegram flood waits up to 10 seconds; a longer wait or
transient RPC/connection failure stops safely with partial counters so the
same command can resume later.

Historical import makes **zero Bybit requests**. A current ticker snapshot
would be invalid evidence for an old signal. Historical outcome simulation is
a separate explicit command that uses timestamp-aligned public Bybit candles.

Ordinary channel posts and older signal formats that do not contain the
verified header, pair/timeframe, declared percentages, prices, and embedded
timestamp are counted as `skipped_non_signal` by backfill, reported through
metadata-only listener logs, and are not persisted. Existing `invalid` audit
history from older versions remains untouched. Incompatible posts are never
coerced into the current strategy schema.

### Reprocess existing audit rows

After a parser upgrade, re-evaluate existing `invalid` rows and repair legacy
rows whose canonical outcome was overwritten by an older duplicate replay:

```bash
./.venv/bin/traderrd reprocess-audit
```

This command reads SQLite only, performs no Telegram or Bybit request, never
captures a current quote, and prints aggregate counters only. It is safe to
re-run. Each attempt is recorded separately in
`audit_reprocessing_attempts`; the canonical inbound row is updated only to a
validated current outcome. Use `--limit 500` for a bounded run. If the
database is not at the default `data/traderrd.sqlite3`, pass its path
explicitly with `--database-path` rather than loading Telegram configuration.

## Historical outcome simulation

`simulate-history` evaluates the validated signal-topic history with public
Bybit V5 one-minute klines for USDT linear contracts. It never reads Telegram
configuration, never uses a Bybit credential or private endpoint, and never
places or emulates an exchange order.

The default invocation is a read-only dry run. It reports how many signals are
eligible and performs no public-data request or database write:

```bash
./.venv/bin/traderrd simulate-history
```

Start with a bounded explicit public-data run:

```bash
./.venv/bin/traderrd simulate-history --limit 5 --fetch-public-data
```

After reviewing the aggregate results, remove `--limit` to process all eligible
signals. Stop the live observer while the simulation writes SQLite evidence,
then restart it afterward. Re-running the same model is idempotent: terminal
outcomes are skipped, while incomplete market-data attempts can be retried.

The simulation uses these conservative rules:

- Entry is a post-only limit hypothesis at the signal entry price, valid for
  three hours after Telegram receipt.
- Evaluation starts with the first complete one-minute candle at or after
  receipt. A limit touch requires `low <= entry <= high`.
- After entry, TP and SL are evaluated for 24 hours by default. Override the
  research horizon with `--exit-horizon-hours`, which creates a distinct model
  version rather than overwriting a terminal result from another horizon.
- Outcomes are `no_entry_before_expiry`, `take_profit_first`,
  `stop_loss_first`, `no_exit_within_horizon`, and `intrabar_ambiguity`.
  Missing or discontinuous candle coverage is recorded as `data_unavailable`.
- If entry and an exit threshold share a candle, or both TP and SL are possible
  in one candle, the result remains ambiguous. It is NEVER assigned a guessed
  event order and is excluded from resolved gross-return totals.

Each attempt is audited separately. The current outcome row stores the model
version, requested range, provider timestamps, row/page counts, a SHA-256 hash
of the normalized candle dataset, and decisive candle metadata. Console output
contains aggregate counters only.

The eligibility query is locked to parent chat `2180632014`, topic root
`231508`, and the completed topic-backfill checkpoint range. This excludes the
known parent-chat-only audit prefix without rewriting it. Because the original
`signals` schema predates per-row topic provenance, this range is recorded as
`topic_checkpoint_range`; it is a provenance limitation of the existing 141
signals, not proof embedded in each legacy row.

### Data limitations

One-minute OHLC data can show only that prices were inside a candle range. It
cannot prove post-only acceptance, maker status, queue position, an actual fill,
or the order of multiple price events inside one candle. Gross return is based
only on the configured entry and TP/SL prices. It excludes fees, funding,
slippage, latency, partial fills, leverage, and capital allocation. The printed
resolved gross-return percentage is an arithmetic evidence total, not portfolio
profitability or a promise of executable performance.

## Portfolio risk engine

TraderRd includes a deterministic portfolio/risk state machine. It consumes
explicit mark-to-market equity snapshots and signal prices, then emits audited
decisions and **proposed executor actions only**. It does not read an exchange
balance, contact Telegram or Bybit, submit orders, cancel orders, or close
positions. The snapshot producer must supply net equity including unrealized
PnL and its estimated position-closing costs; the engine never invents account
state from public prices.

The fixed policy is:

- 10x isolated leverage.
- Position notional sized from entry-to-stop price risk plus a configurable
  estimated round-trip cost rate. Target risk is 1.5% of current equity, with
  notional capped at 50% of equity.
- At most three active reservations across pending entries, filled positions,
  and close-requested positions. Total reserved worst-case risk cannot exceed
  5% of current equity.
- Maker-first/PostOnly entry proposals expire after three hours. Pending orders
  reserve their full projected risk so multiple orders cannot all fill beyond
  the portfolio limits.
- A same-symbol opposite signal cancels an unfilled pending entry. If the
  existing position is filled, the engine emits a close request and stages the
  inverse proposal. It creates the new PostOnly reservation only after an
  explicit close confirmation and a fresh equity mark; both directions are
  never held simultaneously.
- America/Lima defines daily and Monday-based weekly boundaries. A 6% daily or
  10% weekly mark-to-market loss latches an entry pause for the corresponding
  period, cancels pending entries, and leaves filled positions under their hard
  exchange TP/SL.
- A 15% mark-to-market drawdown from the high-water mark cancels pending
  entries, requests closure of every filled position, and latches a kill switch.
  Recovery snapshots NEVER rearm it. Rearming requires an explicit operator
  reference and confirmation that every position is closed.
- Admission evaluates current reserved risk, projected stop loss, and estimated
  costs before accepting a trade. A proposal that reaches a daily, weekly, or
  drawdown boundary is rejected before creating a reservation.

All arithmetic uses `Decimal`. The risk engine uses only its explicit
`estimated_cost_rate`; Demo preflight does not verify actual exchange fee rates.
Sizing is intentionally unrounded because instrument tick size, quantity step,
and minimum notional belong to the executor. That executor must round
conservatively, place isolated/PostOnly orders, maintain hard TP/SL, confirm
fills/cancellations and closes, and feed those confirmations back into this
state machine. It must NEVER interpret a risk action as proof that an exchange
action succeeded.

### Offline replay and reporting

Risk replay accepts JSON Lines. It is dry-run and in-memory by default:

```bash
./.venv/bin/traderrd risk-replay --commands-file risk-commands.jsonl
```

Persist the state and append-only command/event audit only with the explicit
flag below. This still performs zero exchange execution:

```bash
./.venv/bin/traderrd risk-replay \
  --commands-file risk-commands.jsonl \
  --apply-risk-state
```

Inspect aggregate local state without network or environment configuration:

```bash
./.venv/bin/traderrd risk-report
```

A minimal command file starts with explicit equity and then proposes a signal:

```json
{"type":"initialize","command_id":"init-1","occurred_at":"2026-08-17T09:00:00-05:00","equity":"1000","estimated_cost_rate":"0.001"}
{"type":"propose","command_id":"proposal-1","occurred_at":"2026-08-17T09:01:00-05:00","mark_equity":"1000","signal_id":"signal-1","symbol":"BTCUSDT","direction":"LONG","entry":"100","take_profit":"100.8","stop_loss":"97"}
```

Supported command types are `initialize`, `snapshot`, `propose`,
`confirm_fill`, `cancel_pending`, `confirm_close`, and `manual_rearm`. Every
command needs a unique `command_id` and timezone-aware `occurred_at`. Replaying
the exact command ID and payload is idempotent; reusing an ID with different
input fails closed. Console replay output contains counters only. SQLite keeps
the canonical state, normalized input digest, decision, ordered transition
events, and revision for auditability.

## Bybit Demo Trading execution

Private execution is restricted in code to the documented Demo Trading REST
origin `https://api-demo.bybit.com`. Mainnet, testnet, and arbitrary private API
origins are rejected. The signed client allowlists only the account, wallet,
instrument, position, order, execution, cancellation, and trading-stop V5
operations supported and needed by this workflow. Withdrawal and fee-rate
endpoints are not callable.

Create a dedicated **Bybit Demo Trading** API key with contract read/trade
permissions and NO withdrawal permission. Do not reuse a mainnet or testnet key.
Keep credentials outside source control and export them only in the terminal
that runs the command:

```bash
export BYBIT_DEMO_API_KEY='your-demo-key'
export BYBIT_DEMO_API_SECRET='your-demo-secret'
export BYBIT_DEMO_BASE_URL='https://api-demo.bybit.com'
```

Never paste key values into chat, logs, command arguments, Git, screenshots, or
support messages. TraderRd never prints the values and credential objects hide
them from their representation.

Every `demo-*` command loads `--env-file` before reading `BYBIT_DEMO_*` values.
The default path is `.env`; a custom private path is supported, for example
`--env-file /secure/path/traderrd-demo.env`. Values already exported in the
process environment take precedence over file values. Credential values are
never included in console output or errors.

Rejected Demo API operations are reported with an opaque operation code such as
`wallet_balance_unavailable`. Diagnostics never include the request URL,
credentials, response payload, `retMsg`, or account values.

### Read-only preflight

Before any submission, configure the Demo Trading account for isolated margin
and one-way linear positions, ensure Demo USDT is available, then run:

```bash
./.venv/bin/traderrd demo-preflight --symbol BTCUSDT
```

Preflight performs GET requests only. It fails closed unless it can verify:

- the exact Demo Trading endpoint and synchronized clock;
- isolated account margin mode and one-way `positionIdx=0` behavior;
- positive available Demo Trading balance; in isolated margin this is derived
  from the USDT coin row as
  `walletBalance - totalPositionIM - totalOrderIM - locked - bonus`, because
  account-wide available balance is not applicable;
- fee verification status. Demo reports exchange fee verification as
  `unavailable`; risk sizing uses only the explicit configured
  `estimated_cost_rate` and never presents it as an exchange-verified fee;
- symbol tick size, quantity step, minimum/maximum quantity, and minimum
  notional.

### Deliberate submission workflow

The Telegram listener NEVER submits orders. A supervised bridge can evaluate
one newly stored signal from the validated Telegram source. It requires the
exact parent chat `2180632014`, topic `231508`, numeric sender provenance, and
message ID already recorded on the signal row. Historical rows without that
provenance and messages from any other source are not selectable. The supplied
sender ID must also match the confirmed `TELEGRAM_EXPECTED_SENDER_ID` in the
selected environment file; the value itself is never printed.

The bridge reads a fresh private Demo account snapshot but performs no mutation
by default:

```bash
./.venv/bin/traderrd demo-bridge \
  --message-id TELEGRAM_MESSAGE_ID \
  --source-sender-id NUMERIC_SENDER_ID
```

Sizing uses the ENTIRE current strategy-account mark-to-market USDT equity as
its basis on every proposal: `walletBalance + unrealisedPnl`, cross-checked
against the reported coin equity. It does not size from available/free margin
and does not use a fixed allocation. The 50% equity notional cap, 1.5% target
risk including configured estimated costs, three-reservation/5% aggregate-risk
limits, daily/weekly loss gates, and drawdown kill switch still apply.

The account snapshot fails closed if it is incomplete or stale, or if any open
position/order is not represented by TraderRd's owned local execution state.
The PostOnly expiry is always the original Telegram receipt time plus three
hours. An expired signal can be reported as stale but can never create a risk
reservation or execution intent.

Only this additional flag persists the audited risk decision and creates
deterministic Demo execution intents; it still submits ZERO orders:

```bash
./.venv/bin/traderrd demo-bridge \
  --message-id TELEGRAM_MESSAGE_ID \
  --source-sender-id NUMERIC_SENDER_ID \
  --persist-risk
```

The output returns the deterministic `risk_command_id`. Re-running the same
signal is idempotent. The persistent risk state retains America/Lima daily and
weekly anchors across process restarts while applying each fresh mark-to-market
equity snapshot before deciding.

Private submission remains a separate, explicit command. Previewing an audited
risk command performs no private request or database write:

```bash
./.venv/bin/traderrd demo-execute \
  --symbol BTCUSDT \
  --risk-command-id proposal-1
```

Only the additional explicit flag below permits private Demo Trading mutation:

```bash
./.venv/bin/traderrd demo-execute \
  --symbol BTCUSDT \
  --risk-command-id proposal-1 \
  --submit-demo
```

Entry requests use deterministic `trd-demo-e-*` `orderLinkId` values, USDT
linear one-way mode, a PostOnly limit at the normalized signal entry, and the
risk engine's three-hour expiry. Expiry cancels that owned link and NEVER falls
back to a market entry. A create-order acknowledgement is recorded only as
`acknowledged`; it is NEVER treated as a fill.

Reconciliation is also dry by default:

```bash
./.venv/bin/traderrd demo-reconcile --intent-id INTENT_ID
```

The explicit reconciliation flag permits private reads and, only after a fill
is proven by owned order status, execution history, and one-way position state,
sets and verifies exchange-side TP/SL:

```bash
./.venv/bin/traderrd demo-reconcile \
  --intent-id INTENT_ID \
  --apply-demo-reconciliation
```

Demo Trading does not provide WebSocket Trade. TraderRd deliberately uses
bounded REST polling of realtime orders, closed-order history, executions, and
positions instead of pretending that an acknowledgement is final state.
Confirmation latency therefore depends on how often reconciliation is run.
Unknown, partial, or contradictory states become `reconciliation_required`;
they never become invented fills.

The lifecycle monitor automates this reconciliation for every owned active
intent. A dry run is the default:

```bash
./.venv/bin/traderrd demo-monitor
```

The explicit mutation flag performs one REST polling cycle, confirms fills in
the risk engine, verifies exchange-side TP/SL, and cancels owned entries that
have reached their three-hour expiry:

```bash
./.venv/bin/traderrd demo-monitor --apply-demo-monitor
```

For foreground operation, `--watch` repeats cycles at the configured interval:

```bash
./.venv/bin/traderrd demo-monitor \
  --apply-demo-monitor --watch --interval-seconds 30
```

The monitor never submits a new entry directly and never falls back to market
orders. It may persist a staged inverse-entry intent after a confirmed close;
the fresh-signal worker submits that intent only after its ownership and
dependency checks. The monitor fails closed on unmanaged orders, positions, or
contradictory exchange state.
Until a process supervisor is configured, keep the watch command in a managed
terminal session.

### Automatic fresh-signal worker

The worker closes the remaining gap between the Telegram observer and the
Demo execution lifecycle. It is dry-run by default and requires a separate
explicit mutation flag:

```bash
./.venv/bin/traderrd demo-worker
./.venv/bin/traderrd demo-worker --apply-demo-worker --watch --interval-seconds 30
```

Run the observer, worker, and lifecycle monitor in managed terminal sessions.
The worker is restricted to parent chat `2180632014`, topic `231508`, and
sender `8003985182`; it cannot consume a signal from another Telegram source.
Signals older than three hours are skipped and never submitted. On the first
applied startup it records a durable live boundary at the newest already
stored signal, so historical/backfill rows are **not replayed**. Each later
signal is advanced only after risk admission and any required Demo order
submission have been durably recorded. A transport or ambiguous exchange
failure leaves the cursor behind and retries the owned deterministic intent;
it does not silently move past the signal.

The worker sizes from the current full mark-to-market strategy equity through
the persistent risk engine and submits only normalized `PostOnly` Demo entries.
The existing lifecycle monitor remains responsible for fill evidence,
exchange-side TP/SL, three-hour cancellation, and risk-state confirmation. For
an inverse signal, a pending entry is cancelled before the new entry is
submitted. A filled opposite position is closed with an owned reduce-only
order; only after a confirmed flat position is the staged inverse PostOnly
entry materialized and picked up by the worker. Mainnet and testnet origins are
rejected before any private request.

Close actions are allowed only when the same reservation has an owned filled
entry intent. They submit a unique `trd-demo-c-*` market order with
`reduceOnly=true`
and require both execution evidence and a flat position before becoming
`close_confirmed`. The risk engine creates an inverse PostOnly action only after
that close confirmation is fed back through `confirm_close`. TraderRd queries,
cancels, and reconciles only its `trd-demo-*` namespace and never mass-cancels
or modifies unrelated orders.

Only the explicitly allowlisted documented V5 REST endpoints are supported.
Demo Trading validates signing, precision, PostOnly behavior, state
reconciliation, cancellation, and TP/SL plumbing. It does not reproduce real
liquidity, queue position, slippage, funding, production fees, or real-capital
operational risk.

### Migration from the retired testnet workflow

`testnet-preflight`, `testnet-execute`, and `testnet-reconcile` are retained
only as fail-closed command aliases. They print an explicit migration error and
never contact either environment. `BYBIT_TESTNET_*` variables are not read.
Generate a Demo Trading key, use the dedicated `BYBIT_DEMO_*` variables, and
switch scripts to the `demo-*` commands and flags shown above. Demo execution
uses separate `demo_execution_*` audit tables; no testnet table is migrated or
reused automatically.

## Tests

The domain, application, and SQLite tests use only the Python standard library
and never contact Telegram:

```bash
PYTHONPATH=src python3 -m unittest discover -s tests -v
```

## Current limits

- The signal body contains no timezone. The embedded signal timestamp is
  therefore retained exactly as a timezone-naive provider timestamp. Telegram
  receipt/edit timestamps are stored separately with timezone information.
- The accepted strategy is fixed to TP 0.8% and SL 3%. A materially different
  price or percentage is rejected rather than silently interpreted.
- Historical simulation is an OHLC evidence model, not an execution engine or
  paper broker. Intrabar ambiguity is deliberately unresolved.
- A ticker snapshot describes the market near ingestion time only. It does not
  prove that an entry, take profit, or stop loss could or did execute.

## Operational and maintenance runbook

This section is the shortest reliable operating reference for the live Demo
workflow. It describes the current implementation, not a production-broker
guarantee.

### End-to-end architecture and data flow

1. **Telegram observer** — `traderrd run` uses the authenticated Telethon user
   session. It resolves the configured parent chat, validates the forum topic
   and sender, parses signal bodies, and stores the inbound audit row and the
   canonical signal in SQLite. Public Bybit ticker captures are evidence only.
2. **Signal store** — `inbound_messages`, `inbound_revisions`, and `signals`
   preserve the source message, parsed strategy values, provenance, edits, and
   fingerprints. Backfill and live observation share this audit path; the
   execution worker does not read unscoped parent-chat history.
3. **Demo signal worker** — `demo-worker --apply-demo-worker` reads only
   source-attested signals after its durable live cursor, rejects signals older
   than three hours, asks the private Demo snapshot provider for current
   mark-to-market equity and instrument rules, and persists the risk decision
   and deterministic execution intents before submitting any owned PostOnly
   entry.
4. **Bybit Demo** — the signed client uses only
   `https://api-demo.bybit.com` and the allowlisted V5 account, wallet,
   instrument, position, order, execution, cancellation, and trading-stop
   operations. Mainnet and testnet credentials/origins are rejected.
5. **Lifecycle monitor** — `demo-monitor --apply-demo-monitor` reconciles the
   owned `trd-demo-*` namespace through bounded REST reads. It distinguishes an
   order acknowledgement from a fill, verifies the one-way position and
   exchange-side TP/SL, cancels expired entries, confirms closes, and syncs
   risk state. It never mass-cancels unrelated exchange orders.

### Telegram source hierarchy and guards

The accepted source is deliberately narrow:

| Level | Required value | Meaning |
| --- | ---: | --- |
| Parent chat | `2180632014` | The Telegram group/supergroup container |
| Forum topic | `231508` | **Señales Tr4derBOT**, the only readable topic |
| Seed message | `231906` | The message used by source inspection to validate the topic |
| Numeric sender | `8003985182` | The expected signal publisher identity |

The observer first resolves the parent chat from the authorized account's
dialogs, then verifies topic membership before parsing. Parser-valid messages
must also match the numeric sender guard. Display names, channel labels, and
links are not identity controls. General-group posts, other topics, service
messages, and unexpected senders are ignored or fail closed according to the
operation. Run the metadata-only check before any live session:

```bash
./.venv/bin/traderrd inspect-source
```

Do not widen the chat/topic/sender scope to make a missing signal pass. If the
seed or sender changes, inspect the new source deliberately and update the
configuration only after confirming the Telegram hierarchy.

### Risk policy and compounding basis

Every proposal uses the current full strategy-account mark-to-market equity,
not free or available margin:

```text
strategy equity = walletBalance + unrealisedPnl
```

The persistent risk engine applies this fixed policy:

- isolated margin, 10x leverage, one-way linear positions;
- target notional capped at 50% of equity;
- dynamic sizing toward approximately 1.5% account risk per trade,
  including the configured estimated cost rate;
- maximum three active reservations and 5% aggregate open/reserved risk;
- daily entry pause at -6%, weekly entry pause at -10%, and a -15% high-water
  mark drawdown circuit breaker;
- America/Lima daily and Monday-based weekly anchors;
- no automatic rearm after the circuit breaker; rearm is an explicit operator
  action after positions are closed.

The engine reserves risk before an order is submitted. Instrument precision is
applied by the executor, and an exchange acknowledgement is never treated as
proof of a fill.

### Signal and order lifecycle

```text
Telegram message
    -> source guard + parser
    -> SQLite signal/fingerprint
    -> fresh-cursor worker (<= 3 hours)
    -> equity snapshot + risk admission
    -> persisted reservation + deterministic intent
    -> Demo PostOnly entry
    -> monitor proves fill/position
    -> exchange-side TP/SL verified
    -> close by TP, SL, expiry, or inverse policy
```

- **Entry expiry:** the entry window is Telegram receipt time plus three hours.
  An unfilled PostOnly entry is cancelled; the system never replaces it with a
  market entry.
- **Pending inverse:** an opposite signal cancels the owned pending order first.
  The new entry is submitted only after cancellation is confirmed.
- **Filled inverse:** an opposite signal requests an owned reduce-only market
  close. The opposite PostOnly reservation is staged only after execution
  evidence and a flat one-way position are confirmed.
- **Protection:** after a fill is proven, the monitor sets and verifies the
  signal TP and SL on the exchange. Missing, contradictory, or incomplete
  evidence becomes `reconciliation_required`, never an invented fill.
- **Protected-position closure:** a protected entry stays monitorable after
  TP/SL verification. The monitor re-reads its owned one-way position each
  cycle; an observed flat position first enters durable
  `position_closed_pending`, then becomes terminal `position_closed` only
  after Bybit supplies one complete and unique closed-PnL record matching the
  owned symbol, closing side, size, and time window, and then the risk ledger
  accepts the close with fresh account equity. Missing, partial, or ambiguous
  closed-PnL evidence remains `position_closed_pending`, reserves risk, and is
  excluded from performance. The local record intentionally stores
  `exit_reason_unknown` unless concrete exchange evidence identifies TP or SL.
  A reopened, mismatched, or ambiguous position fails closed and remains
  available for reconciliation.

### One visible Demo runtime terminal

For normal Demo operation, use one foreground command:

```bash
./.venv/bin/traderrd demo-run
```

`demo-run` starts the observer, fresh-signal worker, and lifecycle monitor as
three supervised child processes. In an interactive terminal it automatically
opens the full-screen **TraderRd Demo dashboard**: live health and risk state, source/worker
lag, active and pending orders, expiry, ledger performance by pair, and a clearly
labelled *hypothetical* TP/SL scenario.
It uses a calm dark terminal palette when colors are available: cyan for
hierarchy/Demo boundary, green for healthy and positive P&L, yellow for warning
risk, and red for stopped or severe drawdown. Non-color terminals keep the same
text hierarchy without ANSI escape sequences.
It refreshes from local SQLite roughly every second and makes no extra Telegram,
Bybit, credential, or database-write call. If stdin/stdout are not interactive
or `TERM` is unsupported (for example `dumb`), it prints the exact fallback
reason and continues with visible prefixed logs; this is expected behaviour,
not a silent dashboard failure. `q` stops all three components
cleanly; `r` refreshes; the footer lists controls. The
projection is not a profitability forecast: it only sums existing planned
TP/SL outcomes and excludes fills, fees, slippage, funding, and future signals.
`Ctrl-C` also stops all three in a controlled way. It always uses the existing **Bybit Demo Trading
only** commands and has no mainnet, real-account, or environment-switch flag.
The dashboard is Demo-only and is not a future real-account control plane;
any real-account launcher requires a separate, explicitly designed approval,
credentials, endpoint, and recovery boundary. It holds an exclusive per-database runtime lock, so a second `demo-run` fails
instead of creating duplicate order workers. Stop the existing `demo-run`
terminal first; the lock clears automatically when it exits.

Use `--interval-seconds` only when deliberately changing the normal 30-second
cadence:

```bash
./.venv/bin/traderrd demo-run --interval-seconds 30

# Useful for pipes, small terminals, or traditional prefixed logs
./.venv/bin/traderrd demo-run --no-tui
```

The independent commands below remain available for diagnostics and isolated
recovery. Do not run them alongside `demo-run`, because that would duplicate a
component outside the unified runtime lock.

### Three visible runtime terminals (diagnostic mode)

Keep these as separate visible processes so each responsibility and failure is
obvious:

**Terminal 1 — Telegram observer**

```bash
./.venv/bin/traderrd run
```

**Terminal 2 — fresh-signal worker**

```bash
./.venv/bin/traderrd demo-worker \
  --apply-demo-worker --watch --interval-seconds 30
```

**Terminal 3 — lifecycle monitor**

```bash
./.venv/bin/traderrd demo-monitor \
  --apply-demo-monitor --watch --interval-seconds 30
```

The worker and monitor intentionally have separate mutation flags. A worker
dry run and a monitor dry run perform no private exchange mutation and report
`database_writes=false`/`orders_submitted=0` where applicable.

`position_closed` is terminal for the local execution intent and is excluded
from active ownership/risk monitoring. The reservation is released exactly
once through a deterministic risk command; repeating a monitor cycle or
restarting the monitor does not create another close or risk event.

### Safe startup, restart, and stop procedure

**Before the first applied startup:**

1. Confirm the `.env` file contains only dedicated `BYBIT_DEMO_API_KEY`,
   `BYBIT_DEMO_API_SECRET`, and `BYBIT_DEMO_BASE_URL=https://api-demo.bybit.com`
   values. Never paste secrets into the shell history, chat, logs, or Git.
2. Confirm the Telegram session is authorized and run `inspect-source`.
3. Run a read-only Demo preflight for every symbol family that will be traded:
   `./.venv/bin/traderrd demo-preflight --symbol BTCUSDT`.
4. Run the test suite and a dry worker/monitor check.
5. Prefer `./.venv/bin/traderrd demo-run`. In diagnostic mode, start the
   observer first, then the worker, then the monitor. On its first
   applied cycle, the worker records the newest already-stored exact-source
   message ID as its live boundary. It does **not** replay backfill/history
   rows; only later stored signals are eligible.

**Restart after a network or process interruption:**

1. Prefer restarting `demo-run` from the same authorized session. In
   diagnostic mode, restart the observer first; it has bounded
   reconnect backoff and preserves SQLite ingestion idempotency.
2. Restart the worker with the same database. Its cursor, risk commands,
   intents, order links, and submission-attempt records are durable, so an
   interrupted request is reconciled before a deterministic intent is retried.
3. Restart the monitor with the same database. It re-reads owned exchange
   state and fails closed on ambiguity. Do not create a manual replacement
   order while a `trd-demo-*` intent is unresolved.

**Stop safely:**

1. With `demo-run`, use `Ctrl-C` once; it stops the worker, monitor, and
   observer together. In diagnostic mode, stop the worker first with `Ctrl-C`
   to prevent new entries.
2. Keep the monitor running while owned pending orders or positions remain. If
   it must also stop, verify the Demo account has no unresolved TraderRd order
   or position and plan the next monitor restart before stopping it.
3. Stop the observer last. Do not delete or manually edit the SQLite database,
   risk state, cursor, or session file as a shutdown shortcut.

Avoid running Telegram backfill, historical simulation, or ad-hoc database
migrations concurrently with these three processes. Stop the observer before
backfill as described above; take a database backup only while mutation
processes are stopped.

### Future real-account operations

The unified runtime is deliberately an operations interface, not a real-money
feature. It currently launches **only Demo Trading** and no command or
configuration switch may promote it to mainnet. Before any future real-account
work, define a separate approval, credential, endpoint, deployment, alerting,
and rollback design; do not repurpose `demo-run` or Demo credentials as a
shortcut.

### Health and state inspection

Use read-only or dry commands first:

```bash
./.venv/bin/traderrd inspect-source
./.venv/bin/traderrd demo-preflight --symbol BTCUSDT
./.venv/bin/traderrd demo-worker
./.venv/bin/traderrd demo-monitor
./.venv/bin/traderrd risk-report
PYTHONPATH=src python3 -m unittest discover -s tests -v
```

The worker prints aggregate counters such as `fresh`, `stale`, `cursor`, and
`orders_submitted`; the monitor prints `inspected`, `working`, `filled`,
`protected`, `cancelled`, and `risk_updates`. Logs intentionally omit raw
signal text, credentials, response payloads, and account secrets. For one
owned intent, `demo-reconcile --intent-id INTENT_ID` is a dry state inspection;
add `--apply-demo-reconciliation` only when the account state and intent link
have been verified.

For a single read-only operational snapshot, use:

```bash
./.venv/bin/traderrd status
./.venv/bin/traderrd status --no-color
./.venv/bin/traderrd status --json

# Read-only dashboard without starting bot processes
./.venv/bin/traderrd tui
```

`status` reads the default `data/traderrd.sqlite3`, or an explicit
`--database-path`, through SQLite's read-only mode. It never loads `.env`,
credentials, Telegram, or Bybit, and it never initializes or writes a table.
The default terminal output is a scan-friendly Demo Trading snapshot grouped by
systems, signal flow, execution, risk, **performance**, and incidents. It displays timestamps in
America/Lima (`PET`) and separates current incidents from resolved historical
errors, so a healthy component is not presented as failing solely because it
retains a past error. In an interactive terminal it uses restrained ANSI color:
green for healthy components and positive P&L, yellow for warnings, and red for
stops or negative P&L. Color is disabled automatically when output is piped,
when `NO_COLOR` is set, or with `status --no-color`. The performance section
always explains the per-pair state: it displays a readable row for each pair
with attributed closures, or explicitly says that no verified closes exist yet.
The JSON output is stable schema version `1` and includes
exact-source signal freshness, worker cursor/lag, intent state counts and
three-hour expiry, risk equity/halts, active reserved risk, current drawdown
and circuit-breaker latch, reconciliation-required intents, and sanitized
recent errors. Its stable `performance` object is ledger-only and begins at
this feature rollout; it never backfills or estimates historic results. It
reports attributed closed trades by pair, wins/losses/breakeven, win rate
(excluding breakeven trades), exchange-reported Closed P&L, trading fees, and
net P&L in USDT, plus unresolved exits. Bybit defines Closed P&L as the final
result after opening/closing fees and funding, so `net_pnl` equals the
exchange-reported Closed P&L; fees are a breakdown and are **not** subtracted a
second time. See Bybit's [Closed P&L calculation](https://www.bybit.com/en/help-center/article/Profit-Loss-calculations-USDT-Contract)
and [closed-PnL API fields](https://bybit-exchange.github.io/docs/v5/position/close-pnl).
Component health is `healthy` at 90 seconds or less since its last heartbeat,
`degraded` through five minutes or after a newer error, and `stopped` after
five minutes or an explicit stop; a component with no heartbeat is
`not_started`. A missing component makes an otherwise running overall system
degraded, while a stopped component makes it stopped.

Heartbeat telemetry is additive and best effort: the observer pulses while
idle and the worker/monitor record startup, cycle, error, and shutdown events.
The currently running terminals do not begin emitting these new heartbeats
until they are manually restarted. Do not stop active processes just to enable
status; restart them safely using the procedure above when the next controlled
maintenance window is available.

When a component is `not_started` or `stopped`, the human summary prints the
single visible `demo-run` activation command. A `degraded` component prints an
investigation warning instead and never prints a start command; its JSON
`activation_command` is `null`, as it is for healthy components, to prevent a
duplicate runtime from being started before the fault is understood.

### Troubleshooting

- **Network permission or connection errors:** run the command from a visible
  terminal with outbound network access. The observer retries transient
  transport interruptions with bounded backoff, but permission failures stop
  immediately. Do not disable source guards or retry an ambiguous order by
  hand.
- **Missing/invalid credentials:** use only the dedicated `BYBIT_DEMO_*`
  variables and the exact Demo origin. `BYBIT_TESTNET_*` values are ignored;
  a mainnet URL is rejected before private execution.
- **Source isolation rejected:** run `inspect-source`, verify chat `2180632014`,
  topic `231508`, seed `231906`, and sender `8003985182`, then fix the
  configuration or Telegram session. Never broaden the query to the general
  group.
- **`reconciliation_required`:** the exchange response was incomplete,
  contradictory, or not uniquely owned. Stop new submissions, inspect the
  corresponding `trd-demo-*` intent and Demo order/position, and use the
  explicit reconciliation command only after the ownership evidence is clear.
  Never mark a fill or close manually just to advance the cursor.
- **`position_closed_pending`:** the monitor observed an owned flat position but
  has not yet received unique, complete Bybit closed-PnL evidence and completed
  the durable risk close. Keep the monitor running; it retries the evidence
  query and only then the same idempotent close command. A missing, partial, or
  ambiguous record is shown as an unresolved exit in `status` and never counts
  in P&L. If the position reappears or becomes ambiguous, the intent moves to
  `reconciliation_required` without releasing risk.
- **`position_closed`:** the local terminal record means the owned position was
  verified flat and the reservation was closed in the risk ledger. Its exit
  reason is intentionally unknown unless supported by concrete exchange
  evidence; do not infer TP versus SL from a flat position alone.
- **Stale account snapshot, unmanaged order/position, margin-mode, precision,
  or clock errors:** treat these as safety stops. Correct the Demo account or
  environment, rerun preflight, and let the monitor/worker retry the durable
  intent. Do not delete risk state to bypass a guard.

### Demo-only invariant

This project is currently authorized for Bybit Demo Trading only. Any change
that introduces a mainnet/testnet endpoint, reuses a non-Demo credential,
widens the Telegram source, interprets an acknowledgement as a fill, bypasses
exchange-side TP/SL verification, or sends a market fallback for an expired
entry is a safety regression and must be rejected in review.

## Docker server deployment (Demo-only)

This deployment is intended for an Ubuntu server running Docker Engine with the
Docker Compose plugin. It is **not** a real-account deployment: the only private
exchange endpoint accepted by the application is Bybit Demo Trading.

### Prerequisites

- Ubuntu host with Docker Engine and `docker compose` installed.
- A dedicated Telegram user session for the exact configured signal topic.
- Dedicated Bybit **Demo Trading** API credentials with no withdrawal permission.
- A private `.env.server` file; it is intentionally ignored by Git.

Install Docker Engine and the Compose plugin by following Docker's official
[Ubuntu installation guide](https://docs.docker.com/engine/install/ubuntu/) and
[Compose plugin guide](https://docs.docker.com/compose/install/linux/). Verify
both commands before deployment:

```bash
docker --version
docker compose version
```

Build the image and create the private environment file:

```bash
cp .env.server.example .env.server
chmod 600 .env.server
# edit .env.server with the real Telegram and Demo-only Bybit credentials
sudo docker compose build
```

The persistent Docker volume `traderrd_state` holds both the Telegram session
and SQLite database at `/var/lib/traderrd`. Do not remove that volume during a
normal restart or image update.

### First-time Telegram authorization

The bootstrap service is interactive and never starts the observer, worker,
monitor, or a Bybit client. Run it once from a terminal attached to the server:

```bash
sudo docker compose --profile bootstrap run --rm telegram-auth
```

Enter Telegram's code (and password if Telegram requests it). The command only
reports success after the session is authorized. It shares the same persistent
volume used by the application service.

### Initial verification and startup

Before starting applied trading, verify the exact source and Bybit Demo account
from a one-off interactive container. These commands contact their named service
but do not submit an order:

```bash
sudo docker compose run --rm traderrd inspect-source
sudo docker compose run --rm traderrd demo-preflight --symbol BTCUSDT
```

Start the one persistent Demo-only service:

```bash
sudo docker compose up -d traderrd
sudo docker compose ps
sudo docker compose logs -f traderrd
```

`demo-run --no-tui` is used deliberately on a server: the service emits
prefixed component logs while observer, worker, and monitor remain supervised
inside one container. The local full-screen TUI is for an attached interactive
terminal, not for the detached service.

### Health, status, stop, and recovery

Docker marks the service healthy only when SQLite is readable and all three
heartbeats (`observer`, `worker`, and `monitor`) are healthy. Compose allows a
bounded two-minute startup period before checking this strict condition.

```bash
# Read-only operational snapshot, no credentials or network usage
sudo docker compose exec traderrd traderrd status

# Strict local readiness check (non-zero when any component is not healthy)
sudo docker compose exec traderrd traderrd healthcheck

# Stop without deleting the persistent state volume
sudo docker compose stop traderrd

# Start it again
sudo docker compose up -d traderrd
```

If the container is unhealthy, inspect its logs first. Do not run a second
trading container, edit SQLite manually, or remove the state volume as a first
response. Restore the service only after the cause is understood; the worker
and monitor are intentionally fail-closed when exchange state is ambiguous.

### Update and SQLite backup/restore

The image can be updated without deleting `traderrd_state`:

```bash
sudo docker compose pull
sudo docker compose build --pull
sudo docker compose up -d traderrd
sudo docker compose ps
```

Use the included Python `sqlite3` backup API rather than copying database files.
It takes a consistent snapshot from the active database, including committed WAL
content, writes a self-contained backup, and verifies SQLite integrity before it
reports success. This command does not start observer, worker, monitor, Telegram,
or Bybit:

```bash
mkdir -p backups
BACKUP="traderrd-$(date -u +%Y%m%dT%H%M%SZ).sqlite3"
sudo docker compose exec -T traderrd python /app/scripts/sqlite_snapshot.py backup \
  /var/lib/traderrd/traderrd.sqlite3 "/var/lib/traderrd/backups/$BACKUP"
sudo docker compose cp "traderrd:/var/lib/traderrd/backups/$BACKUP" "./backups/$BACKUP"
python3 scripts/sqlite_snapshot.py verify "./backups/$BACKUP"
```

For a verified restore, stop applied trading first. The restore helper validates
the backup, atomically replaces the database, and removes stale WAL/SHM sidecars
so SQLite cannot replay unrelated state. The one-off container runs only Python
with no dependencies and does not start TraderRd components:

```bash
BACKUP=backups/traderrd-YYYYMMDDTHHMMSSZ.sqlite3
python3 scripts/sqlite_snapshot.py verify "$BACKUP"
sudo docker compose stop traderrd
sudo docker compose cp "$BACKUP" traderrd:/var/lib/traderrd/traderrd.sqlite3.restore-source
sudo docker compose run --rm --no-deps --entrypoint python traderrd \
  /app/scripts/sqlite_snapshot.py restore \
  /var/lib/traderrd/traderrd.sqlite3.restore-source \
  /var/lib/traderrd/traderrd.sqlite3
sudo docker compose up -d traderrd
sudo docker compose exec traderrd traderrd status
```

Do not restore a partial database, a database from a different source
configuration, or a Telegram session you do not control. Preserve the previous
state volume and incident logs until the restored runtime has been inspected.

### Real-account boundary

Docker automation does not make this a real-money bot. The image, compose
service, healthcheck, and `.env.server.example` remain Demo-only. A future
real-account deployment requires a separate design, configuration surface,
review, and explicit human approval; changing an endpoint or reusing this
Compose service is not an allowed migration path.
