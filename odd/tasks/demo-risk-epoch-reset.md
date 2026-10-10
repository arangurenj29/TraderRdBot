# Demo risk epoch reset

## Objective
Allow an operator to start a new, auditable Demo-only risk epoch from current mark-to-market equity without erasing the previous risk and trade history.

## Problem and why
The active engine can reject every new signal with `projected_drawdown_limit` before its 15% kill switch trips. `ManualRearm` applies only to a killed engine. A direct high-watermark database edit would bypass risk audit and safety guards.

## Authorized scope
- Local implementation/tests and, after verification, deployment and Demo epoch reset are authorized by the user.
- Remote destination: ubuntu@146.181.63.164 using /Users/anderson/.ssh/traderrd_oci.key; deployment root /home/ubuntu/TraderRd. Check the bot first, then deploy/apply only after fresh complete flatness evidence.
- User explicitly authorized all pending Git synchronization through configured Git/GitHub authentication on origin `https://github.com/arangurenj29/TraderRdBot.git`: preserve work-unit commits, integrate and publish `dev` first, then `main`, without force-push. No PR was requested. Parent owns remote Git delivery; the commit-preparation worker performed local bookkeeping only.
- No real-account support or unrelated remote mutation is authorized.
- No direct SQLite edits, exchange orders, bypass of the existing reservation/kill guards, or fabricated equity evidence.

## Constraints and acceptance criteria
- The reset must be explicit, operator-attributed, idempotent, and persist its old/new epoch boundary in the normal command/event audit trail.
- Risk history and closed-trade performance remain intact.
- Before a reset can authorize new entries, the supervised runtime must be stopped; fresh Bybit Demo account equity and exchange flatness must be independently proven, and local active reservations/reconciliation work must be absent.
- Killed mode remains exclusively handled by `ManualRearm`. Unknown, partial, stale, or contradictory evidence rejects the reset.
- A replay or local numeric equity input alone cannot make the live engine trade again.
- Existing 15% drawdown, daily/weekly, trade sizing, and source constraints remain unchanged in the new epoch.

## Plan and progress
- [x] **T1 — Exclude concurrent Demo mutations.** Add a shared/exclusive operation lock across every applied Demo trading entrypoint, with exclusive epoch ownership, deterministic lock-order tests, and command/runbook updates. Route: delegated direct; multi-file implementation and read-to-write mapping trigger. Checks: isolated-tree compileall, 34 focused tests, 286 full tests, structural diff review. Commit: `5f28b0f4562ae33e7a8223814d0c02f882431378`.
- [x] **T2 — Safe Demo risk-epoch operation.** Implement fresh Demo GET evidence, exchange flatness, transactional local reconciliation and risk-state guards, audited/idempotent epoch command, negative-path tests, and runbook. Prevent live reset through risk replay. Route: delegated direct; multi-file implementation and read-to-write mapping trigger. Checks: isolated-tree compileall, 63 focused tests, 307 full tests, structural diff review. Commit: `44245abeca10cfaac1d5fad57376950f34863c56`.

## Delivery and verification
- Feature branch: `codex/demo-risk-epoch-reset` from `dev`.
- Forecast: approximately 800–1,200 authored changed lines, excluding generated files. Delivery strategy: direct user-authorized integration/publishing through `dev` then `main`; no PR or chain creation. One honest cohesive slicing pass produced T1 (340 authored changed lines) and T2 (681). T2 remains above the advisory 400-line review budget because its fresh-evidence admission, audit/idempotency, replay prohibition, tests, and runbook form one behavior; no code-golf or artificial partial split was used. If a future PR is requested, recommend an explicit size exception for this coherent slice.
- Running authored changed lines: 1,021 across the two behavior commits (T1: 325 additions + 15 deletions; T2: 678 additions + 3 deletions). Recovery-document bookkeeping is separate. Commit order: `5f28b0f` → `44245ab`. Base: `9a47c4497cb91bd51ab66d187e96380e786eca86` on `dev`/`main` before integration.
- RDD: global on. The initial sandbox assessment was unavailable; the consolidated immutable source candidate later assessed medium. User declined that candidate (option 2), target `6f88c43f6c3362594bdd8b0316b4d312771f6fc5a11a2f120b4e1690a2ed6919`, with `declined_this_candidate`; no receipt or approval is claimed. No new review was started for unchanged source or Git delivery. Delivery remains ordinary/unmanaged for this declined candidate.
- Engram mirror topic: `odd/demo-risk-epoch-reset/tasks`. **Pending**: the host has not registered an authoritative runtime session ID; do not perform agent-attributed memory writes.
- T1 implementation evidence (committed): all applied trading CLI entrypoints and applied risk replay hold a shared `.traderrd-demo-operation.lock`; owned-close recovery holds the supervisor lock then an exclusive operation lock. The supervisor holds its supervisor lock then a shared operation lock. Acquisition is nonblocking, precedes credentials/mutations, and releases on exceptions. Read-only paths do not acquire the new lock. README and maintainer guide updated.
- T1 test-first evidence: applied-entrypoint exclusion test observed six failing subtests before wrappers; applied risk-replay exclusion test observed one failure before its wrapper. Both passed after implementation. Final focused run: 34 tests passed. Full suite: 286 tests passed. `python3.12 -m compileall -q src tests` and `git diff --check` passed. Required `.venv/bin/python` is absent; system `python3` is 3.9 and fails on existing `dataclass(slots=True)`, so observed checks used installed `python3.12`. Structural source diff contains no credentials, new endpoints, exchange requests, or direct database writes.
- T1 and T2 work-unit commits and verification are complete. Consolidated source review was declined, not approved. Deployment and Demo reset succeeded under the parent evidence below. Engram mirror remains pending host registration.
- T2 implementation evidence (committed): dedicated `demo-risk-epoch` uses supervisor-then-exclusive-operation ownership before credentials, a fixed-endpoint GET-only Demo adapter and complete account provider, ten-second evidence freshness, zero positions/orders/margins/unrealised P&L, and mark-to-market equity consistency. Repository transaction rechecks reservations/reversals, execution state and risk-sync completion, performance attribution, and freshness before audited state/event persistence. History is retained. Killed state and fresh equity reaching the old 15% kill boundary reject this action. Applied risk replay and ordinary repository execute reject epoch commands. Committed retries use the original audited identity and evidence without new requests.
- T2 verification: initial RED was `tests.test_demo_risk_epoch` failing import because the new dedicated API did not exist. Final GREEN: `PYTHONPATH=src python3.12 -m unittest tests.test_risk_engine tests.test_risk_cli tests.test_demo_cli tests.test_demo_risk_epoch -v` passed 63 tests; full discovery passed 307 tests. `python3.12 -m compileall -q src tests` and `git diff --check` passed. Tests cover stale/future/incomplete/contradictory/nonflat evidence, delay before audit, killed and fresh kill-boundary state, operation lock refusal before credentials, local race recheck, incomplete risk handoffs, preserved closed performance, immutable retry conflicts, GET-only/Demo-only requests, replay rejection, dry-run no writes, and legacy state compatibility. Local worker made no actual exchange/network calls or remote access.

## Work-unit verification and rollback
- One index-only split preserved all runtime worktree bytes; both committed trees were tested from isolated Git archives under `/tmp`, without production data or network calls.
- T1: `python3.12 -m compileall -q src tests` passed; `PYTHONPATH=src python3.12 -m unittest tests.test_demo_runtime tests.test_demo_cli tests.test_risk_cli -v` passed 34 tests; full discovery passed 286 tests. Rollback boundary: shared/exclusive operation locking and its entrypoint wrappers, tests, and matching documentation.
- T2: compileall passed; `PYTHONPATH=src python3.12 -m unittest tests.test_risk_engine tests.test_risk_cli tests.test_demo_cli tests.test_demo_risk_epoch -v` passed 63 tests; full discovery passed 307 tests. Rollback boundary: dedicated epoch command, codecs, transactional guards, replay prohibition, tests, and matching documentation; T1 remains independently runnable.
- Runtime harness: parent deployed-image isolated 307-test run and live Demo epoch/health verification are recorded below; no artificial orders were submitted.
- Git index/commit changes were explicitly authorized; no source normalizer, runtime-file edits, remote operations, merges, pushes, or new review ran during local commit preparation. All 15 non-recovery modified/new file SHA256 hashes match their pre-commit worktree values, and the final code tree matches the verified/deployed bytes.

## Next step
Parent integrates/publishes the verified feature commits into `dev`, then `main`, through configured origin authentication without force-push. Git delivery is pending; do not claim either branch updated until verified. No PR is requested. Recovery document is committed separately so it can name the behavior commits without self-referencing its own hash. Engram mirror remains pending host registration.

## Deployment and epoch evidence — 2026-10-09
- User selected native review option 2; exact target 6f88c43f6c3362594bdd8b0316b4d312771f6fc5a11a2f120b4e1690a2ed6919 returned declined_this_candidate. Native assessment medium; no review receipt claimed.
- New image sha256:b91999c5c67ea850c7e28cc00c78383c88155e83435de7de4511c30fe9f6ed31 passed 307 tests with network disabled and without the production volume. Initial three environment-default expectation failures passed after unsetting production DB/session defaults.
- Source archive SHA256 36d5ccdedadd847c21baf4ab51d4a720154b22c389215c2f9a1e5b61aa8ec427; all 80 remote source files matched.
- SQLite API backup /var/lib/traderrd/backups/pre-epoch-20261009.sqlite3 verified integrity. No live SQLite file copy.
- Old runtime stop exceeded 60 seconds and exited 137; confirmed zero remaining Python/traderrd processes before backup/reset.
- Epoch demo-risk-epoch:demo-restart-2026-10-09 accepted from fresh 2026-10-09T23:54:21.718Z complete Demo GET evidence. High-watermark changed 47090.9144047 -> 40413.96301896; equity unchanged. Daily/weekly anchors rebased.
- All original 1273 risk commands, 531 risk events, 225 execution intents and 93 performance outcomes preserved; exactly one epoch command/event added.
- Parent read-only SSH spot check: compose healthy, active mode, equity/HWM 40413.96301896, zero active reservations/risk, daily_halted=false, weekly_halted=false.
- Zero new signals/submitted orders on observed fresh cycles. Actual future signal admission/fill not yet observed. No artificial orders.
- Rollback source /tmp/traderrd-epoch-20261009/previous-source.tar.gz and image tags traderrd:epoch-rollback-running-20261009 / traderrd:epoch-rollback-tag-20261009 preserved.
- Aggregate account snapshot remains stale; epoch audit contains fresh evidence.
- Work-unit commits and local verification are complete. Parent integration/publishing of `dev` then `main` remains pending; no PR or force-push is authorized by this worker assignment. Engram mirror pending host registration.

## Git synchronization evidence
- User explicitly authorized configured Git/GitHub authentication for origin arangurenj29/TraderRdBot and normal publication through dev then main, with no force-push.
- Fresh origin references matched the local baseline 9a47c44 before integration.
- T1 commit 5f28b0f4562ae33e7a8223814d0c02f882431378 and T2 commit 44245abeca10cfaac1d5fad57376950f34863c56 were integrated with documentation commit 3a7820c9bcb535025e75ed77fe9adb5eccece2fa.
- dev was fast-forwarded and pushed successfully, followed by main fast-forward and successful push. No history rewrite, new source mutation, server restart, or repeated epoch action occurred during Git synchronization.
- Runtime files remained byte-identical to the deployed verified version. Verification remains T1 286 full tests and T2 307 full tests; the final delivery update is passive documentation checked by readback and diff --check.
- Engram mirror remains unavailable until the host registers an authoritative session identity.
- Next step: observe natural new-signal admission; no implementation or Git integration remains pending after this documentation update is published.
