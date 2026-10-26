# GVA queue retention contract — owner decision required

Proposed on 2026-10-26 for Workboard task.5de6f122. Owner: Andrew Pierce.
Status: draft awaiting explicit owner approval. This document grants no cleanup,
DDL, runtime or operator execution authority.

## TTL and failed-task handling

The proposed effective TTL is **indefinite** for tasks, runs, checkpoints and
events in all current mixed financial GVA queues. Keep cleanup unscheduled.
Failed and cancelled tasks remain indefinitely until their exact signed-attempt,
finality and accounting receipt evidence is reconciled. Completed tasks are not
implicitly disposable: prior-attempt recovery and idempotency readers can still
refer to them. No finite generic audit horizon is safe for unresolved financial
proof; growing old does not establish that a submitted transaction failed.

A later finite retention policy requires a new reviewed eligibility/archival
contract that protects unresolved signed submissions and confirms every reader
below. The stock cleanup_tasks deletes completed, failed AND cancelled tasks,
all their runs and checkpoints. cleanup_events separately removes immutable
first-write-wins event history. Neither function expresses this financial
eligibility contract. Do not substitute a generic TTL or current-task state for
proof that no unresolved attempt remains.

No cleanup may run before explicit owner approval. Any later cleanup also needs
separate runtime/data authority, an exact dry-run inventory and bounded batch
limits, before/after counts, retained proof/receipt identifiers and a verified
reader-compatibility/rollback plan. This draft implements no cleanup mechanism,
schedule, queue deletion or data mutation.

## Current reader census

Reviewed GVA e253489297e134fead3a08ce9fc0d3b4e20fc2bc, then canonical integration
dc18464ccd8508a9e0916de59ec61e2ced12c126. Absurd source main
b58d47c85d91c600ba073f222fa29d4403d91b9c and GVA gitlink
a25436dc48f0f60ecd80c69e5ae887303217bf82 have the same runtime SQL/SDK for this
contract; do not restore an old historical pin to match triage.

Direct application SQL readers and their adapters:

| Reader | State it needs |
|---|---|
| lib/workflows/index.ts: queueHasAnyWork, assertWorkflowTaskSucceeded | t/r active state, last attempt, failure reason and exact spawned-task success. Missing rows are errors, not success. |
| lib/nav-custody-floor.ts: taskCheckpointStatesStatement and readRegisteredTaskCheckpoints | Current task and last run via get_task_checkpoint_states; committed own-write and unknown-finality checkpoints, including earlier attempts. |
| lib/optional-refusal-store.ts: readFirstOptionalRefusalRun | Exact first running task/run and absence of any sibling attempt; retained run history is part of the no-send certificate. |
| lib/p0-terminal-recovery.ts: p0PriorSubmissionsStatement | Prior task/checkpoint signed packets across terminal states, explicitly with no time horizon, until matching exact grouped execution receipts exist. |
| lib/workflows/stablecoin/p0-terminal-submissions.ts | Executes the prior-submission statement and finality/accounting reconciliation; missing historical proof cannot become permission to resend. |
| lib/atomic-migration-recovery.ts: atomicMigrationSubmissionsStatement | Prior task/checkpoint migration submissions and two-leg receipt evidence, including cancelled/failed tasks with unresolved finality. |
| lib/workflows/stablecoin/atomic-migration-recovery.ts | Executes and reconciles that statement before new migration work. |

The vendored Absurd TypeScript TaskContext is the other reader boundary. Its
step checkpoints, sleep/awaitEvent state, cancellation checks and
heartbeat/worker claim/fail/complete paths read queue tables through SQL runtime
functions. Every registered task using these APIs inherits that retention need;
removing checkpoints can replay a financial side effect. The GVA runtime owners
portfolio-agent.ts, claim-rewards.ts and gmbtc-agent.ts provision/spawn/process
through that same SDK and workflow helpers, rather than owning independent
cleanup semantics. Whole queues must not be truncated in production; the
existing non-production guarded clear helper is not a retention policy.

Operator readers include Habitat task/run/checkpoint detail, absurdctl inspection
and forensic/audit/replay tools. Retain their recoverability contract alongside
the application readers; source search cannot prove external consumer absence.

Census procedure: search non-test application source for getWorkflowPool,
absurdQueueTableIdentifier, get_task_checkpoint, cleanup_tasks/cleanup_events and
literal absurd t_/r_/c_ references, then follow every shared SQL statement and
SDK TaskContext caller. No GVA cleanup_tasks or cleanup_events caller exists in
the reviewed application source. Test-only fixtures and unrelated public
cross-chain tables are not extra Absurd readers. Refresh this census before any
retention eligibility change.

## State index and manual migration

New queues receive a non-unique btree t_<queue>(state) index in ensure_queue_tables.
Existing create_queue calls return early; use the reviewed
[manual migration draft](../sql/migrations/manual-task-state-index.sql) only
under separate operator authority. It changes the queue factory and builds one
index per existing queue concurrently, outside any transaction. Identifiers are
quoted and byte-length validated. Invalid or conflicting existing relations
cause a hard stop; never drop an index automatically to bypass that check.
Legacy incompatible queue names require their own reviewed plan.

The migration has a two-second lock timeout and ten-minute per-statement timeout.
An interrupted concurrent build can leave an invalid index; inspect that exact
index and prepare separate repair rather than trusting IF NOT EXISTS. Run only
against freshly enumerated intended queues, retain the exact plan/input/source,
and verify every index is ready/valid with its correct state column. Reconcile
any GVA runtime-function source pin/fixture contract before live installation;
this file is a draft, not an automated schema rollout.

Local Postgres16 proof used 100,000 completed tasks and ten pending tasks with
ordinary planner settings. The claim_task cancellation predicate changed from
Seq Scan to the task-state index (15.503 ms to 0.074 ms in that local fixture).
Actual nested auto_explain output from claim_task also names that index. These
are local qualification results, not production savings or proof of a live
index. Current production index/size/state and migration acceptance remain
separate operational evidence.
