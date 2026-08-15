# DurableFlow compared to Temporal

The durable-execution space is defined by Temporal (and its predecessor Cadence).
Any from-scratch orchestrator invites the question "why not just use Temporal?",
so this document states what DurableFlow shares with it, what it deliberately
does differently, and what it genuinely does not do.

The short version: **DurableFlow is a durable job queue with fenced leases.
Temporal is a durable *execution* engine with deterministic replay.** Those solve
overlapping but distinct problems. DurableFlow makes a job survive worker death;
Temporal makes an arbitrary multi-step *program* survive worker death, resuming
mid-function with its local variables intact.

## The core mechanism differs

| Concern | DurableFlow | Temporal |
| --- | --- | --- |
| Unit of durability | One job row plus its append-only event history | A workflow's full event history, replayed to rebuild in-memory state |
| Recovering in-flight work | Lease expiry: a coordinator requeues work whose lease stopped being renewed | Task-queue timeouts; the workflow is replayed from history onto any worker |
| Preventing two active owners | Fencing token — a fresh UUID per delivery, required by every subsequent write | Task tokens plus history/run versioning; a stale completion is rejected on append |
| Worker affinity | None; any worker may claim any queued job | Sticky execution caches workflow state on one worker, with cache-miss replay |
| Retry policy | Per-job `max_attempts` and exponential backoff in the store | Per-activity `RetryPolicy` (initial interval, coefficient, maximum attempts) |
| Exhausted work | `dead_letter` status, redriven by an operator via HTTP | Activity failure surfaces to the workflow, which decides; no queue-level DLQ |
| Execution semantics | At-least-once; handlers must be idempotent | At-least-once activities, with deterministic workflow replay layered on top |
| Storage | One SQL database (SQLite or PostgreSQL) | A Temporal cluster over Cassandra, MySQL, or PostgreSQL |
| Operational surface | Three processes and a database | A cluster (frontend, history, matching, worker) plus its datastore |

### Fenced leases vs task queues and sticky workers

DurableFlow's claim is a single transaction that flips `queued -> running`,
increments the attempt, and stamps a **new** lease token. Every later write
(`heartbeat`, `complete`, `fail`, `acknowledge_cancellation`) carries that token
and is predicated on it still matching an unexpired lease. A worker that pauses
past its expiry and returns is fenced out: its token is stale, and its write
affects zero rows. This is the Chubby/ZooKeeper fencing-token pattern applied to
a queue row, and `test_fencing_token_changes_on_every_redelivery` asserts that a
recovered job never reuses a previously issued token.

Temporal solves the same "two workers think they own this" problem differently.
Work is dispatched through task queues; a worker holds a task token, and its
completion is rejected if the history has moved on. It adds *sticky execution*:
after the first task, subsequent tasks for that workflow prefer the worker that
already has its state cached, which avoids replaying history on every step. That
optimisation only makes sense because Temporal has state to cache — DurableFlow
jobs are self-contained, so any worker is as good as any other.

### At-least-once plus idempotency vs deterministic replay

DurableFlow guarantees at-least-once execution and pushes the consequences to the
handler: a job can run twice, so external effects must be deduplicated. The
`ledger_write` task demonstrates this with a unique `(job_id, effect_key)` row,
and `record_effect` returns whether it created the record so the handler can tell
a first run from a replay.

Temporal's stronger claim is that *workflow code* resumes as if it never
crashed. It records every decision in an event history and, on recovery, re-runs
the workflow function while feeding results back from history. Reaching a point
already recorded returns the stored result instead of re-executing. The price is
that workflow code must be **deterministic**: no unguarded clocks, randomness, or
map iteration order, since a divergent replay corrupts the reconstruction. That
constraint is exactly what DurableFlow does not impose, because it never replays
anything — and it is why the two systems suit different problems.

### DLQ vs retry policies

DurableFlow treats attempt exhaustion as a first-class terminal state. A job that
runs out of attempts becomes `dead_letter`, stops consuming resources, stays
queryable, and returns via `POST /v1/jobs/{id}/redrive` (which resets attempts
and requeues). Notably, a *lost lease* also consumes an attempt, so a job that
repeatedly kills its worker still terminates instead of looping forever
(`test_exhausted_attempts_dead_letter_via_lease_expiry`).

Temporal expresses retries as a per-activity `RetryPolicy` and has no queue-level
DLQ in this sense. An activity that exhausts its policy raises into the workflow,
which decides what to do — compensate, escalate, or fail. That is more flexible
and more code; DurableFlow's version is a fixed operational contract with an
operator escape hatch.

## What DurableFlow does NOT do

Stated plainly, because these are the honest limits:

- **No deterministic workflow replay.** There is no event-sourced reconstruction
  of in-memory state. A handler restarts from the beginning of the attempt, not
  from where it died.
- **No multi-step workflow orchestration.** No child workflows, signals,
  queries, timers-as-workflow-state, saga compensation, or
  continue-as-new. A job is one unit of work; DurableFlow will not coordinate
  step 3 of 7.
- **No versioning of in-flight work.** Temporal versions workflow code so a
  deploy does not break executions mid-flight. DurableFlow has no equivalent: if
  a handler's meaning changes while jobs of that type are queued, in-flight jobs
  run under the new code with no compatibility shim.
- **No exactly-once side effects.** Fencing protects DurableFlow's own state, not
  a third-party API. Deduplication remains the handler's job.
- **Cooperative cancellation only.** A running handler must poll
  `context.check_cancelled()`. There is no preemption; a wedged handler is
  reclaimed only when its lease expires.
- **No distributed clock.** Leases are wall-clock intervals in one database.
  Severe clock skew between hosts affects lease timing.
- **Single control plane.** PostgreSQL raises the ceiling well above SQLite, but
  this is still one database, not a replicated multi-region control plane.

## When each is the right choice

Use Temporal when the durable thing is a *process*: long-running, multi-step,
needing compensation, human-in-the-loop waits, or safe redeploys mid-execution.
Reimplementing that on job rows means rebuilding history, replay, and versioning
— which is why this project does not attempt it.

DurableFlow's shape fits when the durable thing is a *task*: independently
retryable, idempotent, and self-contained. There the queue-plus-lease model is
appropriate, and a single Postgres table with `FOR UPDATE SKIP LOCKED` is a
well-understood, low-operational-cost design. The trade-off is deliberate: far
less machinery, far fewer guarantees, and a fraction of the operational surface.

The purpose of building it from scratch is to make the mechanisms — fencing
tokens, lease expiry, attempt budgets, backoff, dead-letter redrive — explicit
and testable rather than delegated to a framework. See
[the failure model](failure-model.md) for the guarantees and races in detail.
