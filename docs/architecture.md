# Architecture

DurableFlow is a durable job orchestration service. Independent API,
coordinator, and worker processes communicate through one SQL database:
PostgreSQL in production, or SQLite in WAL mode for zero-setup local runs. The
design exercises the same ownership and failure boundaries as a larger queueing
system, and the two backends differ only in how a claim acquires isolation.

```text
 clients
    |
    v
+---------+        submit/query/cancel
| HTTP API| -------------------------------+
| FastAPI |                                |
+---------+                                v
+-------------+  expire/requeue      +----------------+  claim/heartbeat/finish
| coordinator | -------------------> |  PostgreSQL    | <---------------------+
+-------------+                      |  or SQLite WAL |                       |
                                     +----------------+                       |
                                           ^                                  |
                                           |                                  |
                                      +---------+                             |
                                      | workers | ----------------------------+
                                      +---------+
```

## Components

- **API:** A FastAPI application served by uvicorn. Pydantic v2 models define
  the request/response contracts (published as OpenAPI), and it exposes job
  state, event history, cancellation, dead-letter redrive, liveness, readiness,
  and Prometheus metrics.
- **Coordinator:** A stateless sweep loop. It transitions expired running jobs
  to delayed retries, dead-letter, or cancelled. Multiple coordinators are
  safe because each sweep uses a write transaction and conditional state
  transitions.
- **Worker:** A bounded thread pool. It claims only its free capacity,
  heartbeats active leases, cooperatively observes cancellation, and reports a
  terminal result or retryable failure.
- **Store:** The consistency boundary, built on SQLAlchemy 2.x Core. Every state
  transition and audit event commits in one transaction. Isolation for the claim
  is dialect-specific (see "Claim protocol").

## Persistence and migrations

The schema is declared once as a SQLAlchemy declarative model (`orm.py`) and
applied two ways: `alembic upgrade head` for real deployments, and
`create_all` for tests and the local SQLite path. A test asserts the two produce
identical schemas, so migrated production cannot drift from metadata-built tests.

Timestamps are epoch seconds in 8-byte `Double` columns. The width is
load-bearing: lease expiry is a strict `>` comparison against one of these
values, so a 4-byte real would round an epoch timestamp to roughly minute
resolution and silently break fencing. JSON-bearing columns hold canonical
(sorted-key) JSON as `TEXT`, so the same bytes persist on both backends and
failure traces stay byte-comparable.

## Data model

`jobs` contains the state machine, retry policy, availability time, lease
owner/token/expiry, cancellation flag, and result. `job_events` is an
append-only transition history used for debugging and counters. `workers`
contains ephemeral liveness snapshots. `effects` demonstrates a job-scoped
unique key for idempotent handler side effects.

The queue index is ordered by `(queue, status, available_at, created_at)`.
Idempotency is enforced by a partial unique index on
`(queue, idempotency_key)`.

## Claim protocol

1. A worker reports its currently free capacity.
2. The store opens a write transaction and selects eligible jobs.
3. Each selected job atomically becomes `running`, increments its attempt, and
   receives a random lease token and expiry.
4. Heartbeat, completion, failure, and cancellation acknowledgement require
   the matching worker, token, running state, and an unexpired lease.
5. A coordinator makes expired work available only after exponential backoff.

The token is a fencing value. A recovered job gets a different token, so a
late completion from the previous worker is rejected even if it arrives after
redelivery.

Step 2 is the one place the backends genuinely differ:

- **PostgreSQL** uses `SELECT ... FOR UPDATE SKIP LOCKED`, the industry-standard
  queue-claim primitive. Candidate rows are locked, and any row already locked by
  a concurrent claimant is skipped rather than waited on, so claim throughput
  scales with the number of claimants.
- **SQLite** issues `BEGIN IMMEDIATE`, taking the database-wide write lock before
  the selection, so the claim's read cannot interleave with another claimant's
  write. Correct, but claims serialise globally.

Both then perform the identical conditional `UPDATE ... WHERE id = ? AND status =
'queued'`, so a row is only claimed if it is still claimable. The fencing
guarantee is therefore backend-independent — only contention behaviour differs,
and `tests/test_postgres.py` replays the same fencing and chaos assertions
against PostgreSQL to prove it.

`BEGIN IMMEDIATE` is emitted **only** for write transactions. SQLAlchemy fires
its `begin` event for read connections too, and promoting those would make every
read take the write lock — serialising readers against writers and negating WAL
mode entirely. Read paths use a separate non-transactional helper.

## Scaling boundary

SQLite keeps the project locally reproducible with no external services: WAL
permits concurrent readers while write transactions serialise at the database.
That is appropriate for a single host and moderate queues, but it is a single
writer, so it is the local/test backend rather than the production one.

PostgreSQL raises that ceiling. `SKIP LOCKED` allows many workers to claim
concurrently without blocking each other, and the API and worker processes are
stateless, so both scale horizontally (`docker compose up --scale worker=3`).
The remaining boundary is that this is still one database, not a replicated
multi-region control plane; see [the Temporal comparison](vs-temporal.md) for
where that boundary sits relative to a full durable-execution engine.
