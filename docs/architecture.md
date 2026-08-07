# Architecture

DurableFlow is a single-node durable job orchestration service. Independent API,
coordinator, and worker processes communicate through a SQLite database in WAL
mode. The design exercises the same ownership and failure boundaries as a
larger queueing system without requiring external infrastructure.

```text
 clients
    |
    v
+---------+        submit/query/cancel
| HTTP API| -------------------------------+
+---------+                                |
                                           v
+-------------+  expire/requeue      +------------+  claim/heartbeat/finish
| coordinator | -------------------> | SQLite WAL | <----------------------+
+-------------+                      +------------+                        |
                                           ^                               |
                                           |                               |
                                      +---------+                           |
                                      | workers | --------------------------+
                                      +---------+
```

## Components

- **API:** A threaded HTTP server that validates submissions and exposes job
  state, event history, cancellation, dead-letter redrive, health, and
  Prometheus text metrics.
- **Coordinator:** A stateless sweep loop. It transitions expired running jobs
  to delayed retries, dead-letter, or cancelled. Multiple coordinators are
  safe because each sweep uses a write transaction and conditional state
  transitions.
- **Worker:** A bounded thread pool. It claims only its free capacity,
  heartbeats active leases, cooperatively observes cancellation, and reports a
  terminal result or retryable failure.
- **Store:** The consistency boundary. `BEGIN IMMEDIATE` serializes claim
  selection with updates. Every state transition and audit event commits in
  one transaction.

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
2. The store begins an immediate write transaction and selects eligible jobs.
3. Each selected job atomically becomes `running`, increments its attempt, and
   receives a random lease token and expiry.
4. Heartbeat, completion, failure, and cancellation acknowledgement require
   the matching worker, token, running state, and an unexpired lease.
5. A coordinator makes expired work available only after exponential backoff.

The token is a fencing value. A recovered job gets a different token, so a
late completion from the previous worker is rejected even if it arrives after
redelivery.

## Scaling boundary

SQLite deliberately keeps this project locally reproducible. WAL permits
concurrent readers, while write transactions serialize at the database. This
is appropriate for a single host and moderate queues; it is not multi-host
storage or a highly available control plane. The store interface is the
boundary at which PostgreSQL or a replicated log could replace SQLite without
changing worker task semantics.
