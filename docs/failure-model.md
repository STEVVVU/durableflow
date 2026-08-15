# Failure Model

## Guarantees

These hold identically on PostgreSQL and SQLite. The backends differ only in how
a claim obtains isolation (`SELECT ... FOR UPDATE SKIP LOCKED` versus
`BEGIN IMMEDIATE`); both then issue the same conditional, fenced writes, and
`tests/test_postgres.py` replays the fencing and chaos assertions against
PostgreSQL to demonstrate it.

- **Durable accepted jobs:** A successful submit response follows the database
  transaction commit.
- **At-least-once execution:** A worker can execute a handler and fail before
  recording completion. The coordinator will eventually redeliver it.
- **No concurrent valid owners:** A claim transaction assigns one active
  lease. Expired or replaced lease tokens cannot heartbeat, fail, cancel, or
  complete the job.
- **Idempotent submission:** Reusing a key in the same queue returns the
  original job, including when the new payload differs.
- **Bounded attempts:** Explicit failures and lease losses consume attempts.
  Exhausted jobs enter `dead_letter` and require operator redrive.
- **Restart recovery:** Durable state contains all scheduling information.
  API, worker, and coordinator processes keep no required in-memory state.

## Failure cases

| Failure | Result |
| --- | --- |
| Worker exits before starting a claim | Lease expires; coordinator schedules retry |
| Worker exits after an external side effect | Job may run again; handler must deduplicate |
| Old worker returns after lease expiry | Heartbeat/completion is rejected by expiry and token fence |
| Coordinator exits | Running leases remain persisted; another coordinator resumes sweeping |
| API exits after commit but before response | Client retries with the same idempotency key |
| Handler fails repeatedly | Exponential backoff, then dead-letter |
| Cancellation races with completion | Store transaction orders them; accepted cancellation prevents success |

Backoff is `min(max_backoff, base_backoff * 2^(attempt-1))`. The deterministic
harness uses a manual clock and no jitter so every transition is reproducible.
A production deployment would normally add bounded jitter to reduce retry
synchronization.

## Side effects

Lease fencing protects DurableFlow state, not arbitrary external services.
Handlers must use an idempotency key at the side-effect destination or an
outbox/inbox transaction. The included `ledger_write` task demonstrates
deduplication with a unique `(job_id, effect_key)` record. It does not claim
exactly-once execution.

## Backend-specific contention

SQLite permits exactly one writer. With N claimant threads the write lock forms a
convoy: measured median wait stays a few milliseconds, but the tail grows with
concurrency and batch size. A writer that exceeds `busy_timeout` raises
`database is locked`; the transaction never partially applies, and the job simply
stays claimable, so this is an availability limit and not a correctness one. The
default wait is 60s (`SQLITE_BUSY_TIMEOUT_SECONDS`) after 10s proved too tight at
8 claimants draining thousands of jobs.

PostgreSQL does not have this bottleneck: `SKIP LOCKED` means concurrent
claimants take disjoint row locks and never queue behind one another, which is
the main reason it is the production backend.

## Explicit non-goals and limits

- SQLite is a single-host, single-writer persistence layer, not replicated
  consensus. PostgreSQL raises the concurrency ceiling but is still one database,
  not a replicated multi-region control plane.
- There is no authentication, authorization, TLS termination, tenant quota,
  or payload encryption.
- Cancellation is cooperative while a handler is executing. A lost worker's
  cancellation becomes final when its lease expires.
- Wall-clock changes can affect lease timing. Leases are wall-clock intervals
  measured against one database, so severe skew between hosts affects timing.
- Event rows grow without retention or archival in this version.
- There is no deterministic workflow replay, multi-step workflow orchestration,
  or versioning of in-flight work. See [the Temporal comparison](vs-temporal.md)
  for what that would require and why it is out of scope.
