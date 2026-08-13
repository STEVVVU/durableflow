# DurableFlow

DurableFlow is a locally runnable job orchestration service with real
API/coordinator/worker boundaries. It persists jobs and transition history in
SQLite WAL, assigns fenced worker leases, recovers abandoned work, retries with
exponential backoff, and isolates exhausted jobs for operator redrive.

It runs on Python 3.11+ with a pip-installable dependency set. The default
backend is a local SQLite file, so no external service is required to run it,
test it, or reproduce its measurements. PostgreSQL is an opt-in backend
selected by a connection string.

## What it implements

- Transactional job submission with queue-scoped idempotency keys
- Atomic concurrent claims, expiring leases, heartbeats, and stale-owner fencing
- Bounded worker concurrency and queue routing
- Exponential retry scheduling, attempt limits, and dead-letter redrive
- Queued and cooperative in-flight cancellation
- Stateless coordinator restart and worker-loss recovery
- Persisted event history, worker liveness, state metrics, and Prometheus output
- Deterministic crash/retry/recovery assertions and a fixed local load benchmark

The execution guarantee is **at least once**. External side effects must be
idempotent; see [the failure model](docs/failure-model.md).

## Run locally

Create a virtual environment and install the package. The default database is a
local SQLite file, so this is the whole setup:

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
python -m pip install -e ".[dev]"
```

Start each boundary in a separate terminal against the same database:

```powershell
durableflow --database .durableflow/jobs.db api
durableflow --database .durableflow/jobs.db coordinator
durableflow --database .durableflow/jobs.db worker --concurrency 4
```

Submit a job through the CLI:

```powershell
durableflow --database .durableflow/jobs.db submit `
  '{"task":"flaky","failures_before_success":1}' `
  --idempotency-key demo-1 --max-attempts 3
```

### Opt in to PostgreSQL

SQLite is the zero-setup default. To run against PostgreSQL instead, install the
extra and point `DURABLEFLOW_DATABASE_URL` (or `--database`) at your own server,
then apply the migrations:

```powershell
python -m pip install -e ".[postgres]"
$env:DURABLEFLOW_DATABASE_URL = "postgresql+psycopg://user:password@host:5432/durableflow"
durableflow migrate
```

The backend changes the claim primitive, not the guarantees: PostgreSQL claims
with `SELECT ... FOR UPDATE SKIP LOCKED` so concurrent claimants skip each
other's locked rows, while SQLite serializes claims under the `BEGIN IMMEDIATE`
write lock. Both issue the same fenced update afterwards. Provisioning a
PostgreSQL server is out of scope for this repository.

The HTTP surface includes:

| Method | Path | Purpose |
| --- | --- | --- |
| `POST` | `/v1/jobs` | Submit; accepts `Idempotency-Key` |
| `GET` | `/v1/jobs` | List, optionally by `status` |
| `GET` | `/v1/jobs/{id}` | Read current durable state |
| `GET` | `/v1/jobs/{id}/events` | Read ordered transition history |
| `POST` | `/v1/jobs/{id}/cancel` | Cancel queued/running work |
| `POST` | `/v1/jobs/{id}/redrive` | Reset and requeue a dead-letter job |
| `GET` | `/metrics` | Prometheus text exposition |
| `GET` | `/healthz` | Process health |

Built-in task types are `echo`, `sleep`, `flaky`, `sha256`, and
`ledger_write`. They keep the worker executable useful while making failure
behavior straightforward to reproduce.

## Verify failure behavior

```powershell
ruff check .
pytest
python -m durableflow.chaos
python -m durableflow.benchmark
```

All four run against SQLite and need no external service. The PostgreSQL tests
are marked `postgres` and skip unless `DURABLEFLOW_TEST_POSTGRES_URL` names a
reachable database, so a clean checkout never fails for want of a server:

```powershell
$env:DURABLEFLOW_TEST_POSTGRES_URL = "postgresql+psycopg://user:password@host:5432/durableflow_test"
pytest -m postgres
```

The chaos harness advances a manual clock rather than sleeping. It asserts an
idempotent duplicate submission, simulated worker crash, lease recovery,
second delivery, stale completion rejection, side-effect deduplication,
dead-letter exhaustion, and cancellation. Its JSON output includes the exact
persisted event sequence.

## Measured evidence

One local run on July 16, 2026 used Windows, CPython 3.11.9, a temporary local
SQLite WAL database, 2,000 jobs, 8 claimant threads, and claim batches of 16:

| Phase | Result |
| --- | ---: |
| Durable individual submissions | 70.97 jobs/s (28.179 s) |
| Transactional claim + event + individual completion | 138.02 jobs/s (14.491 s) |
| End to end | 46.87 jobs/s |
| Completed / lost / duplicated claims | 2,000 / 0 / 0 |

This is one development-machine observation, not a production capacity claim.
The benchmark includes a commit and audit event per state transition and does
not batch submissions or completions. Reproduce it with
`python -m durableflow.benchmark`; JSON output records all parameters.

The final local verification passed 41 tests in 25.10 seconds, including an
in-process FastAPI contract test; the 5 PostgreSQL tests skipped, since no
database was configured. CI runs lint, the SQLite suite, and the chaos harness
on Python 3.11-3.13, and runs the PostgreSQL-marked tests plus the chaos
harness against a PostgreSQL service the GitHub runner provides.

## Design

The [architecture](docs/architecture.md) describes component ownership,
transaction boundaries, schema, claim protocol, and the intentional
single-node scaling boundary. The [failure model](docs/failure-model.md)
documents guarantees, races, side-effect ambiguity, and non-goals.

```text
durableflow/api.py          HTTP contract and Prometheus endpoint
durableflow/store.py        schema, transactions, leases, retry state machine
durableflow/coordinator.py  stateless expiry recovery loop
durableflow/worker.py       bounded claims, heartbeats, task execution
durableflow/chaos.py        deterministic crash/retry/recovery trace
durableflow/benchmark.py    reproducible concurrent claim benchmark
tests/                      store, failure, concurrency, worker, and API tests
```

## Resume bullets

- Built a durable Python job orchestration service with separate API,
  coordinator, and worker processes, SQLite WAL persistence, atomic concurrent
  claims, fenced leases/heartbeats, cancellation, exponential retries, and
  dead-letter redrive.
- Designed and tested at-least-once crash recovery: persisted transition
  events, stateless coordinator restart, stale-worker rejection, idempotent
  submissions/effects, and bounded concurrent execution.
- Created deterministic chaos and reproducible load harnesses; verified 2,000
  durable jobs with zero lost or duplicate claims and measured 138 claim-plus-
  completion transitions per second on a local Windows development machine.

## License

MIT
