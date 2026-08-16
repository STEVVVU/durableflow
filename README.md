# DurableFlow

DurableFlow is a durable job orchestration service with real
API/coordinator/worker boundaries. It persists jobs and transition history in
PostgreSQL or SQLite, assigns fenced worker leases, recovers abandoned work,
retries with exponential backoff, and isolates exhausted jobs for operator
redrive.

It runs on Python 3.11+ with a pip-installable dependency set. The default
backend is a local SQLite file, so no external service is required to run it,
test it, or reproduce its measurements. PostgreSQL is the production backend,
selected by a connection string.

Not a Temporal clone, and deliberately so — see
[how it compares to Temporal](docs/vs-temporal.md) for the mechanism
differences and an explicit list of what this engine does *not* do.

## Stack and why

| Dependency | Role | Why it is here |
| --- | --- | --- |
| `fastapi` + `uvicorn` | HTTP API and ASGI server | Replaces a hand-rolled `BaseHTTPRequestHandler` with routed endpoints and generated OpenAPI |
| `pydantic` v2 | Request/response contracts | Declarative validation instead of manual `isinstance`/`int()` coercion |
| `sqlalchemy` 2.x | Typed persistence | One schema for two dialects, and `SKIP LOCKED` without hand-writing per-dialect SQL |
| `alembic` | Schema migrations | Versioned schema changes; a test asserts migrations match the ORM metadata |
| `psycopg[binary]` | PostgreSQL driver | The production backend's `FOR UPDATE SKIP LOCKED` claim path |
| `structlog` | Structured logging | Queryable JSON events keyed by job/worker/attempt |
| `prometheus-client` | Metrics | Real counters/gauges for queue depth, retries, DLQ, and lease expiry |
| `opentelemetry-sdk` + FastAPI instrumentation | Tracing | One span per HTTP request and per job attempt |

## What it implements

- Transactional job submission with queue-scoped idempotency keys
- Atomic concurrent claims, expiring leases, heartbeats, and stale-owner fencing
- Bounded worker concurrency and queue routing
- Exponential retry scheduling, attempt limits, and dead-letter redrive
- Queued and cooperative in-flight cancellation
- Stateless coordinator restart and worker-loss recovery
- PostgreSQL `SELECT ... FOR UPDATE SKIP LOCKED` claims, with SQLite for local runs
- Persisted event history, worker liveness, state metrics, and Prometheus output
- Structured logs, Prometheus metrics, and OpenTelemetry traces
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
write lock. Both issue the same fenced update afterwards, and the
PostgreSQL-marked tests replay the fencing and chaos assertions against it to
demonstrate that.

`SKIP LOCKED` is the reason PostgreSQL is the production backend. SQLite permits
exactly one writer, so N claimant threads form a lock convoy; `SKIP LOCKED` lets
claimants take disjoint row locks and never queue behind one another.

### Run the whole stack with Docker Compose

`compose.yaml` brings up the API, coordinator, worker, PostgreSQL, and Prometheus.
Migrations run to completion before any process starts, so nothing races an
incomplete schema:

```powershell
docker compose up --build
docker compose up --build --scale worker=3   # workers are safe to scale: each claim is lease-fenced
```

The API is then on `http://127.0.0.1:8080` (OpenAPI docs at `/docs`) and
Prometheus on `http://127.0.0.1:9090`. The image is built from
`python:3.12-slim`, runs as a non-root user, and its `HEALTHCHECK` polls
`/readyz`, which fails closed when the database is unreachable.

> The container files have **not** been verified by an actual `docker build`: the
> Docker daemon was unavailable in the environment where this change was made.
> `docker compose config` validates, and the wheel the image installs builds and
> exposes the `durableflow` entry point, but treat the image itself as unverified
> until CI's `docker` job (which builds it and probes `/readyz`) runs green.

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
| `GET` | `/healthz` | Liveness: the process is serving |
| `GET` | `/readyz` | Readiness: the database is reachable (503 when not) |
| `GET` | `/docs`, `/openapi.json` | Generated OpenAPI schema |

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

**These numbers were re-measured on July 28, 2026 after the FastAPI/SQLAlchemy
migration.** They are not comparable to the pre-migration figures this README
used to quote, and they are lower on the drain phase — see the honest accounting
below.

Five consecutive runs on Windows, CPython 3.11.9, a temporary local SQLite WAL
database, 2,000 jobs, 8 claimant threads, and claim batches of 16:

| Phase | Median | Range over 5 runs |
| --- | ---: | ---: |
| Durable individual submissions | 79.9 jobs/s | 71.3 - 126.6 |
| Transactional claim + event + individual completion | 60.1 jobs/s | 57.3 - 73.6 |
| End to end | 35.1 jobs/s | 34.1 - 39.5 |
| Completed / lost / duplicated claims | 2,000 / 0 / 0 | identical every run |

Medians are reported because this machine's SQLite commit throughput varies by
roughly 2x run to run: repeating one identical 100-submission measurement six
times spanned 82-162 submissions/s. Any single-run number here would be
misleading, so treat the range as the result and the median as the point
estimate.

### How this compares to the pre-migration stack

Measured on this same machine immediately before the migration (the old
stdlib/`sqlite3` code, so the comparison is like-for-like rather than against the
README's older figures):

| Phase | Before | After (median) |
| --- | ---: | ---: |
| Submissions | 70.4 jobs/s | 79.9 jobs/s |
| Claim + complete | 148.9 jobs/s | 60.1 jobs/s |
| End to end | 47.8 jobs/s | 35.1 jobs/s |

Submissions got faster: SQLAlchemy pools connections, whereas the original store
opened a fresh SQLite connection per transaction. Isolated single-threaded
measurement showed the new path completing ~119 transactions/s against ~23 for
the old connection-per-transaction shape.

The concurrent drain phase got **slower**, and the cause is contention rather
than per-statement overhead. With 8 claimant threads on SQLite's single write
lock, measured wait was p50 3 ms but max 2.7 s — a lock convoy whose tail grows
with concurrency. Statement construction in SQLAlchemy adds real CPU cost per
transaction on top of that. Two consequences worth stating plainly:

- The default `busy_timeout` was raised to 60 s after 10 s proved too tight at
  this concurrency; below that bound the drain intermittently failed with
  `database is locked`. That is a wait bound, not a correctness one — a writer
  that gives up raises, the transaction never partially applies, and the job
  stays claimable.
- This is precisely the bottleneck PostgreSQL's `SKIP LOCKED` removes, which is
  why it is the production backend. The benchmark accepts `--database`, so the
  same harness measures it: `python -m durableflow.benchmark --database <url>`.
  No PostgreSQL figures are quoted here because none were measured on this
  machine; quoting an unmeasured number would defeat the purpose of this section.

These are development-machine observations, not production capacity claims. The
benchmark commits an audit event per state transition and batches neither
submissions nor completions. Reproduce with `python -m durableflow.benchmark`;
the JSON output records every parameter, the backend, and the claim strategy.

The final local verification passed 41 tests in 21.98 seconds, including a live
uvicorn socket test; the 5 PostgreSQL tests skipped, since no database was
configured. CI runs lint, the SQLite suite, and the chaos harness on Python
3.11-3.13; runs the PostgreSQL-marked tests plus the chaos harness against a
PostgreSQL service container; and builds the Docker image and probes `/readyz`.

## Design

The [architecture](docs/architecture.md) describes component ownership,
transaction boundaries, schema, both claim protocols, and the scaling boundary.
The [failure model](docs/failure-model.md) documents guarantees, races,
side-effect ambiguity, backend contention, and non-goals.
[vs-temporal.md](docs/vs-temporal.md) compares this engine's guarantees to
Temporal's and states what it does not do.

```text
durableflow/api.py           FastAPI app, ops endpoints, OpenAPI
durableflow/schemas.py       pydantic v2 request/response contracts
durableflow/store.py         transactions, leases, retry state machine
durableflow/orm.py           SQLAlchemy declarative schema
durableflow/engine.py        backend selection and per-dialect transaction semantics
durableflow/config.py        pydantic-settings configuration
durableflow/observability.py structlog, Prometheus metrics, OTel tracing
durableflow/coordinator.py   stateless expiry recovery loop
durableflow/worker.py        bounded claims, heartbeats, task execution
durableflow/chaos.py         deterministic crash/retry/recovery trace
durableflow/benchmark.py     reproducible concurrent claim benchmark
alembic/                     migrations (initial revision generated from orm.py)
Dockerfile, compose.yaml     container image and the api/worker/postgres/prometheus stack
tests/                       store, failure, concurrency, worker, API, and Postgres tests
```

## Resume bullets

- Built a durable job orchestration service in Python with separate API,
  coordinator, and worker processes: FastAPI/pydantic v2 contracts, SQLAlchemy
  2.x persistence over PostgreSQL or SQLite with alembic migrations, atomic
  concurrent claims, fenced leases/heartbeats, cancellation, exponential
  retries, and dead-letter redrive.
- Implemented the industry-standard `SELECT ... FOR UPDATE SKIP LOCKED` queue
  claim on PostgreSQL and proved it preserves the same fencing guarantees as the
  SQLite path by replaying the fencing and chaos assertions against both.
- Designed and tested at-least-once crash recovery: persisted transition events,
  stateless coordinator restart, stale-worker rejection via monotonic fencing
  tokens, idempotent submissions/effects, and bounded concurrent execution.
- Instrumented with structlog, Prometheus (queue depth, retries, DLQ, lease
  expiry), and OpenTelemetry traces; containerized with a non-root image and a
  Compose stack.
- Created deterministic chaos and reproducible load harnesses; verified 2,000
  durable jobs with zero lost or duplicate claims across repeated runs, and used
  the harness to diagnose and fix a read-path lock-promotion bug that was
  serialising readers against writers.

## License

MIT
