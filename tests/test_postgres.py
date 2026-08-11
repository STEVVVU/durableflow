"""PostgreSQL-backed tests.

These are skipped unless ``DURABLEFLOW_TEST_POSTGRES_URL`` is set, so the default
local suite needs no database. They exist to prove that the
``SELECT ... FOR UPDATE SKIP LOCKED`` claim path preserves exactly the same
guarantees as the SQLite path -- in particular that concurrent claimants skipping
each other's locked rows still yields at-most-one-owner delivery.
"""

from __future__ import annotations

import threading

import pytest

from durableflow.chaos import run_chaos_scenario
from durableflow.clock import ManualClock
from durableflow.models import JobStatus
from durableflow.store import JobStore

from .conftest import require_postgres

pytestmark = pytest.mark.postgres


def test_postgres_store_uses_skip_locked(postgres_store: JobStore) -> None:
    assert postgres_store.uses_skip_locked
    assert postgres_store.engine.dialect.name == "postgresql"


def test_concurrent_skip_locked_claims_are_unique(postgres_store: JobStore) -> None:
    """The core guarantee: SKIP LOCKED must not deliver one job twice."""
    job_count = 200
    for number in range(job_count):
        postgres_store.submit({"task": "echo", "value": number})

    worker_count = 8
    barrier = threading.Barrier(worker_count)
    claimed_ids: list[str] = []
    lock = threading.Lock()

    def claim_until_empty(worker_number: int) -> None:
        barrier.wait()
        while True:
            claimed = postgres_store.claim(
                f"pg-worker-{worker_number}", limit=8, lease_seconds=60
            )
            if not claimed:
                return
            with lock:
                claimed_ids.extend(job.id for job in claimed)

    threads = [
        threading.Thread(target=claim_until_empty, args=(number,))
        for number in range(worker_count)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert len(claimed_ids) == job_count
    assert len(set(claimed_ids)) == job_count


def test_postgres_fencing_rejects_superseded_owner(
    postgres_store: JobStore, clock: ManualClock
) -> None:
    job = postgres_store.submit({"task": "echo"}, max_attempts=3).job
    first = postgres_store.claim("pg-worker-1", lease_seconds=5)[0]

    clock.advance(5)
    assert postgres_store.recover_expired() == 1
    clock.advance(postgres_store.backoff_for(1))
    second = postgres_store.claim("pg-worker-2", lease_seconds=5)[0]

    assert second.lease_token != first.lease_token
    assert not postgres_store.complete(
        job.id, "pg-worker-1", first.lease_token or "", {"stale": True}
    )
    assert postgres_store.complete(
        job.id, "pg-worker-2", second.lease_token or "", {"ok": True}
    )
    assert postgres_store.get(job.id).status == JobStatus.SUCCEEDED


def test_postgres_idempotency_and_redrive(postgres_store: JobStore) -> None:
    first = postgres_store.submit({"task": "echo"}, idempotency_key="pg-1")
    duplicate = postgres_store.submit({"task": "echo"}, idempotency_key="pg-1")
    assert first.created and not duplicate.created
    assert first.job.id == duplicate.job.id

    job = postgres_store.submit({"task": "echo"}, max_attempts=1).job
    lease = postgres_store.claim("pg-worker")[0]
    assert (
        postgres_store.fail(job.id, "pg-worker", lease.lease_token or "", "poison")
        == JobStatus.DEAD_LETTER
    )
    assert postgres_store.redrive(job.id)
    assert postgres_store.get(job.id).attempts == 0


def test_chaos_scenario_passes_on_postgres() -> None:
    """The full deterministic failure trace, replayed against PostgreSQL."""
    url = require_postgres()
    result = run_chaos_scenario(database_url=url)
    assert result["backend"] == "postgresql"
    assert result["claim_strategy"] == "SELECT ... FOR UPDATE SKIP LOCKED"
    assert all(result["assertions"].values())
