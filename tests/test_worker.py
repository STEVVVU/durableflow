from __future__ import annotations

import time
from pathlib import Path

from durableflow.models import JobStatus
from durableflow.store import JobStore
from durableflow.worker import Worker


def test_bounded_worker_retries_and_completes(tmp_path: Path) -> None:
    store = JobStore(tmp_path / "worker.db", base_backoff=0.01, max_backoff=0.01)
    jobs = [
        store.submit(
            {"task": "flaky", "failures_before_success": 1},
            max_attempts=2,
        ).job
        for _ in range(6)
    ]
    worker = Worker(
        store,
        worker_id="test-worker",
        concurrency=2,
        lease_seconds=1,
        poll_interval=0.005,
    )
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        worker.run_once()
        if all(store.get(job.id).status == JobStatus.SUCCEEDED for job in jobs):
            break
        time.sleep(0.005)
    worker.shutdown()

    completed = [store.get(job.id) for job in jobs]
    assert all(job.status == JobStatus.SUCCEEDED for job in completed)
    assert all(job.attempts == 2 for job in completed)
    # Concurrency is bounded, so the worker must not have claimed each job
    # repeatedly beyond its two attempts.
    assert store.metrics()["events"].get("claimed", 0) <= 12
    store.close()


def test_worker_dead_letters_unknown_task(tmp_path: Path) -> None:
    """An unregistered task name is a failure, consuming the attempt budget."""
    store = JobStore(tmp_path / "unknown.db", base_backoff=0.01, max_backoff=0.01)
    job = store.submit({"task": "does-not-exist"}, max_attempts=1).job
    worker = Worker(store, worker_id="unknown-task-worker", concurrency=1, lease_seconds=5)

    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        worker.run_once()
        if store.get(job.id).status == JobStatus.DEAD_LETTER:
            break
        time.sleep(0.005)
    worker.shutdown()

    final = store.get(job.id)
    assert final.status == JobStatus.DEAD_LETTER
    assert "unknown task" in (final.last_error or "")
    store.close()


def test_worker_respects_queue_routing(tmp_path: Path) -> None:
    """A worker claims only from its own queue."""
    store = JobStore(tmp_path / "queues.db", base_backoff=0.01, max_backoff=0.01)
    other = store.submit({"task": "echo", "value": 1}, queue="other").job
    mine = store.submit({"task": "echo", "value": 2}, queue="mine").job
    worker = Worker(store, worker_id="mine-worker", queue="mine", concurrency=1)

    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        worker.run_once()
        if store.get(mine.id).status == JobStatus.SUCCEEDED:
            break
        time.sleep(0.005)
    worker.shutdown()

    assert store.get(mine.id).status == JobStatus.SUCCEEDED
    assert store.get(other.id).status == JobStatus.QUEUED
    store.close()
