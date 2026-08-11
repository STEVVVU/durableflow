from __future__ import annotations

import threading
from pathlib import Path

from durableflow.clock import ManualClock
from durableflow.coordinator import Coordinator
from durableflow.models import JobStatus
from durableflow.store import JobStore


def test_idempotency_key_returns_original_job(store: JobStore) -> None:
    first = store.submit({"task": "echo", "value": 1}, idempotency_key="request-1")
    duplicate = store.submit({"task": "echo", "value": 999}, idempotency_key="request-1")

    assert first.created
    assert not duplicate.created
    assert first.job.id == duplicate.job.id
    # The original payload wins; a repeated key never overwrites the stored job.
    assert duplicate.job.payload["value"] == 1


def test_duplicate_delivery_is_fenced_and_effect_is_idempotent(
    store: JobStore, clock: ManualClock
) -> None:
    job = store.submit({"task": "ledger_write", "value": 50}, max_attempts=3).job
    first = store.claim("worker-1", lease_seconds=5)[0]
    _, created = store.record_effect(job.id, "charge", 50)
    assert created
    assert store.claim("worker-2", lease_seconds=5) == []

    clock.advance(5)
    assert store.recover_expired() == 1
    clock.advance(1)
    second = store.claim("worker-2", lease_seconds=5)[0]
    _, created_again = store.record_effect(job.id, "charge", 50)

    assert not created_again
    assert not store.complete(first.id, "worker-1", first.lease_token or "", {"stale": True})
    assert store.complete(second.id, "worker-2", second.lease_token or "", {"ok": True})
    assert store.get(job.id).status == JobStatus.SUCCEEDED


def test_lease_expiry_applies_backoff_before_redelivery(
    store: JobStore, clock: ManualClock
) -> None:
    job = store.submit({"task": "echo"}, max_attempts=2).job
    lease = store.claim("lost-worker", lease_seconds=5)[0]

    clock.advance(5)
    assert not store.complete(
        job.id, "lost-worker", lease.lease_token or "", {"too_late": True}
    )
    assert not store.heartbeat(job.id, "lost-worker", lease.lease_token or "", lease_seconds=5)
    assert store.recover_expired() == 1
    recovered = store.get(job.id)
    assert recovered.status == JobStatus.QUEUED
    assert recovered.last_error == "worker lease expired"
    # Backoff must elapse before the job becomes claimable again.
    assert store.claim("replacement", lease_seconds=5) == []

    clock.advance(1)
    second = store.claim("replacement", lease_seconds=5)[0]
    assert second.attempts == 2


def test_coordinator_restart_recovers_persisted_lease(
    store: JobStore, database_path: Path, clock: ManualClock
) -> None:
    job = store.submit({"task": "echo"}).job
    store.claim("worker-that-dies", lease_seconds=4)
    first_coordinator = Coordinator(store)
    assert first_coordinator.tick() == 0

    clock.advance(4)
    restarted_store = JobStore(database_path, clock=clock, base_backoff=1, max_backoff=10)
    restarted_coordinator = Coordinator(restarted_store)
    assert restarted_coordinator.tick() == 1
    assert restarted_store.get(job.id).status == JobStatus.QUEUED
    events = restarted_store.events(job.id)
    assert "lease_expired_requeued" in [event["event_type"] for event in events]
    restarted_store.close()


def test_concurrent_claims_are_unique(store: JobStore) -> None:
    job_count = 80
    for number in range(job_count):
        store.submit({"task": "echo", "value": number})

    barrier = threading.Barrier(8)
    claimed_ids: list[str] = []
    lock = threading.Lock()

    def claim_until_empty(worker_number: int) -> None:
        barrier.wait()
        while True:
            claimed = store.claim(f"worker-{worker_number}", limit=1, lease_seconds=30)
            if not claimed:
                return
            with lock:
                claimed_ids.append(claimed[0].id)

    threads = [
        threading.Thread(target=claim_until_empty, args=(number,)) for number in range(8)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    # Every job delivered exactly once: no losses and no double delivery.
    assert len(claimed_ids) == job_count
    assert len(set(claimed_ids)) == job_count


def test_retry_dead_letter_and_redrive(store: JobStore) -> None:
    job = store.submit({"task": "unknown"}, max_attempts=1).job
    lease = store.claim("worker")[0]
    status = store.fail(job.id, "worker", lease.lease_token or "", "poison")
    assert status == JobStatus.DEAD_LETTER
    assert store.redrive(job.id)
    redriven = store.get(job.id)
    assert redriven.status == JobStatus.QUEUED
    assert redriven.attempts == 0


def test_running_cancellation_is_acknowledged_by_lease_owner(store: JobStore) -> None:
    job = store.submit({"task": "sleep", "seconds": 10}).job
    lease = store.claim("worker")[0]
    assert store.cancel(job.id)
    assert store.cancellation_requested(job.id, "worker", lease.lease_token or "")
    assert store.acknowledge_cancellation(job.id, "worker", lease.lease_token or "")
    assert store.get(job.id).status == JobStatus.CANCELLED


def test_fencing_token_changes_on_every_redelivery(
    store: JobStore, clock: ManualClock
) -> None:
    """The fencing token must be fresh for each delivery, never reused.

    This is the guarantee that makes a late write from a superseded worker
    rejectable: if a recovered job could be handed back a previously issued
    token, a stale completion would be indistinguishable from the current
    owner's.
    """
    job = store.submit({"task": "echo"}, max_attempts=5).job
    seen_tokens: list[str] = []

    for expected_attempt in range(1, 5):
        lease = store.claim(f"worker-{expected_attempt}", lease_seconds=5)[0]
        assert lease.attempts == expected_attempt
        assert lease.lease_token is not None
        seen_tokens.append(lease.lease_token)

        # Every previously issued token is permanently fenced out.
        for stale_token in seen_tokens[:-1]:
            assert not store.complete(job.id, "worker-1", stale_token, {"stale": True})
            assert not store.heartbeat(job.id, "worker-1", stale_token, lease_seconds=5)
            assert store.fail(job.id, "worker-1", stale_token, "stale") is None

        clock.advance(5)
        store.recover_expired()
        clock.advance(store.backoff_for(expected_attempt))

    assert len(set(seen_tokens)) == len(seen_tokens), "lease tokens must never repeat"


def test_attempts_increase_monotonically_across_recoveries(
    store: JobStore, clock: ManualClock
) -> None:
    """Attempt counts must never regress, so the attempt budget is honoured."""
    store.submit({"task": "echo"}, max_attempts=4)
    observed: list[int] = []
    for _ in range(4):
        claimed = store.claim("worker", lease_seconds=5)
        if not claimed:
            break
        observed.append(claimed[0].attempts)
        clock.advance(5)
        store.recover_expired()
        clock.advance(store.backoff_for(claimed[0].attempts))

    assert observed == sorted(observed)
    assert observed == list(range(1, len(observed) + 1))


def test_exhausted_attempts_dead_letter_via_lease_expiry(
    store: JobStore, clock: ManualClock
) -> None:
    """Lost leases consume attempts and eventually dead-letter, not loop forever."""
    job = store.submit({"task": "echo"}, max_attempts=1).job
    store.claim("doomed-worker", lease_seconds=5)
    clock.advance(5)
    assert store.recover_expired() == 1
    recovered = store.get(job.id)
    assert recovered.status == JobStatus.DEAD_LETTER
    assert "lease_expired_dead_lettered" in [
        event["event_type"] for event in store.events(job.id)
    ]


def test_backoff_is_exponential_and_capped(store: JobStore) -> None:
    assert store.backoff_for(1) == 1
    assert store.backoff_for(2) == 2
    assert store.backoff_for(3) == 4
    assert store.backoff_for(4) == 8
    # base_backoff=1, max_backoff=10 for this fixture, so growth clamps at 10.
    assert store.backoff_for(5) == 10
    assert store.backoff_for(50) == 10


def test_delayed_submission_is_not_immediately_claimable(
    store: JobStore, clock: ManualClock
) -> None:
    store.submit({"task": "echo"}, delay_seconds=10)
    assert store.claim("worker") == []
    clock.advance(10)
    assert len(store.claim("worker")) == 1


def test_cancelled_queued_job_is_never_claimed(store: JobStore) -> None:
    job = store.submit({"task": "echo"}).job
    assert store.cancel(job.id)
    assert store.claim("worker") == []
    assert store.get(job.id).status == JobStatus.CANCELLED


def test_redrive_only_applies_to_dead_letter_jobs(store: JobStore) -> None:
    job = store.submit({"task": "echo"}).job
    assert not store.redrive(job.id)
    assert store.get(job.id).status == JobStatus.QUEUED
