from __future__ import annotations

import tempfile
import threading
import unittest
from pathlib import Path

from durableflow.clock import ManualClock
from durableflow.coordinator import Coordinator
from durableflow.models import JobStatus
from durableflow.store import JobStore


class StoreTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.clock = ManualClock()
        self.database = Path(self.temporary.name) / "test.db"
        self.store = JobStore(
            self.database,
            clock=self.clock,
            base_backoff=1,
            max_backoff=10,
        )

    def tearDown(self) -> None:
        self.store.close()
        self.temporary.cleanup()

    def test_idempotency_key_returns_original_job(self) -> None:
        first = self.store.submit(
            {"task": "echo", "value": 1}, idempotency_key="request-1"
        )
        duplicate = self.store.submit(
            {"task": "echo", "value": 999}, idempotency_key="request-1"
        )

        self.assertTrue(first.created)
        self.assertFalse(duplicate.created)
        self.assertEqual(first.job.id, duplicate.job.id)
        self.assertEqual(1, duplicate.job.payload["value"])

    def test_duplicate_delivery_is_fenced_and_effect_is_idempotent(self) -> None:
        job = self.store.submit(
            {"task": "ledger_write", "value": 50}, max_attempts=3
        ).job
        first = self.store.claim("worker-1", lease_seconds=5)[0]
        _, created = self.store.record_effect(job.id, "charge", 50)
        self.assertTrue(created)
        self.assertEqual([], self.store.claim("worker-2", lease_seconds=5))

        self.clock.advance(5)
        self.assertEqual(1, self.store.recover_expired())
        self.clock.advance(1)
        second = self.store.claim("worker-2", lease_seconds=5)[0]
        _, created_again = self.store.record_effect(job.id, "charge", 50)

        self.assertFalse(created_again)
        self.assertFalse(
            self.store.complete(
                first.id, "worker-1", first.lease_token or "", {"stale": True}
            )
        )
        self.assertTrue(
            self.store.complete(
                second.id, "worker-2", second.lease_token or "", {"ok": True}
            )
        )
        self.assertEqual(JobStatus.SUCCEEDED, self.store.get(job.id).status)

    def test_lease_expiry_applies_backoff_before_redelivery(self) -> None:
        job = self.store.submit({"task": "echo"}, max_attempts=2).job
        lease = self.store.claim("lost-worker", lease_seconds=5)[0]

        self.clock.advance(5)
        self.assertFalse(
            self.store.complete(
                job.id,
                "lost-worker",
                lease.lease_token or "",
                {"too_late": True},
            )
        )
        self.assertFalse(
            self.store.heartbeat(
                job.id,
                "lost-worker",
                lease.lease_token or "",
                lease_seconds=5,
            )
        )
        self.assertEqual(1, self.store.recover_expired())
        recovered = self.store.get(job.id)
        self.assertEqual(JobStatus.QUEUED, recovered.status)
        self.assertEqual("worker lease expired", recovered.last_error)
        self.assertEqual([], self.store.claim("replacement", lease_seconds=5))

        self.clock.advance(1)
        second = self.store.claim("replacement", lease_seconds=5)[0]
        self.assertEqual(2, second.attempts)

    def test_coordinator_restart_recovers_persisted_lease(self) -> None:
        job = self.store.submit({"task": "echo"}).job
        self.store.claim("worker-that-dies", lease_seconds=4)
        first_coordinator = Coordinator(self.store)
        self.assertEqual(0, first_coordinator.tick())

        self.clock.advance(4)
        restarted_store = JobStore(
            self.database,
            clock=self.clock,
            base_backoff=1,
            max_backoff=10,
        )
        restarted_coordinator = Coordinator(restarted_store)
        self.assertEqual(1, restarted_coordinator.tick())
        self.assertEqual(JobStatus.QUEUED, restarted_store.get(job.id).status)
        events = restarted_store.events(job.id)
        self.assertIn(
            "lease_expired_requeued",
            [event["event_type"] for event in events],
        )
        restarted_store.close()

    def test_concurrent_claims_are_unique(self) -> None:
        job_count = 80
        for number in range(job_count):
            self.store.submit({"task": "echo", "value": number})

        barrier = threading.Barrier(8)
        claimed_ids: list[str] = []
        lock = threading.Lock()

        def claim_until_empty(worker_number: int) -> None:
            barrier.wait()
            while True:
                claimed = self.store.claim(
                    f"worker-{worker_number}", limit=1, lease_seconds=30
                )
                if not claimed:
                    return
                with lock:
                    claimed_ids.append(claimed[0].id)

        threads = [
            threading.Thread(target=claim_until_empty, args=(number,))
            for number in range(8)
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        self.assertEqual(job_count, len(claimed_ids))
        self.assertEqual(job_count, len(set(claimed_ids)))

    def test_retry_dead_letter_and_redrive(self) -> None:
        job = self.store.submit({"task": "unknown"}, max_attempts=1).job
        lease = self.store.claim("worker")[0]
        status = self.store.fail(
            job.id, "worker", lease.lease_token or "", "poison"
        )
        self.assertEqual(JobStatus.DEAD_LETTER, status)
        self.assertTrue(self.store.redrive(job.id))
        redriven = self.store.get(job.id)
        self.assertEqual(JobStatus.QUEUED, redriven.status)
        self.assertEqual(0, redriven.attempts)

    def test_running_cancellation_is_acknowledged_by_lease_owner(self) -> None:
        job = self.store.submit({"task": "sleep", "seconds": 10}).job
        lease = self.store.claim("worker")[0]
        self.assertTrue(self.store.cancel(job.id))
        self.assertTrue(
            self.store.cancellation_requested(
                job.id, "worker", lease.lease_token or ""
            )
        )
        self.assertTrue(
            self.store.acknowledge_cancellation(
                job.id, "worker", lease.lease_token or ""
            )
        )
        self.assertEqual(JobStatus.CANCELLED, self.store.get(job.id).status)


if __name__ == "__main__":
    unittest.main()
