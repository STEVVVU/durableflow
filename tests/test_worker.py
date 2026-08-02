from __future__ import annotations

import tempfile
import time
import unittest
from pathlib import Path

from durableflow.models import JobStatus
from durableflow.store import JobStore
from durableflow.worker import Worker


class WorkerTestCase(unittest.TestCase):
    def test_bounded_worker_retries_and_completes(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = JobStore(
                Path(directory) / "worker.db",
                base_backoff=0.01,
                max_backoff=0.01,
            )
            jobs = [
                store.submit(
                    {
                        "task": "flaky",
                        "failures_before_success": 1,
                    },
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
                if all(
                    store.get(job.id).status == JobStatus.SUCCEEDED
                    for job in jobs
                ):
                    break
                time.sleep(0.005)
            worker.shutdown()

            completed = [store.get(job.id) for job in jobs]
            self.assertTrue(
                all(job.status == JobStatus.SUCCEEDED for job in completed)
            )
            self.assertTrue(all(job.attempts == 2 for job in completed))
            self.assertLessEqual(
                store.metrics()["events"].get("claimed", 0), 12
            )
            store.close()


if __name__ == "__main__":
    unittest.main()
