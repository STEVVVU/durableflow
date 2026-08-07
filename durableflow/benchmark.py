from __future__ import annotations

import json
import tempfile
import threading
import time
from pathlib import Path
from typing import Any

from durableflow.store import JobStore


def run_benchmark(
    *,
    jobs: int = 2000,
    workers: int = 8,
    claim_batch: int = 16,
) -> dict[str, Any]:
    if jobs < 1 or workers < 1 or claim_batch < 1:
        raise ValueError("jobs, workers, and claim_batch must be positive")
    with tempfile.TemporaryDirectory(prefix="durableflow-bench-") as directory:
        store = JobStore(Path(directory) / "benchmark.db")

        submit_started = time.perf_counter()
        for number in range(jobs):
            store.submit(
                {"task": "echo", "value": number},
                idempotency_key=f"benchmark-{number}",
            )
        submit_seconds = time.perf_counter() - submit_started

        barrier = threading.Barrier(workers)
        completed = 0
        completed_lock = threading.Lock()
        errors: list[str] = []

        def consume(worker_number: int) -> None:
            nonlocal completed
            worker_id = f"benchmark-worker-{worker_number}"
            barrier.wait()
            while True:
                claimed = store.claim(
                    worker_id,
                    limit=claim_batch,
                    lease_seconds=60,
                )
                if not claimed:
                    with completed_lock:
                        if completed >= jobs:
                            return
                    time.sleep(0.001)
                    continue
                for job in claimed:
                    accepted = store.complete(
                        job.id,
                        worker_id,
                        job.lease_token or "",
                        {"value": job.payload["value"]},
                    )
                    if not accepted:
                        errors.append(f"completion rejected for {job.id}")
                    with completed_lock:
                        completed += 1

        drain_started = time.perf_counter()
        threads = [
            threading.Thread(target=consume, args=(number,))
            for number in range(workers)
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        drain_seconds = time.perf_counter() - drain_started

        metrics = store.metrics()
        assert not errors, errors
        assert completed == jobs
        assert metrics["jobs"]["succeeded"] == jobs
        result = {
            "benchmark": "sqlite_wal_claim_and_complete",
            "jobs": jobs,
            "workers": workers,
            "claim_batch": claim_batch,
            "submit_seconds": round(submit_seconds, 6),
            "submit_jobs_per_second": round(jobs / submit_seconds, 2),
            "drain_seconds": round(drain_seconds, 6),
            "drain_jobs_per_second": round(jobs / drain_seconds, 2),
            "end_to_end_jobs_per_second": round(
                jobs / (submit_seconds + drain_seconds), 2
            ),
            "completed": completed,
            "database": "temporary local SQLite WAL",
        }
        store.close()
        return result


def main() -> None:
    print(json.dumps(run_benchmark(), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
