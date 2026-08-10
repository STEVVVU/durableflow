from __future__ import annotations

import json
import tempfile
import threading
import time
from contextlib import ExitStack
from pathlib import Path
from typing import Any

from durableflow.store import JobStore


def run_benchmark(
    *,
    jobs: int = 2000,
    workers: int = 8,
    claim_batch: int = 16,
    database_url: str | None = None,
) -> dict[str, Any]:
    """Measure durable submission and claim-plus-completion throughput.

    Passing ``database_url`` benchmarks a real backend (for example PostgreSQL,
    which claims with ``FOR UPDATE SKIP LOCKED``); otherwise a temporary local
    SQLite WAL database is used. Every state transition still commits with its
    audit event, and neither submissions nor completions are batched, so the
    numbers stay comparable across backends.
    """
    if jobs < 1 or workers < 1 or claim_batch < 1:
        raise ValueError("jobs, workers, and claim_batch must be positive")
    with ExitStack() as stack:
        if database_url:
            store = JobStore(database_url)
            # Reuse of a persistent database must start from a clean queue, or
            # leftover rows would corrupt the completion assertions below.
            store.purge()
            description = store.engine.url.render_as_string(hide_password=True)
        else:
            directory = stack.enter_context(
                tempfile.TemporaryDirectory(prefix="durableflow-bench-")
            )
            store = JobStore(Path(directory) / "benchmark.db")
            description = "temporary local SQLite WAL"
        stack.callback(store.close)

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
            "benchmark": (
                "postgres_skip_locked_claim_and_complete"
                if store.uses_skip_locked
                else "sqlite_wal_claim_and_complete"
            ),
            "jobs": jobs,
            "workers": workers,
            "claim_batch": claim_batch,
            "submit_seconds": round(submit_seconds, 6),
            "submit_jobs_per_second": round(jobs / submit_seconds, 2),
            "drain_seconds": round(drain_seconds, 6),
            "drain_jobs_per_second": round(jobs / drain_seconds, 2),
            "end_to_end_jobs_per_second": round(jobs / (submit_seconds + drain_seconds), 2),
            "completed": completed,
            "database": description,
            "claim_strategy": (
                "SELECT ... FOR UPDATE SKIP LOCKED"
                if store.uses_skip_locked
                else "BEGIN IMMEDIATE + conditional UPDATE"
            ),
        }
        return result


def main() -> None:
    import argparse

    from durableflow import observability

    argument_parser = argparse.ArgumentParser(
        prog="durableflow.benchmark",
        description="Reproducible claim-and-complete throughput benchmark",
    )
    argument_parser.add_argument("--jobs", type=int, default=2000)
    argument_parser.add_argument("--workers", type=int, default=8)
    argument_parser.add_argument("--claim-batch", type=int, default=16)
    argument_parser.add_argument(
        "--database",
        default=None,
        help="Benchmark this URL instead of a temporary SQLite database.",
    )
    arguments = argument_parser.parse_args()
    # Benchmark output is the JSON document on stdout; keep logs from interleaving.
    observability.configure_logging(level="WARNING", json_logs=True)
    print(
        json.dumps(
            run_benchmark(
                jobs=arguments.jobs,
                workers=arguments.workers,
                claim_batch=arguments.claim_batch,
                database_url=arguments.database,
            ),
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
