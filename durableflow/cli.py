from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Sequence

from durableflow.api import serve
from durableflow.benchmark import run_benchmark
from durableflow.chaos import run_chaos_scenario
from durableflow.coordinator import Coordinator
from durableflow.store import JobStore
from durableflow.worker import Worker


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(
        prog="durableflow",
        description="Durable leased job orchestration",
    )
    root.add_argument(
        "--database",
        default=".durableflow/durableflow.db",
        help="SQLite database path",
    )
    commands = root.add_subparsers(dest="command", required=True)

    api = commands.add_parser("api", help="run the HTTP API")
    api.add_argument("--host", default="127.0.0.1")
    api.add_argument("--port", type=int, default=8080)

    coordinator = commands.add_parser(
        "coordinator", help="run lease recovery"
    )
    coordinator.add_argument("--sweep-interval", type=float, default=1.0)

    worker = commands.add_parser("worker", help="run a task worker")
    worker.add_argument("--worker-id")
    worker.add_argument("--queue", default="default")
    worker.add_argument("--concurrency", type=int, default=4)
    worker.add_argument("--lease-seconds", type=float, default=10.0)
    worker.add_argument("--poll-interval", type=float, default=0.1)

    submit = commands.add_parser("submit", help="submit one JSON job")
    submit.add_argument(
        "payload", help='JSON object, e.g. {"task":"echo","value":"hello"}'
    )
    submit.add_argument("--queue", default="default")
    submit.add_argument("--idempotency-key")
    submit.add_argument("--max-attempts", type=int, default=3)
    submit.add_argument("--delay-seconds", type=float, default=0)

    chaos = commands.add_parser(
        "chaos", help="run deterministic failure assertions"
    )
    chaos.set_defaults(uses_database=False)

    benchmark = commands.add_parser(
        "benchmark", help="run a reproducible local throughput benchmark"
    )
    benchmark.add_argument("--jobs", type=int, default=2000)
    benchmark.add_argument("--workers", type=int, default=8)
    benchmark.add_argument("--claim-batch", type=int, default=16)
    benchmark.set_defaults(uses_database=False)
    return root


def main(argv: Sequence[str] | None = None) -> None:
    args = parser().parse_args(argv)
    if args.command == "chaos":
        print(json.dumps(run_chaos_scenario(), indent=2, sort_keys=True))
        return
    if args.command == "benchmark":
        result = run_benchmark(
            jobs=args.jobs,
            workers=args.workers,
            claim_batch=args.claim_batch,
        )
        print(json.dumps(result, indent=2, sort_keys=True))
        return

    store = JobStore(Path(args.database))
    if args.command == "api":
        serve(store, args.host, args.port)
    elif args.command == "coordinator":
        Coordinator(
            store, sweep_interval=args.sweep_interval
        ).run_forever()
    elif args.command == "worker":
        Worker(
            store,
            worker_id=args.worker_id,
            queue=args.queue,
            concurrency=args.concurrency,
            lease_seconds=args.lease_seconds,
            poll_interval=args.poll_interval,
        ).run_forever()
    elif args.command == "submit":
        payload = json.loads(args.payload)
        if not isinstance(payload, dict):
            raise SystemExit("payload must be a JSON object")
        submission = store.submit(
            payload,
            queue=args.queue,
            idempotency_key=args.idempotency_key,
            max_attempts=args.max_attempts,
            delay_seconds=args.delay_seconds,
        )
        print(
            json.dumps(
                {
                    "created": submission.created,
                    "job_id": submission.job.id,
                    "status": submission.job.status.value,
                },
                sort_keys=True,
            )
        )


if __name__ == "__main__":
    main()
