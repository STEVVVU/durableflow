from __future__ import annotations

import argparse
import json
from collections.abc import Sequence

from durableflow import observability
from durableflow.api import serve
from durableflow.benchmark import run_benchmark
from durableflow.chaos import run_chaos_scenario
from durableflow.config import load_settings
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
        default=None,
        help=(
            "SQLAlchemy URL or SQLite path. Defaults to DURABLEFLOW_DATABASE_URL, "
            "then a local SQLite file."
        ),
    )
    root.add_argument(
        "--log-format",
        choices=("json", "console"),
        default=None,
        help="Log rendering; defaults to JSON.",
    )
    root.add_argument("--log-level", default=None, help="Log level, e.g. INFO or DEBUG.")
    commands = root.add_subparsers(dest="command", required=True)

    api = commands.add_parser("api", help="run the HTTP API")
    api.add_argument("--host", default=None)
    api.add_argument("--port", type=int, default=None)

    coordinator = commands.add_parser("coordinator", help="run lease recovery")
    coordinator.add_argument("--sweep-interval", type=float, default=None)

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

    migrate = commands.add_parser(
        "migrate", help="apply alembic migrations to the configured database"
    )
    migrate.add_argument("--revision", default="head")
    migrate.set_defaults(uses_database=False)

    chaos = commands.add_parser("chaos", help="run deterministic failure assertions")
    chaos.set_defaults(uses_database=False)

    benchmark = commands.add_parser(
        "benchmark", help="run a reproducible local throughput benchmark"
    )
    benchmark.add_argument("--jobs", type=int, default=2000)
    benchmark.add_argument("--workers", type=int, default=8)
    benchmark.add_argument("--claim-batch", type=int, default=16)
    benchmark.add_argument(
        "--benchmark-database",
        default=None,
        help="Benchmark against this URL instead of a temporary SQLite file.",
    )
    benchmark.set_defaults(uses_database=False)
    return root


def main(argv: Sequence[str] | None = None) -> None:
    args = parser().parse_args(argv)

    overrides: dict[str, object] = {}
    if args.database is not None:
        overrides["database_url"] = args.database
    if args.log_level is not None:
        overrides["log_level"] = args.log_level
    if args.log_format is not None:
        overrides["log_json"] = args.log_format == "json"
    settings = load_settings(**overrides)
    observability.configure_logging(level=settings.log_level, json_logs=settings.log_json)

    if args.command == "chaos":
        print(json.dumps(run_chaos_scenario(), indent=2, sort_keys=True))
        return
    if args.command == "benchmark":
        result = run_benchmark(
            jobs=args.jobs,
            workers=args.workers,
            claim_batch=args.claim_batch,
            database_url=args.benchmark_database,
        )
        print(json.dumps(result, indent=2, sort_keys=True))
        return
    if args.command == "migrate":
        _migrate(settings.database_url, args.revision)
        return

    store = JobStore(
        settings.database_url,
        base_backoff=settings.base_backoff,
        max_backoff=settings.max_backoff,
    )
    if args.command == "api":
        serve(
            store,
            args.host or settings.host,
            args.port or settings.port,
            settings=settings,
        )
    elif args.command == "coordinator":
        Coordinator(
            store,
            sweep_interval=(
                args.sweep_interval
                if args.sweep_interval is not None
                else settings.sweep_interval
            ),
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


def _migrate(database_url: str, revision: str) -> None:
    """Run ``alembic upgrade`` in-process against the configured database."""
    from pathlib import Path

    from alembic.config import Config

    from alembic import command

    root = Path(__file__).resolve().parent.parent
    config = Config(str(root / "alembic.ini"))
    config.set_main_option("script_location", str(root / "alembic"))
    config.set_main_option("sqlalchemy.url", database_url)
    command.upgrade(config, revision)


if __name__ == "__main__":
    main()
