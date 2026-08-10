from __future__ import annotations

import json
import tempfile
from contextlib import ExitStack
from pathlib import Path
from typing import Any

from durableflow.clock import ManualClock
from durableflow.coordinator import Coordinator
from durableflow.models import JobStatus
from durableflow.store import JobStore


def run_chaos_scenario(*, database_url: str | None = None) -> dict[str, Any]:
    """Run a deterministic crash/retry/fencing scenario with no sleeps.

    The scenario drives a manual clock, so lease expiry and backoff are exact
    rather than timing-dependent. Passing ``database_url`` runs the same
    assertions against a real backend (for example PostgreSQL), which is how the
    ``FOR UPDATE SKIP LOCKED`` claim path is proven to preserve fencing.
    """
    with ExitStack() as stack:
        clock = ManualClock()
        if database_url:
            store = JobStore(database_url, clock=clock, base_backoff=1, max_backoff=8)
            # Assertions below count events and states, so start from an empty queue.
            store.purge()
        else:
            directory = stack.enter_context(
                tempfile.TemporaryDirectory(prefix="durableflow-chaos-")
            )
            store = JobStore(
                Path(directory) / "chaos.db",
                clock=clock,
                base_backoff=1,
                max_backoff=8,
            )
        stack.callback(store.close)
        coordinator = Coordinator(store)

        crash_job = store.submit(
            {"task": "ledger_write", "effect_key": "charge", "value": 42},
            idempotency_key="order-42",
            max_attempts=3,
        ).job
        duplicate = store.submit(
            {"task": "ledger_write", "effect_key": "charge", "value": 42},
            idempotency_key="order-42",
            max_attempts=3,
        )
        assert duplicate.created is False and duplicate.job.id == crash_job.id

        first_lease = store.claim(
            "worker-crashes", limit=1, lease_seconds=5
        )[0]
        effect, effect_created = store.record_effect(
            first_lease.id, "charge", 42
        )
        assert effect == 42 and effect_created

        clock.advance(5)
        assert coordinator.tick() == 1
        clock.advance(store.backoff_for(1))
        second_lease = store.claim(
            "worker-recovers", limit=1, lease_seconds=5
        )[0]
        effect, effect_created_again = store.record_effect(
            second_lease.id, "charge", 42
        )
        assert effect == 42 and not effect_created_again
        assert not store.complete(
            first_lease.id,
            "worker-crashes",
            first_lease.lease_token or "",
            {"stale": True},
        )
        assert store.complete(
            second_lease.id,
            "worker-recovers",
            second_lease.lease_token or "",
            {"effect_created": effect_created_again},
        )

        poison = store.submit(
            {"task": "flaky", "failures_before_success": 99},
            max_attempts=2,
        ).job
        poison_first = store.claim("worker-a", limit=1, lease_seconds=5)[0]
        assert (
            store.fail(
                poison.id,
                "worker-a",
                poison_first.lease_token or "",
                "injected failure 1",
            )
            == JobStatus.QUEUED
        )
        clock.advance(store.backoff_for(1))
        poison_second = store.claim("worker-b", limit=1, lease_seconds=5)[0]
        assert (
            store.fail(
                poison.id,
                "worker-b",
                poison_second.lease_token or "",
                "injected failure 2",
            )
            == JobStatus.DEAD_LETTER
        )

        cancelled = store.submit({"task": "echo", "value": "unused"}).job
        assert store.cancel(cancelled.id)

        final_crash = store.get(crash_job.id)
        final_poison = store.get(poison.id)
        final_cancelled = store.get(cancelled.id)
        assert final_crash and final_crash.status == JobStatus.SUCCEEDED
        assert final_crash.attempts == 2
        assert final_poison and final_poison.status == JobStatus.DEAD_LETTER
        assert final_cancelled and final_cancelled.status == JobStatus.CANCELLED

        result = {
            "scenario": "crash_retry_recovery",
            "assertions": {
                "idempotent_submission": True,
                "expired_lease_recovered": True,
                "stale_completion_fenced": True,
                "side_effect_deduplicated": True,
                "poison_job_dead_lettered": True,
                "queued_job_cancelled": True,
            },
            "jobs": {
                "crash_recovery": {
                    "id": final_crash.id,
                    "status": final_crash.status.value,
                    "attempts": final_crash.attempts,
                    "events": store.events(final_crash.id),
                },
                "poison": {
                    "id": final_poison.id,
                    "status": final_poison.status.value,
                    "attempts": final_poison.attempts,
                    "events": store.events(final_poison.id),
                },
                "cancelled": {
                    "id": final_cancelled.id,
                    "status": final_cancelled.status.value,
                    "events": store.events(final_cancelled.id),
                },
            },
            "metrics": store.metrics(),
            "backend": store.engine.dialect.name,
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
        prog="durableflow.chaos",
        description="Deterministic crash/retry/recovery assertions",
    )
    argument_parser.add_argument(
        "--database",
        default=None,
        help="Run the scenario against this URL instead of a temporary SQLite database.",
    )
    arguments = argument_parser.parse_args()
    # The JSON trace on stdout is the artifact; keep expected warnings out of it.
    observability.configure_logging(level="ERROR", json_logs=True)
    print(
        json.dumps(
            run_chaos_scenario(database_url=arguments.database),
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
