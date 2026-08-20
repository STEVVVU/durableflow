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
        submission_deduplicated = (
            duplicate.created is False and duplicate.job.id == crash_job.id
        )
        assert submission_deduplicated

        first_lease = store.claim(
            "worker-crashes", limit=1, lease_seconds=5
        )[0]
        effect, effect_created = store.record_effect(
            first_lease.id, "charge", 42
        )
        first_effect_recorded = effect == 42 and effect_created
        assert first_effect_recorded

        clock.advance(5)
        recovered = coordinator.tick()
        lease_recovered = recovered == 1
        assert lease_recovered
        clock.advance(store.backoff_for(1))
        second_lease = store.claim(
            "worker-recovers", limit=1, lease_seconds=5
        )[0]
        effect, effect_created_again = store.record_effect(
            second_lease.id, "charge", 42
        )
        effect_replay_deduplicated = effect == 42 and not effect_created_again
        assert effect_replay_deduplicated
        stale_completion_rejected = not store.complete(
            first_lease.id,
            "worker-crashes",
            first_lease.lease_token or "",
            {"stale": True},
        )
        assert stale_completion_rejected
        fresh_owner_completed = store.complete(
            second_lease.id,
            "worker-recovers",
            second_lease.lease_token or "",
            {"effect_created": effect_created_again},
        )
        assert fresh_owner_completed

        poison = store.submit(
            {"task": "flaky", "failures_before_success": 99},
            max_attempts=2,
        ).job
        poison_first = store.claim("worker-a", limit=1, lease_seconds=5)[0]
        first_failure_requeued = (
            store.fail(
                poison.id,
                "worker-a",
                poison_first.lease_token or "",
                "injected failure 1",
            )
            == JobStatus.QUEUED
        )
        assert first_failure_requeued
        clock.advance(store.backoff_for(1))
        poison_second = store.claim("worker-b", limit=1, lease_seconds=5)[0]
        final_failure_dead_lettered = (
            store.fail(
                poison.id,
                "worker-b",
                poison_second.lease_token or "",
                "injected failure 2",
            )
            == JobStatus.DEAD_LETTER
        )
        assert final_failure_dead_lettered

        cancelled = store.submit({"task": "echo", "value": "unused"}).job
        cancel_accepted = store.cancel(cancelled.id)
        assert cancel_accepted

        final_crash = store.get(crash_job.id)
        final_poison = store.get(poison.id)
        final_cancelled = store.get(cancelled.id)
        crash_job_succeeded = bool(final_crash) and final_crash.status == JobStatus.SUCCEEDED
        assert crash_job_succeeded
        crash_job_retried_once = final_crash.attempts == 2
        assert crash_job_retried_once
        poison_job_dead_lettered = (
            bool(final_poison) and final_poison.status == JobStatus.DEAD_LETTER
        )
        assert poison_job_dead_lettered
        cancelled_job_is_cancelled = (
            bool(final_cancelled) and final_cancelled.status == JobStatus.CANCELLED
        )
        assert cancelled_job_is_cancelled

        result = {
            "scenario": "crash_retry_recovery",
            # Each value is the observed outcome of the condition the
            # correspondingly-named bare assert above checks, not a constant.
            # A caller asserting on these therefore fails if the behaviour
            # regresses; the bare asserts remain to fail fast and localize.
            "assertions": {
                "idempotent_submission": submission_deduplicated,
                "expired_lease_recovered": lease_recovered and crash_job_retried_once,
                "stale_completion_fenced": (
                    stale_completion_rejected and fresh_owner_completed and crash_job_succeeded
                ),
                "side_effect_deduplicated": (
                    first_effect_recorded and effect_replay_deduplicated
                ),
                "poison_job_dead_lettered": (
                    first_failure_requeued and final_failure_dead_lettered
                    and poison_job_dead_lettered
                ),
                "queued_job_cancelled": cancel_accepted and cancelled_job_is_cancelled,
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
