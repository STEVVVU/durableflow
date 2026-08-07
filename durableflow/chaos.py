from __future__ import annotations

import json
import tempfile
from pathlib import Path
from typing import Any

from durableflow.clock import ManualClock
from durableflow.coordinator import Coordinator
from durableflow.models import JobStatus
from durableflow.store import JobStore


def run_chaos_scenario() -> dict[str, Any]:
    """Run a deterministic crash/retry/fencing scenario with no sleeps."""
    with tempfile.TemporaryDirectory(prefix="durableflow-chaos-") as directory:
        clock = ManualClock()
        store = JobStore(
            Path(directory) / "chaos.db",
            clock=clock,
            base_backoff=1,
            max_backoff=8,
        )
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
        }
        store.close()
        return result


def main() -> None:
    print(json.dumps(run_chaos_scenario(), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
