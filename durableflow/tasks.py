from __future__ import annotations

import hashlib
import time
from dataclasses import dataclass
from typing import Any, Callable, Mapping


class TaskCancelled(Exception):
    pass


@dataclass(frozen=True)
class TaskContext:
    job_id: str
    attempt: int
    cancellation_requested: Callable[[], bool]
    record_effect: Callable[[str, Any], tuple[Any, bool]]

    def check_cancelled(self) -> None:
        if self.cancellation_requested():
            raise TaskCancelled("job cancellation requested")


TaskHandler = Callable[[Mapping[str, Any], TaskContext], Any]


def echo(payload: Mapping[str, Any], context: TaskContext) -> Any:
    context.check_cancelled()
    return payload.get("value")


def sleep_task(payload: Mapping[str, Any], context: TaskContext) -> dict[str, Any]:
    seconds = float(payload.get("seconds", 0))
    if seconds < 0 or seconds > 3600:
        raise ValueError("seconds must be between 0 and 3600")
    deadline = time.monotonic() + seconds
    while True:
        context.check_cancelled()
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        time.sleep(min(0.1, remaining))
    return {"slept_seconds": seconds}


def flaky(payload: Mapping[str, Any], context: TaskContext) -> dict[str, Any]:
    failures = int(payload.get("failures_before_success", 1))
    if context.attempt <= failures:
        raise RuntimeError(
            f"deterministic failure {context.attempt}/{failures}"
        )
    return {"attempt": context.attempt, "recovered": True}


def sha256_task(payload: Mapping[str, Any], context: TaskContext) -> dict[str, Any]:
    context.check_cancelled()
    value = str(payload.get("value", "")).encode()
    rounds = int(payload.get("rounds", 1))
    if rounds < 1 or rounds > 1_000_000:
        raise ValueError("rounds must be between 1 and 1000000")
    digest = value
    for iteration in range(rounds):
        if iteration % 1000 == 0:
            context.check_cancelled()
        digest = hashlib.sha256(digest).digest()
    return {"sha256": digest.hex(), "rounds": rounds}


def ledger_write(payload: Mapping[str, Any], context: TaskContext) -> dict[str, Any]:
    context.check_cancelled()
    value, created = context.record_effect(
        str(payload.get("effect_key", "default")),
        payload.get("value"),
    )
    return {"value": value, "effect_created": created}


DEFAULT_TASKS: dict[str, TaskHandler] = {
    "echo": echo,
    "sleep": sleep_task,
    "flaky": flaky,
    "sha256": sha256_task,
    "ledger_write": ledger_write,
}
