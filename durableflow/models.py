from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from typing import Any


class JobStatus(StrEnum):
    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    DEAD_LETTER = "dead_letter"
    CANCELLED = "cancelled"


TERMINAL_STATUSES = {
    JobStatus.SUCCEEDED,
    JobStatus.DEAD_LETTER,
    JobStatus.CANCELLED,
}


@dataclass(frozen=True)
class Job:
    id: str
    queue: str
    payload: Mapping[str, Any]
    status: JobStatus
    attempts: int
    max_attempts: int
    available_at: float
    lease_owner: str | None
    lease_token: str | None
    lease_expires_at: float | None
    cancel_requested: bool
    result: Any
    last_error: str | None
    idempotency_key: str | None
    created_at: float
    updated_at: float
    completed_at: float | None

    @classmethod
    def from_row(cls, row: Mapping[str, Any]) -> Job:
        result_raw = row["result"]
        return cls(
            id=row["id"],
            queue=row["queue"],
            payload=json.loads(row["payload"]),
            status=JobStatus(row["status"]),
            attempts=row["attempts"],
            max_attempts=row["max_attempts"],
            available_at=row["available_at"],
            lease_owner=row["lease_owner"],
            lease_token=row["lease_token"],
            lease_expires_at=row["lease_expires_at"],
            cancel_requested=bool(row["cancel_requested"]),
            result=json.loads(result_raw) if result_raw is not None else None,
            last_error=row["last_error"],
            idempotency_key=row["idempotency_key"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
            completed_at=row["completed_at"],
        )


@dataclass(frozen=True)
class Submission:
    job: Job
    created: bool
