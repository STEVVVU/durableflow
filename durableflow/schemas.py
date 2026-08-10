"""Pydantic v2 request/response contracts for the HTTP API.

These models replace hand-rolled ``isinstance`` checks and manual ``int()``
coercion. Constraints that used to live in imperative validation code (payload
must be an object, bounded ``max_attempts``, non-negative delay) are now
declarative and appear in the generated OpenAPI schema.
"""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

from durableflow.models import Job

JobStatusLiteral = Literal["queued", "running", "succeeded", "dead_letter", "cancelled"]


class SubmitJobRequest(BaseModel):
    """A job submission. ``payload`` must be a JSON object, as before."""

    model_config = ConfigDict(extra="forbid")

    payload: dict[str, Any] = Field(description="Task payload; 'task' selects the handler.")
    queue: str = Field(default="default", min_length=1, max_length=255)
    idempotency_key: str | None = Field(
        default=None,
        max_length=255,
        description="Queue-scoped key; a repeat returns the original job.",
    )
    max_attempts: int = Field(default=3, ge=1, le=1000)
    delay_seconds: float = Field(default=0, ge=0)


class JobResponse(BaseModel):
    """Durable job state. Field names match the previous JSON document."""

    model_config = ConfigDict(from_attributes=True)

    id: str
    queue: str
    payload: dict[str, Any]
    status: JobStatusLiteral
    attempts: int
    max_attempts: int
    available_at: float
    lease_owner: str | None
    lease_token: str | None
    lease_expires_at: float | None
    cancel_requested: bool
    result: Any = None
    last_error: str | None
    idempotency_key: str | None
    created_at: float
    updated_at: float
    completed_at: float | None

    @classmethod
    def from_job(cls, job: Job) -> JobResponse:
        return cls.model_validate(job, from_attributes=True)


class SubmitJobResponse(BaseModel):
    created: bool = Field(description="False when an idempotency key matched an existing job.")
    job: JobResponse


class JobListResponse(BaseModel):
    jobs: list[JobResponse]


class JobEvent(BaseModel):
    sequence: int
    event_type: str
    worker_id: str | None
    attempt: int | None
    detail: dict[str, Any] | None
    occurred_at: float


class JobEventsResponse(BaseModel):
    job_id: str
    events: list[JobEvent]


class ErrorResponse(BaseModel):
    error: str


class HealthResponse(BaseModel):
    status: Literal["ok"]


class ReadinessResponse(BaseModel):
    """Readiness distinguishes 'process is up' from 'dependencies are usable'."""

    status: Literal["ready", "unready"]
    database: Literal["ok", "unreachable"]
    backend: str
