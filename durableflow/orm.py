"""SQLAlchemy 2.x declarative schema.

The column set is a faithful port of the original hand-written DDL. Timestamps
stay as epoch seconds in ``Double`` (8-byte) columns because the injectable clock
abstraction -- and therefore the deterministic chaos harness -- is defined in
terms of ``float`` seconds. The width matters: lease expiry is a strict ``>``
comparison against one of these values, so a 4-byte real would round an epoch
timestamp to roughly minute resolution and silently break fencing.
JSON-bearing columns stay as ``TEXT`` holding
canonical (sorted-key, separator-normalised) JSON so that the same bytes are
persisted on SQLite and PostgreSQL; that keeps failure traces byte-comparable
across backends.
"""

from __future__ import annotations

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    Double,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

JOB_STATUS_VALUES = ("queued", "running", "succeeded", "dead_letter", "cancelled")


class Base(DeclarativeBase):
    """Declarative base owning the shared ``MetadataData`` used by alembic."""


class JobRecord(Base):
    """The durable job state machine, retry policy, and lease fence."""

    __tablename__ = "jobs"

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    queue: Mapped[str] = mapped_column(String(255), nullable=False)
    payload: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False)
    idempotency_key: Mapped[str | None] = mapped_column(String(255), nullable=True)
    attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    max_attempts: Mapped[int] = mapped_column(Integer, nullable=False)
    available_at: Mapped[float] = mapped_column(Double, nullable=False)
    lease_owner: Mapped[str | None] = mapped_column(String(255), nullable=True)
    lease_token: Mapped[str | None] = mapped_column(String(36), nullable=True)
    lease_expires_at: Mapped[float | None] = mapped_column(Double, nullable=True)
    heartbeat_at: Mapped[float | None] = mapped_column(Double, nullable=True)
    cancel_requested: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default="0"
    )
    result: Mapped[str | None] = mapped_column(Text, nullable=True)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[float] = mapped_column(Double, nullable=False)
    updated_at: Mapped[float] = mapped_column(Double, nullable=False)
    completed_at: Mapped[float | None] = mapped_column(Double, nullable=True)

    __table_args__ = (
        CheckConstraint(
            "status IN ('queued', 'running', 'succeeded', 'dead_letter', 'cancelled')",
            name="jobs_status_check",
        ),
        # Queue-scoped idempotency. A partial index lets unlimited rows omit the
        # key while still rejecting a duplicate key inside one queue.
        Index(
            "jobs_idempotency",
            "queue",
            "idempotency_key",
            unique=True,
            sqlite_where=idempotency_key.isnot(None),
            postgresql_where=idempotency_key.isnot(None),
        ),
        Index("jobs_claimable", "queue", "status", "available_at", "created_at"),
        Index("jobs_expired", "status", "lease_expires_at"),
    )


class JobEventRecord(Base):
    """Append-only transition history used for debugging and counters."""

    __tablename__ = "job_events"

    # BIGINT is not a rowid alias on SQLite, so a BigInteger primary key would
    # not auto-assign there. The INTEGER variant keeps SQLite's AUTOINCREMENT
    # behaviour while PostgreSQL still gets a 64-bit identity column.
    sequence: Mapped[int] = mapped_column(
        BigInteger().with_variant(Integer, "sqlite"),
        primary_key=True,
        autoincrement=True,
    )
    job_id: Mapped[str] = mapped_column(String(36), ForeignKey("jobs.id"), nullable=False)
    event_type: Mapped[str] = mapped_column(String(64), nullable=False)
    worker_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    attempt: Mapped[int | None] = mapped_column(Integer, nullable=True)
    detail: Mapped[str | None] = mapped_column(Text, nullable=True)
    occurred_at: Mapped[float] = mapped_column(Double, nullable=False)

    __table_args__ = (
        Index("job_events_job", "job_id", "sequence"),
        # Matches the original DDL's AUTOINCREMENT: sequence numbers are never
        # reused, so event ordering is stable for the whole database lifetime.
        {"sqlite_autoincrement": True},
    )


class WorkerRecord(Base):
    """Ephemeral worker liveness snapshot."""

    __tablename__ = "workers"

    worker_id: Mapped[str] = mapped_column(String(255), primary_key=True)
    queue: Mapped[str] = mapped_column(String(255), nullable=False)
    capacity: Mapped[int] = mapped_column(Integer, nullable=False)
    in_flight: Mapped[int] = mapped_column(Integer, nullable=False)
    last_seen_at: Mapped[float] = mapped_column(Double, nullable=False)


class EffectRecord(Base):
    """Job-scoped unique key demonstrating idempotent handler side effects."""

    __tablename__ = "effects"

    job_id: Mapped[str] = mapped_column(String(36), ForeignKey("jobs.id"), primary_key=True)
    effect_key: Mapped[str] = mapped_column(String(255), primary_key=True)
    value: Mapped[str] = mapped_column(Text, nullable=False)
    recorded_at: Mapped[float] = mapped_column(Double, nullable=False)
