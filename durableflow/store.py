from __future__ import annotations

import json
import uuid
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from sqlalchemy import Engine, delete, func, insert, select, update
from sqlalchemy.engine import Connection

from durableflow import observability
from durableflow.clock import SystemClock
from durableflow.engine import create_db_engine, create_schema, is_postgres, normalize_url
from durableflow.models import Job, JobStatus, Submission
from durableflow.orm import EffectRecord, JobEventRecord, JobRecord, WorkerRecord

LOGGER = observability.get_logger("store")


def _json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


class JobStore:
    """Transactional persistence and lease-fencing boundary."""

    def __init__(
        self,
        database: str | Path | None = None,
        *,
        clock: Any | None = None,
        base_backoff: float = 1.0,
        max_backoff: float = 60.0,
        engine: Engine | None = None,
        create_tables: bool = True,
    ) -> None:
        if engine is None and database is None:
            raise ValueError("either database or engine must be provided")
        self.clock = clock or SystemClock()
        self.base_backoff = base_backoff
        self.max_backoff = max_backoff
        if engine is not None:
            self._engine = engine
            self._owns_engine = False
            self.database = str(engine.url)
        else:
            self.database = normalize_url(database)  # type: ignore[arg-type]
            self._engine = create_db_engine(self.database)
            self._owns_engine = True
        # PostgreSQL claims with row-level locks and SKIP LOCKED; SQLite claims
        # under the database-wide write lock taken by BEGIN IMMEDIATE.
        self._skip_locked = is_postgres(str(self._engine.url))
        if create_tables:
            create_schema(self._engine)

    @property
    def engine(self) -> Engine:
        return self._engine

    @property
    def uses_skip_locked(self) -> bool:
        """True when claims use PostgreSQL ``FOR UPDATE SKIP LOCKED``."""
        return self._skip_locked

    def close(self) -> None:
        if self._owns_engine and self._engine is not None:
            self._engine.dispose()

    def __enter__(self) -> JobStore:
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()

    @contextmanager
    def _transaction(self) -> Iterator[Connection]:
        """Run one write transaction.

        On SQLite the engine's ``begin`` hook emits ``BEGIN IMMEDIATE``, so the
        write lock is held for the whole block and a claim's read cannot
        interleave with another claimant's write. On PostgreSQL isolation comes
        from row locks taken inside the claim query itself.
        """
        with self._engine.begin() as connection:
            yield connection

    @contextmanager
    def _reader(self) -> Iterator[Connection]:
        with self._engine.connect() as connection:
            yield connection

    def _event(
        self,
        connection: Connection,
        job_id: str,
        event_type: str,
        *,
        worker_id: str | None = None,
        attempt: int | None = None,
        detail: Any = None,
        now: float,
    ) -> None:
        connection.execute(
            insert(JobEventRecord).values(
                job_id=job_id,
                event_type=event_type,
                worker_id=worker_id,
                attempt=attempt,
                detail=_json(detail) if detail is not None else None,
                occurred_at=now,
            )
        )
        observability.record_event_metric(event_type)

    def submit(
        self,
        payload: Mapping[str, Any],
        *,
        queue: str = "default",
        idempotency_key: str | None = None,
        max_attempts: int = 3,
        delay_seconds: float = 0,
    ) -> Submission:
        if not queue:
            raise ValueError("queue must not be empty")
        if max_attempts < 1:
            raise ValueError("max_attempts must be at least 1")
        if delay_seconds < 0:
            raise ValueError("delay_seconds must not be negative")
        now = self.clock.now()
        with self._transaction() as connection:
            if idempotency_key is not None:
                existing = connection.execute(
                    select(JobRecord).where(
                        JobRecord.queue == queue,
                        JobRecord.idempotency_key == idempotency_key,
                    )
                ).mappings().first()
                if existing:
                    observability.JOBS_DUPLICATE_SUBMISSIONS.labels(queue=queue).inc()
                    LOGGER.info(
                        "duplicate submission short-circuited",
                        job_id=existing["id"],
                        queue=queue,
                        idempotency_key=idempotency_key,
                    )
                    return Submission(Job.from_row(existing), created=False)

            job_id = str(uuid.uuid4())
            connection.execute(
                insert(JobRecord).values(
                    id=job_id,
                    queue=queue,
                    payload=_json(payload),
                    status=JobStatus.QUEUED.value,
                    idempotency_key=idempotency_key,
                    attempts=0,
                    max_attempts=max_attempts,
                    available_at=now + delay_seconds,
                    cancel_requested=0,
                    created_at=now,
                    updated_at=now,
                )
            )
            self._event(
                connection,
                job_id,
                "submitted",
                detail={"queue": queue, "delay_seconds": delay_seconds},
                now=now,
            )
            row = connection.execute(
                select(JobRecord).where(JobRecord.id == job_id)
            ).mappings().one()
            observability.JOBS_SUBMITTED.labels(queue=queue).inc()
            LOGGER.info(
                "job submitted",
                job_id=job_id,
                queue=queue,
                max_attempts=max_attempts,
                delay_seconds=delay_seconds,
            )
            return Submission(Job.from_row(row), created=True)

    def get(self, job_id: str) -> Job | None:
        with self._reader() as connection:
            row = connection.execute(
                select(JobRecord).where(JobRecord.id == job_id)
            ).mappings().first()
        return Job.from_row(row) if row else None

    def list_jobs(
        self,
        *,
        status: JobStatus | str | None = None,
        limit: int = 100,
    ) -> list[Job]:
        limit = max(1, min(limit, 1000))
        query = select(JobRecord)
        if status is not None:
            query = query.where(JobRecord.status == str(status))
        query = query.order_by(JobRecord.created_at.desc()).limit(limit)
        with self._reader() as connection:
            rows = connection.execute(query).mappings().all()
        return [Job.from_row(row) for row in rows]

    def claim(
        self,
        worker_id: str,
        *,
        queue: str = "default",
        limit: int = 1,
        lease_seconds: float = 30,
    ) -> list[Job]:
        if limit < 1:
            return []
        if lease_seconds <= 0:
            raise ValueError("lease_seconds must be positive")
        now = self.clock.now()
        claimed_ids: list[str] = []
        with observability.CLAIM_LATENCY.time(), self._transaction() as connection:
            selection = (
                select(JobRecord.id)
                .where(
                    JobRecord.queue == queue,
                    JobRecord.status == JobStatus.QUEUED.value,
                    JobRecord.cancel_requested == 0,
                    JobRecord.available_at <= now,
                )
                .order_by(JobRecord.available_at, JobRecord.created_at, JobRecord.id)
                .limit(limit)
            )
            if self._skip_locked:
                # Lock the candidate rows and skip any a concurrent claimant
                # already holds, so claimants never queue behind each other.
                selection = selection.with_for_update(skip_locked=True)
            rows = connection.execute(selection).mappings().all()
            for row in rows:
                token = str(uuid.uuid4())
                changed = connection.execute(
                    update(JobRecord)
                    .where(
                        JobRecord.id == row["id"],
                        JobRecord.status == JobStatus.QUEUED.value,
                    )
                    .values(
                        status=JobStatus.RUNNING.value,
                        attempts=JobRecord.attempts + 1,
                        lease_owner=worker_id,
                        lease_token=token,
                        lease_expires_at=now + lease_seconds,
                        heartbeat_at=now,
                        updated_at=now,
                    )
                ).rowcount
                if changed:
                    claimed_ids.append(row["id"])
                    attempt = connection.execute(
                        select(JobRecord.attempts).where(JobRecord.id == row["id"])
                    ).scalar_one()
                    self._event(
                        connection,
                        row["id"],
                        "claimed",
                        worker_id=worker_id,
                        attempt=attempt,
                        detail={"lease_seconds": lease_seconds},
                        now=now,
                    )
            if not claimed_ids:
                return []
            claimed = connection.execute(
                select(JobRecord).where(JobRecord.id.in_(claimed_ids))
            ).mappings().all()
        observability.JOBS_CLAIMED.labels(queue=queue).inc(len(claimed_ids))
        LOGGER.info(
            "jobs claimed",
            worker_id=worker_id,
            queue=queue,
            claimed=len(claimed_ids),
            lease_seconds=lease_seconds,
        )
        by_id = {row["id"]: Job.from_row(row) for row in claimed}
        return [by_id[job_id] for job_id in claimed_ids]

    def heartbeat(
        self,
        job_id: str,
        worker_id: str,
        lease_token: str,
        *,
        lease_seconds: float,
    ) -> bool:
        now = self.clock.now()
        with self._transaction() as connection:
            changed = connection.execute(
                update(JobRecord)
                .where(
                    JobRecord.id == job_id,
                    JobRecord.status == JobStatus.RUNNING.value,
                    JobRecord.lease_owner == worker_id,
                    JobRecord.lease_token == lease_token,
                    JobRecord.cancel_requested == 0,
                    JobRecord.lease_expires_at > now,
                )
                .values(
                    lease_expires_at=now + lease_seconds,
                    heartbeat_at=now,
                    updated_at=now,
                )
            ).rowcount
        if changed != 1:
            observability.STALE_FENCE_REJECTIONS.labels(operation="heartbeat").inc()
            LOGGER.warning(
                "heartbeat rejected by lease fence", job_id=job_id, worker_id=worker_id
            )
        return changed == 1

    def complete(
        self,
        job_id: str,
        worker_id: str,
        lease_token: str,
        result: Any,
    ) -> bool:
        now = self.clock.now()
        with self._transaction() as connection:
            row = connection.execute(
                select(JobRecord.attempts, JobRecord.cancel_requested).where(
                    JobRecord.id == job_id,
                    JobRecord.status == JobStatus.RUNNING.value,
                    JobRecord.lease_owner == worker_id,
                    JobRecord.lease_token == lease_token,
                    JobRecord.lease_expires_at > now,
                )
            ).mappings().first()
            if not row:
                observability.STALE_FENCE_REJECTIONS.labels(operation="complete").inc()
                LOGGER.warning(
                    "completion rejected by lease fence",
                    job_id=job_id,
                    worker_id=worker_id,
                )
                return False
            if row["cancel_requested"]:
                self._mark_cancelled(connection, job_id, worker_id, row["attempts"], now)
                return False
            connection.execute(
                update(JobRecord)
                .where(JobRecord.id == job_id)
                .values(
                    status=JobStatus.SUCCEEDED.value,
                    result=_json(result),
                    completed_at=now,
                    updated_at=now,
                    lease_owner=None,
                    lease_token=None,
                    lease_expires_at=None,
                )
            )
            self._event(
                connection,
                job_id,
                "succeeded",
                worker_id=worker_id,
                attempt=row["attempts"],
                now=now,
            )
        LOGGER.info(
            "job succeeded", job_id=job_id, worker_id=worker_id, attempt=row["attempts"]
        )
        return True

    def fail(
        self,
        job_id: str,
        worker_id: str,
        lease_token: str,
        error: str,
    ) -> JobStatus | None:
        now = self.clock.now()
        with self._transaction() as connection:
            row = connection.execute(
                select(
                    JobRecord.attempts,
                    JobRecord.max_attempts,
                    JobRecord.cancel_requested,
                ).where(
                    JobRecord.id == job_id,
                    JobRecord.status == JobStatus.RUNNING.value,
                    JobRecord.lease_owner == worker_id,
                    JobRecord.lease_token == lease_token,
                    JobRecord.lease_expires_at > now,
                )
            ).mappings().first()
            if not row:
                observability.STALE_FENCE_REJECTIONS.labels(operation="fail").inc()
                LOGGER.warning(
                    "failure report rejected by lease fence",
                    job_id=job_id,
                    worker_id=worker_id,
                )
                return None
            if row["cancel_requested"]:
                self._mark_cancelled(connection, job_id, worker_id, row["attempts"], now)
                return JobStatus.CANCELLED
            if row["attempts"] >= row["max_attempts"]:
                status = JobStatus.DEAD_LETTER
                available_at = now
                event_type = "dead_lettered"
            else:
                status = JobStatus.QUEUED
                available_at = now + self.backoff_for(row["attempts"])
                event_type = "retry_scheduled"
            connection.execute(
                update(JobRecord)
                .where(JobRecord.id == job_id)
                .values(
                    status=status.value,
                    available_at=available_at,
                    last_error=error[:4000],
                    updated_at=now,
                    completed_at=now if status == JobStatus.DEAD_LETTER else None,
                    lease_owner=None,
                    lease_token=None,
                    lease_expires_at=None,
                )
            )
            self._event(
                connection,
                job_id,
                event_type,
                worker_id=worker_id,
                attempt=row["attempts"],
                detail={"error": error[:500], "available_at": available_at},
                now=now,
            )
        LOGGER.warning(
            "job attempt failed",
            job_id=job_id,
            worker_id=worker_id,
            attempt=row["attempts"],
            outcome=status.value,
            available_at=available_at,
            error=error[:500],
        )
        return status

    def recover_expired(self, *, limit: int = 1000) -> int:
        now = self.clock.now()
        recovered = 0
        with self._transaction() as connection:
            selection = (
                select(
                    JobRecord.id,
                    JobRecord.attempts,
                    JobRecord.max_attempts,
                    JobRecord.lease_owner,
                    JobRecord.cancel_requested,
                )
                .where(
                    JobRecord.status == JobStatus.RUNNING.value,
                    JobRecord.lease_expires_at <= now,
                )
                .order_by(JobRecord.lease_expires_at)
                .limit(limit)
            )
            if self._skip_locked:
                # Several coordinators may sweep concurrently; skipping locked
                # rows keeps each expired job with exactly one sweeper.
                selection = selection.with_for_update(skip_locked=True)
            rows = connection.execute(selection).mappings().all()
            for row in rows:
                if row["cancel_requested"]:
                    self._mark_cancelled(
                        connection,
                        row["id"],
                        row["lease_owner"],
                        row["attempts"],
                        now,
                    )
                    recovered += 1
                    continue
                if row["attempts"] >= row["max_attempts"]:
                    status = JobStatus.DEAD_LETTER
                    available_at = now
                    completed_at = now
                    event_type = "lease_expired_dead_lettered"
                else:
                    status = JobStatus.QUEUED
                    available_at = now + self.backoff_for(row["attempts"])
                    completed_at = None
                    event_type = "lease_expired_requeued"
                connection.execute(
                    update(JobRecord)
                    .where(
                        JobRecord.id == row["id"],
                        JobRecord.status == JobStatus.RUNNING.value,
                    )
                    .values(
                        status=status.value,
                        available_at=available_at,
                        updated_at=now,
                        completed_at=completed_at,
                        last_error="worker lease expired",
                        lease_owner=None,
                        lease_token=None,
                        lease_expires_at=None,
                    )
                )
                self._event(
                    connection,
                    row["id"],
                    event_type,
                    worker_id=row["lease_owner"],
                    attempt=row["attempts"],
                    detail={"available_at": available_at},
                    now=now,
                )
                LOGGER.warning(
                    "expired lease recovered",
                    job_id=row["id"],
                    worker_id=row["lease_owner"],
                    attempt=row["attempts"],
                    outcome=status.value,
                    available_at=available_at,
                )
                recovered += 1
        return recovered

    def cancel(self, job_id: str) -> bool:
        now = self.clock.now()
        with self._transaction() as connection:
            row = connection.execute(
                select(JobRecord.status, JobRecord.attempts).where(JobRecord.id == job_id)
            ).mappings().first()
            if not row or row["status"] in {
                "succeeded",
                "dead_letter",
                "cancelled",
            }:
                return False
            if row["status"] == "queued":
                self._mark_cancelled(connection, job_id, None, row["attempts"], now)
            else:
                connection.execute(
                    update(JobRecord)
                    .where(
                        JobRecord.id == job_id,
                        JobRecord.status == JobStatus.RUNNING.value,
                    )
                    .values(cancel_requested=1, updated_at=now)
                )
                self._event(
                    connection,
                    job_id,
                    "cancellation_requested",
                    attempt=row["attempts"],
                    now=now,
                )
        LOGGER.info("cancellation accepted", job_id=job_id, previous_status=row["status"])
        return True

    def acknowledge_cancellation(self, job_id: str, worker_id: str, lease_token: str) -> bool:
        now = self.clock.now()
        with self._transaction() as connection:
            row = connection.execute(
                select(JobRecord.attempts).where(
                    JobRecord.id == job_id,
                    JobRecord.status == JobStatus.RUNNING.value,
                    JobRecord.cancel_requested == 1,
                    JobRecord.lease_owner == worker_id,
                    JobRecord.lease_token == lease_token,
                    JobRecord.lease_expires_at > now,
                )
            ).mappings().first()
            if not row:
                observability.STALE_FENCE_REJECTIONS.labels(
                    operation="acknowledge_cancellation"
                ).inc()
                return False
            self._mark_cancelled(connection, job_id, worker_id, row["attempts"], now)
        return True

    def _mark_cancelled(
        self,
        connection: Connection,
        job_id: str,
        worker_id: str | None,
        attempt: int,
        now: float,
    ) -> None:
        connection.execute(
            update(JobRecord)
            .where(JobRecord.id == job_id)
            .values(
                status=JobStatus.CANCELLED.value,
                cancel_requested=1,
                completed_at=now,
                updated_at=now,
                lease_owner=None,
                lease_token=None,
                lease_expires_at=None,
            )
        )
        self._event(
            connection,
            job_id,
            "cancelled",
            worker_id=worker_id,
            attempt=attempt,
            now=now,
        )

    def redrive(self, job_id: str) -> bool:
        now = self.clock.now()
        with self._transaction() as connection:
            changed = connection.execute(
                update(JobRecord)
                .where(
                    JobRecord.id == job_id,
                    JobRecord.status == JobStatus.DEAD_LETTER.value,
                )
                .values(
                    status=JobStatus.QUEUED.value,
                    attempts=0,
                    available_at=now,
                    updated_at=now,
                    completed_at=None,
                    last_error=None,
                    cancel_requested=0,
                )
            ).rowcount
            if changed:
                self._event(
                    connection, job_id, "redriven", detail={"attempts_reset": True}, now=now
                )
                LOGGER.info("dead-letter job redriven", job_id=job_id)
        return changed == 1

    def cancellation_requested(self, job_id: str, worker_id: str, lease_token: str) -> bool:
        with self._reader() as connection:
            row = connection.execute(
                select(JobRecord.cancel_requested, JobRecord.lease_expires_at).where(
                    JobRecord.id == job_id,
                    JobRecord.status == JobStatus.RUNNING.value,
                    JobRecord.lease_owner == worker_id,
                    JobRecord.lease_token == lease_token,
                )
            ).mappings().first()
        return (
            row is None
            or bool(row["cancel_requested"])
            or row["lease_expires_at"] <= self.clock.now()
        )

    def register_worker(self, worker_id: str, queue: str, capacity: int, in_flight: int) -> None:
        now = self.clock.now()
        with self._transaction() as connection:
            # Portable upsert: the dialect-specific ON CONFLICT syntaxes differ,
            # and this row is an ephemeral liveness snapshot rather than part of
            # any correctness-critical transition.
            updated = connection.execute(
                update(WorkerRecord)
                .where(WorkerRecord.worker_id == worker_id)
                .values(
                    queue=queue,
                    capacity=capacity,
                    in_flight=in_flight,
                    last_seen_at=now,
                )
            ).rowcount
            if not updated:
                connection.execute(
                    insert(WorkerRecord).values(
                        worker_id=worker_id,
                        queue=queue,
                        capacity=capacity,
                        in_flight=in_flight,
                        last_seen_at=now,
                    )
                )

    def record_effect(self, job_id: str, effect_key: str, value: Any) -> tuple[Any, bool]:
        """Persist an application-side idempotent effect in the same database."""
        now = self.clock.now()
        with self._transaction() as connection:
            existing = connection.execute(
                select(EffectRecord.value).where(
                    EffectRecord.job_id == job_id,
                    EffectRecord.effect_key == effect_key,
                )
            ).mappings().first()
            if existing:
                return json.loads(existing["value"]), False
            connection.execute(
                insert(EffectRecord).values(
                    job_id=job_id,
                    effect_key=effect_key,
                    value=_json(value),
                    recorded_at=now,
                )
            )
        return value, True

    def events(self, job_id: str) -> list[dict[str, Any]]:
        with self._reader() as connection:
            rows = connection.execute(
                select(
                    JobEventRecord.sequence,
                    JobEventRecord.event_type,
                    JobEventRecord.worker_id,
                    JobEventRecord.attempt,
                    JobEventRecord.detail,
                    JobEventRecord.occurred_at,
                )
                .where(JobEventRecord.job_id == job_id)
                .order_by(JobEventRecord.sequence)
            ).mappings().all()
        return [
            {
                "sequence": row["sequence"],
                "event_type": row["event_type"],
                "worker_id": row["worker_id"],
                "attempt": row["attempt"],
                "detail": json.loads(row["detail"]) if row["detail"] else None,
                "occurred_at": row["occurred_at"],
            }
            for row in rows
        ]

    def purge(self) -> None:
        """Delete all rows. Used by the benchmark and Postgres test fixtures."""
        with self._transaction() as connection:
            connection.execute(delete(JobEventRecord))
            connection.execute(delete(EffectRecord))
            connection.execute(delete(WorkerRecord))
            connection.execute(delete(JobRecord))

    def metrics(self) -> dict[str, Any]:
        now = self.clock.now()
        with self._reader() as connection:
            statuses = {
                row["status"]: row["count"]
                for row in connection.execute(
                    select(JobRecord.status, func.count().label("count")).group_by(
                        JobRecord.status
                    )
                ).mappings()
            }
            event_totals = {
                row["event_type"]: row["count"]
                for row in connection.execute(
                    select(
                        JobEventRecord.event_type, func.count().label("count")
                    ).group_by(JobEventRecord.event_type)
                ).mappings()
            }
            oldest = connection.execute(
                select(func.min(JobRecord.created_at)).where(
                    JobRecord.status == JobStatus.QUEUED.value
                )
            ).scalar()
            active_workers = connection.execute(
                select(func.count())
                .select_from(WorkerRecord)
                .where(WorkerRecord.last_seen_at >= now - 30)
            ).scalar_one()
        return {
            "jobs": {status.value: statuses.get(status.value, 0) for status in JobStatus},
            "events": event_totals,
            "active_workers": active_workers,
            "oldest_queued_age_seconds": max(0, now - oldest) if oldest else 0,
        }

    def prometheus_metrics(self) -> str:
        """Render the Prometheus text exposition for this store's registry.

        Gauges are refreshed from the database on every scrape so a restarted
        process reports true queue depth instead of its own process-local view.
        """
        from prometheus_client import generate_latest

        observability.refresh_gauges(self.metrics())
        return generate_latest(observability.REGISTRY).decode()

    def backoff_for(self, attempts: int) -> float:
        return min(self.max_backoff, self.base_backoff * (2 ** max(0, attempts - 1)))
