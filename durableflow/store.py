from __future__ import annotations

import json
import sqlite3
import uuid
from contextlib import closing, contextmanager
from pathlib import Path
from typing import Any, Iterator, Mapping

from durableflow.clock import SystemClock
from durableflow.models import Job, JobStatus, Submission


SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
    id TEXT PRIMARY KEY,
    queue TEXT NOT NULL,
    payload TEXT NOT NULL,
    status TEXT NOT NULL CHECK (
        status IN ('queued', 'running', 'succeeded', 'dead_letter', 'cancelled')
    ),
    idempotency_key TEXT,
    attempts INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL,
    available_at REAL NOT NULL,
    lease_owner TEXT,
    lease_token TEXT,
    lease_expires_at REAL,
    heartbeat_at REAL,
    cancel_requested INTEGER NOT NULL DEFAULT 0,
    result TEXT,
    last_error TEXT,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL,
    completed_at REAL
);

CREATE UNIQUE INDEX IF NOT EXISTS jobs_idempotency
ON jobs(queue, idempotency_key) WHERE idempotency_key IS NOT NULL;

CREATE INDEX IF NOT EXISTS jobs_claimable
ON jobs(queue, status, available_at, created_at);

CREATE INDEX IF NOT EXISTS jobs_expired
ON jobs(status, lease_expires_at);

CREATE TABLE IF NOT EXISTS job_events (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id TEXT NOT NULL REFERENCES jobs(id),
    event_type TEXT NOT NULL,
    worker_id TEXT,
    attempt INTEGER,
    detail TEXT,
    occurred_at REAL NOT NULL
);

CREATE INDEX IF NOT EXISTS job_events_job
ON job_events(job_id, sequence);

CREATE TABLE IF NOT EXISTS workers (
    worker_id TEXT PRIMARY KEY,
    queue TEXT NOT NULL,
    capacity INTEGER NOT NULL,
    in_flight INTEGER NOT NULL,
    last_seen_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS effects (
    job_id TEXT NOT NULL REFERENCES jobs(id),
    effect_key TEXT NOT NULL,
    value TEXT NOT NULL,
    recorded_at REAL NOT NULL,
    PRIMARY KEY (job_id, effect_key)
);
"""


def _json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


class JobStore:
    """Transactional persistence and lease-fencing boundary."""

    def __init__(
        self,
        database: str | Path,
        *,
        clock: Any | None = None,
        base_backoff: float = 1.0,
        max_backoff: float = 60.0,
    ) -> None:
        self.database = str(database)
        self.clock = clock or SystemClock()
        self.base_backoff = base_backoff
        self.max_backoff = max_backoff
        Path(self.database).parent.mkdir(parents=True, exist_ok=True)
        self._anchor: sqlite3.Connection | None = self._connect()
        self._anchor.execute("PRAGMA journal_mode=WAL")
        self._anchor.executescript(SCHEMA)

    def close(self) -> None:
        if self._anchor is not None:
            self._anchor.close()
            self._anchor = None

    def __enter__(self) -> "JobStore":
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()

    def __del__(self) -> None:
        anchor = getattr(self, "_anchor", None)
        if anchor is not None:
            anchor.close()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(
            self.database,
            timeout=10,
            isolation_level=None,
        )
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA busy_timeout=10000")
        connection.execute("PRAGMA synchronous=NORMAL")
        return connection

    @contextmanager
    def _transaction(self) -> Iterator[sqlite3.Connection]:
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            yield connection
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()

    def _event(
        self,
        connection: sqlite3.Connection,
        job_id: str,
        event_type: str,
        *,
        worker_id: str | None = None,
        attempt: int | None = None,
        detail: Any = None,
        now: float,
    ) -> None:
        connection.execute(
            """
            INSERT INTO job_events
                (job_id, event_type, worker_id, attempt, detail, occurred_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                job_id,
                event_type,
                worker_id,
                attempt,
                _json(detail) if detail is not None else None,
                now,
            ),
        )

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
                    "SELECT * FROM jobs WHERE queue = ? AND idempotency_key = ?",
                    (queue, idempotency_key),
                ).fetchone()
                if existing:
                    return Submission(Job.from_row(existing), created=False)

            job_id = str(uuid.uuid4())
            connection.execute(
                """
                INSERT INTO jobs (
                    id, queue, payload, status, idempotency_key, attempts,
                    max_attempts, available_at, created_at, updated_at
                ) VALUES (?, ?, ?, 'queued', ?, 0, ?, ?, ?, ?)
                """,
                (
                    job_id,
                    queue,
                    _json(payload),
                    idempotency_key,
                    max_attempts,
                    now + delay_seconds,
                    now,
                    now,
                ),
            )
            self._event(
                connection,
                job_id,
                "submitted",
                detail={"queue": queue, "delay_seconds": delay_seconds},
                now=now,
            )
            row = connection.execute(
                "SELECT * FROM jobs WHERE id = ?", (job_id,)
            ).fetchone()
            return Submission(Job.from_row(row), created=True)

    def get(self, job_id: str) -> Job | None:
        with closing(self._connect()) as connection:
            row = connection.execute(
                "SELECT * FROM jobs WHERE id = ?", (job_id,)
            ).fetchone()
        return Job.from_row(row) if row else None

    def list_jobs(
        self,
        *,
        status: JobStatus | str | None = None,
        limit: int = 100,
    ) -> list[Job]:
        limit = max(1, min(limit, 1000))
        query = "SELECT * FROM jobs"
        parameters: list[Any] = []
        if status is not None:
            query += " WHERE status = ?"
            parameters.append(str(status))
        query += " ORDER BY created_at DESC LIMIT ?"
        parameters.append(limit)
        with closing(self._connect()) as connection:
            rows = connection.execute(query, parameters).fetchall()
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
        with self._transaction() as connection:
            rows = connection.execute(
                """
                SELECT id FROM jobs
                WHERE queue = ?
                  AND status = 'queued'
                  AND cancel_requested = 0
                  AND available_at <= ?
                ORDER BY available_at, created_at, id
                LIMIT ?
                """,
                (queue, now, limit),
            ).fetchall()
            for row in rows:
                token = str(uuid.uuid4())
                changed = connection.execute(
                    """
                    UPDATE jobs
                    SET status = 'running',
                        attempts = attempts + 1,
                        lease_owner = ?,
                        lease_token = ?,
                        lease_expires_at = ?,
                        heartbeat_at = ?,
                        updated_at = ?
                    WHERE id = ? AND status = 'queued'
                    """,
                    (
                        worker_id,
                        token,
                        now + lease_seconds,
                        now,
                        now,
                        row["id"],
                    ),
                ).rowcount
                if changed:
                    claimed_ids.append(row["id"])
                    attempt = connection.execute(
                        "SELECT attempts FROM jobs WHERE id = ?", (row["id"],)
                    ).fetchone()["attempts"]
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
            placeholders = ",".join("?" for _ in claimed_ids)
            claimed = connection.execute(
                f"SELECT * FROM jobs WHERE id IN ({placeholders})",
                claimed_ids,
            ).fetchall()
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
                """
                UPDATE jobs
                SET lease_expires_at = ?, heartbeat_at = ?, updated_at = ?
                WHERE id = ?
                  AND status = 'running'
                  AND lease_owner = ?
                  AND lease_token = ?
                  AND cancel_requested = 0
                  AND lease_expires_at > ?
                """,
                (
                    now + lease_seconds,
                    now,
                    now,
                    job_id,
                    worker_id,
                    lease_token,
                    now,
                ),
            ).rowcount
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
                """
                SELECT attempts, cancel_requested FROM jobs
                WHERE id = ? AND status = 'running'
                  AND lease_owner = ? AND lease_token = ?
                  AND lease_expires_at > ?
                """,
                (job_id, worker_id, lease_token, now),
            ).fetchone()
            if not row:
                return False
            if row["cancel_requested"]:
                self._mark_cancelled(
                    connection, job_id, worker_id, row["attempts"], now
                )
                return False
            connection.execute(
                """
                UPDATE jobs
                SET status = 'succeeded', result = ?, completed_at = ?,
                    updated_at = ?, lease_owner = NULL, lease_token = NULL,
                    lease_expires_at = NULL
                WHERE id = ?
                """,
                (_json(result), now, now, job_id),
            )
            self._event(
                connection,
                job_id,
                "succeeded",
                worker_id=worker_id,
                attempt=row["attempts"],
                now=now,
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
                """
                SELECT attempts, max_attempts, cancel_requested FROM jobs
                WHERE id = ? AND status = 'running'
                  AND lease_owner = ? AND lease_token = ?
                  AND lease_expires_at > ?
                """,
                (job_id, worker_id, lease_token, now),
            ).fetchone()
            if not row:
                return None
            if row["cancel_requested"]:
                self._mark_cancelled(
                    connection, job_id, worker_id, row["attempts"], now
                )
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
                """
                UPDATE jobs
                SET status = ?, available_at = ?, last_error = ?,
                    updated_at = ?, completed_at = ?,
                    lease_owner = NULL, lease_token = NULL,
                    lease_expires_at = NULL
                WHERE id = ?
                """,
                (
                    status.value,
                    available_at,
                    error[:4000],
                    now,
                    now if status == JobStatus.DEAD_LETTER else None,
                    job_id,
                ),
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
        return status

    def recover_expired(self, *, limit: int = 1000) -> int:
        now = self.clock.now()
        recovered = 0
        with self._transaction() as connection:
            rows = connection.execute(
                """
                SELECT id, attempts, max_attempts, lease_owner, cancel_requested
                FROM jobs
                WHERE status = 'running' AND lease_expires_at <= ?
                ORDER BY lease_expires_at
                LIMIT ?
                """,
                (now, limit),
            ).fetchall()
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
                    """
                    UPDATE jobs
                    SET status = ?, available_at = ?, updated_at = ?,
                        completed_at = ?, last_error = 'worker lease expired',
                        lease_owner = NULL, lease_token = NULL,
                        lease_expires_at = NULL
                    WHERE id = ? AND status = 'running'
                    """,
                    (
                        status.value,
                        available_at,
                        now,
                        completed_at,
                        row["id"],
                    ),
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
                recovered += 1
        return recovered

    def cancel(self, job_id: str) -> bool:
        now = self.clock.now()
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT status, attempts FROM jobs WHERE id = ?", (job_id,)
            ).fetchone()
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
                    """
                    UPDATE jobs SET cancel_requested = 1, updated_at = ?
                    WHERE id = ? AND status = 'running'
                    """,
                    (now, job_id),
                )
                self._event(
                    connection,
                    job_id,
                    "cancellation_requested",
                    attempt=row["attempts"],
                    now=now,
                )
        return True

    def acknowledge_cancellation(
        self, job_id: str, worker_id: str, lease_token: str
    ) -> bool:
        now = self.clock.now()
        with self._transaction() as connection:
            row = connection.execute(
                """
                SELECT attempts FROM jobs
                WHERE id = ? AND status = 'running'
                  AND cancel_requested = 1
                  AND lease_owner = ? AND lease_token = ?
                  AND lease_expires_at > ?
                """,
                (job_id, worker_id, lease_token, now),
            ).fetchone()
            if not row:
                return False
            self._mark_cancelled(
                connection, job_id, worker_id, row["attempts"], now
            )
        return True

    def _mark_cancelled(
        self,
        connection: sqlite3.Connection,
        job_id: str,
        worker_id: str | None,
        attempt: int,
        now: float,
    ) -> None:
        connection.execute(
            """
            UPDATE jobs
            SET status = 'cancelled', cancel_requested = 1,
                completed_at = ?, updated_at = ?,
                lease_owner = NULL, lease_token = NULL,
                lease_expires_at = NULL
            WHERE id = ?
            """,
            (now, now, job_id),
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
                """
                UPDATE jobs
                SET status = 'queued', attempts = 0, available_at = ?,
                    updated_at = ?, completed_at = NULL, last_error = NULL,
                    cancel_requested = 0
                WHERE id = ? AND status = 'dead_letter'
                """,
                (now, now, job_id),
            ).rowcount
            if changed:
                self._event(
                    connection, job_id, "redriven", detail={"attempts_reset": True}, now=now
                )
        return changed == 1

    def cancellation_requested(
        self, job_id: str, worker_id: str, lease_token: str
    ) -> bool:
        with closing(self._connect()) as connection:
            row = connection.execute(
                """
                SELECT cancel_requested, lease_expires_at FROM jobs
                WHERE id = ? AND status = 'running'
                  AND lease_owner = ? AND lease_token = ?
                """,
                (job_id, worker_id, lease_token),
            ).fetchone()
        return (
            row is None
            or bool(row["cancel_requested"])
            or row["lease_expires_at"] <= self.clock.now()
        )

    def register_worker(
        self, worker_id: str, queue: str, capacity: int, in_flight: int
    ) -> None:
        now = self.clock.now()
        with self._transaction() as connection:
            connection.execute(
                """
                INSERT INTO workers
                    (worker_id, queue, capacity, in_flight, last_seen_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(worker_id) DO UPDATE SET
                    queue = excluded.queue,
                    capacity = excluded.capacity,
                    in_flight = excluded.in_flight,
                    last_seen_at = excluded.last_seen_at
                """,
                (worker_id, queue, capacity, in_flight, now),
            )

    def record_effect(self, job_id: str, effect_key: str, value: Any) -> tuple[Any, bool]:
        """Persist an application-side idempotent effect in the same database."""
        now = self.clock.now()
        with self._transaction() as connection:
            existing = connection.execute(
                "SELECT value FROM effects WHERE job_id = ? AND effect_key = ?",
                (job_id, effect_key),
            ).fetchone()
            if existing:
                return json.loads(existing["value"]), False
            connection.execute(
                """
                INSERT INTO effects (job_id, effect_key, value, recorded_at)
                VALUES (?, ?, ?, ?)
                """,
                (job_id, effect_key, _json(value), now),
            )
        return value, True

    def events(self, job_id: str) -> list[dict[str, Any]]:
        with closing(self._connect()) as connection:
            rows = connection.execute(
                """
                SELECT sequence, event_type, worker_id, attempt, detail, occurred_at
                FROM job_events WHERE job_id = ? ORDER BY sequence
                """,
                (job_id,),
            ).fetchall()
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

    def metrics(self) -> dict[str, Any]:
        now = self.clock.now()
        with closing(self._connect()) as connection:
            statuses = {
                row["status"]: row["count"]
                for row in connection.execute(
                    "SELECT status, COUNT(*) AS count FROM jobs GROUP BY status"
                )
            }
            event_totals = {
                row["event_type"]: row["count"]
                for row in connection.execute(
                    """
                    SELECT event_type, COUNT(*) AS count
                    FROM job_events GROUP BY event_type
                    """
                )
            }
            oldest = connection.execute(
                """
                SELECT MIN(created_at) AS created_at FROM jobs
                WHERE status = 'queued'
                """
            ).fetchone()["created_at"]
            active_workers = connection.execute(
                "SELECT COUNT(*) AS count FROM workers WHERE last_seen_at >= ?",
                (now - 30,),
            ).fetchone()["count"]
        return {
            "jobs": {status.value: statuses.get(status.value, 0) for status in JobStatus},
            "events": event_totals,
            "active_workers": active_workers,
            "oldest_queued_age_seconds": max(0, now - oldest) if oldest else 0,
        }

    def prometheus_metrics(self) -> str:
        snapshot = self.metrics()
        lines = [
            "# HELP durableflow_jobs Current jobs by state.",
            "# TYPE durableflow_jobs gauge",
        ]
        for status, count in snapshot["jobs"].items():
            lines.append(f'durableflow_jobs{{status="{status}"}} {count}')
        lines.extend(
            [
                "# HELP durableflow_events_total Persisted state transition events.",
                "# TYPE durableflow_events_total counter",
            ]
        )
        for event, count in sorted(snapshot["events"].items()):
            lines.append(f'durableflow_events_total{{event="{event}"}} {count}')
        lines.extend(
            [
                "# HELP durableflow_active_workers Workers seen in the last 30 seconds.",
                "# TYPE durableflow_active_workers gauge",
                f"durableflow_active_workers {snapshot['active_workers']}",
                "# HELP durableflow_oldest_queued_age_seconds Age of the oldest queued job.",
                "# TYPE durableflow_oldest_queued_age_seconds gauge",
                "durableflow_oldest_queued_age_seconds "
                f"{snapshot['oldest_queued_age_seconds']:.6f}",
            ]
        )
        return "\n".join(lines) + "\n"

    def backoff_for(self, attempts: int) -> float:
        return min(self.max_backoff, self.base_backoff * (2 ** max(0, attempts - 1)))
