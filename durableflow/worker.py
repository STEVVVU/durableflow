from __future__ import annotations

import threading
import time
import traceback
import uuid
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Any, Mapping

from durableflow.models import Job
from durableflow.store import JobStore
from durableflow.tasks import DEFAULT_TASKS, TaskCancelled, TaskContext, TaskHandler


class Worker:
    """Bounded executor that acquires and renews fenced job leases."""

    def __init__(
        self,
        store: JobStore,
        *,
        worker_id: str | None = None,
        queue: str = "default",
        concurrency: int = 4,
        lease_seconds: float = 10,
        poll_interval: float = 0.1,
        handlers: Mapping[str, TaskHandler] | None = None,
    ) -> None:
        if concurrency < 1:
            raise ValueError("concurrency must be at least 1")
        if lease_seconds <= 0:
            raise ValueError("lease_seconds must be positive")
        self.store = store
        self.worker_id = worker_id or f"worker-{uuid.uuid4().hex[:8]}"
        self.queue = queue
        self.concurrency = concurrency
        self.lease_seconds = lease_seconds
        self.poll_interval = poll_interval
        self.handlers = dict(handlers or DEFAULT_TASKS)
        self._executor = ThreadPoolExecutor(
            max_workers=concurrency,
            thread_name_prefix=self.worker_id,
        )
        self._active: dict[Future[Any], Job] = {}
        self._active_lock = threading.Lock()
        self._stop = threading.Event()
        self._last_heartbeat = 0.0

    def run_once(self) -> int:
        self._reap()
        self._heartbeat_active()
        with self._active_lock:
            available = self.concurrency - len(self._active)
        if available <= 0:
            self._announce()
            return 0
        claimed = self.store.claim(
            self.worker_id,
            queue=self.queue,
            limit=available,
            lease_seconds=self.lease_seconds,
        )
        with self._active_lock:
            for job in claimed:
                future = self._executor.submit(self._execute, job)
                self._active[future] = job
        self._announce()
        return len(claimed)

    def _execute(self, job: Job) -> None:
        assert job.lease_token is not None
        task_name = str(job.payload.get("task", "echo"))
        handler = self.handlers.get(task_name)
        if handler is None:
            self.store.fail(
                job.id,
                self.worker_id,
                job.lease_token,
                f"unknown task: {task_name}",
            )
            return
        context = TaskContext(
            job_id=job.id,
            attempt=job.attempts,
            cancellation_requested=lambda: self.store.cancellation_requested(
                job.id, self.worker_id, job.lease_token or ""
            ),
            record_effect=lambda key, value: self.store.record_effect(
                job.id, key, value
            ),
        )
        try:
            result = handler(job.payload, context)
            self.store.complete(job.id, self.worker_id, job.lease_token, result)
        except TaskCancelled:
            self.store.acknowledge_cancellation(
                job.id, self.worker_id, job.lease_token
            )
        except BaseException as error:
            rendered = "".join(
                traceback.format_exception_only(type(error), error)
            ).strip()
            self.store.fail(job.id, self.worker_id, job.lease_token, rendered)

    def _reap(self) -> None:
        with self._active_lock:
            finished = [future for future in self._active if future.done()]
            for future in finished:
                future.result()
                del self._active[future]

    def _heartbeat_active(self) -> None:
        now = time.monotonic()
        if now - self._last_heartbeat < self.lease_seconds / 3:
            return
        with self._active_lock:
            jobs = list(self._active.values())
        for job in jobs:
            if job.lease_token:
                self.store.heartbeat(
                    job.id,
                    self.worker_id,
                    job.lease_token,
                    lease_seconds=self.lease_seconds,
                )
        self._last_heartbeat = now

    def _announce(self) -> None:
        with self._active_lock:
            in_flight = len(self._active)
        self.store.register_worker(
            self.worker_id, self.queue, self.concurrency, in_flight
        )

    def run_forever(self) -> None:
        try:
            while not self._stop.is_set():
                claimed = self.run_once()
                if claimed == 0:
                    self._stop.wait(self.poll_interval)
        finally:
            self.shutdown(wait=True)

    def stop(self) -> None:
        self._stop.set()

    def shutdown(self, *, wait: bool = True) -> None:
        self._executor.shutdown(wait=wait, cancel_futures=False)
        self._reap()
