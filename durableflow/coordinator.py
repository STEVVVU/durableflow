from __future__ import annotations

import threading

from durableflow import observability
from durableflow.store import JobStore

LOGGER = observability.get_logger("coordinator")


class Coordinator:
    """Stateless lease-recovery loop; safe to restart at any point."""

    def __init__(self, store: JobStore, *, sweep_interval: float = 1.0) -> None:
        if sweep_interval <= 0:
            raise ValueError("sweep_interval must be positive")
        self.store = store
        self.sweep_interval = sweep_interval
        self._stop = threading.Event()

    def tick(self) -> int:
        return self.store.recover_expired()

    def run_forever(self) -> None:
        LOGGER.info(
            "coordinator sweeping",
            sweep_interval=self.sweep_interval,
            skip_locked=self.store.uses_skip_locked,
        )
        while not self._stop.is_set():
            recovered = self.tick()
            if recovered:
                LOGGER.info("sweep recovered abandoned work", recovered=recovered)
            self._stop.wait(self.sweep_interval)

    def stop(self) -> None:
        self._stop.set()
