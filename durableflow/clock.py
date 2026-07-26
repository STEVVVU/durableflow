from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field


class SystemClock:
    def now(self) -> float:
        return time.time()


@dataclass
class ManualClock:
    """Thread-safe controllable wall clock for deterministic failure tests."""

    value: float = 1_700_000_000.0
    _lock: threading.Lock = field(default_factory=threading.Lock, init=False)

    def now(self) -> float:
        with self._lock:
            return self.value

    def advance(self, seconds: float) -> float:
        if seconds < 0:
            raise ValueError("clock cannot move backwards")
        with self._lock:
            self.value += seconds
            return self.value
