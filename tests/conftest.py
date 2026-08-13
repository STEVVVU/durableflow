"""Shared fixtures.

The default backend is SQLite so the suite runs with no external services. Tests
marked ``postgres`` are skipped unless ``DURABLEFLOW_TEST_POSTGRES_URL`` points at
a reachable database; CI supplies one from the runner's ``services: postgres``.
"""

from __future__ import annotations

import os
from collections.abc import Iterator
from pathlib import Path

import pytest

from durableflow.clock import ManualClock
from durableflow.observability import configure_logging
from durableflow.store import JobStore

POSTGRES_URL_ENV = "DURABLEFLOW_TEST_POSTGRES_URL"


@pytest.fixture(scope="session", autouse=True)
def _quiet_logs() -> None:
    """Keep expected fence-rejection warnings from flooding test output."""
    configure_logging(level="ERROR", json_logs=True)


@pytest.fixture
def clock() -> ManualClock:
    return ManualClock()


@pytest.fixture
def database_path(tmp_path: Path) -> Path:
    return tmp_path / "test.db"


@pytest.fixture
def store(database_path: Path, clock: ManualClock) -> Iterator[JobStore]:
    """A SQLite-backed store on a manual clock."""
    job_store = JobStore(database_path, clock=clock, base_backoff=1, max_backoff=10)
    try:
        yield job_store
    finally:
        job_store.close()


def postgres_url() -> str | None:
    return os.environ.get(POSTGRES_URL_ENV)


def require_postgres() -> str:
    url = postgres_url()
    if not url:
        pytest.skip(f"set {POSTGRES_URL_ENV} to run PostgreSQL-backed tests")
    return url


@pytest.fixture
def postgres_store(clock: ManualClock) -> Iterator[JobStore]:
    """A PostgreSQL-backed store exercising the SKIP LOCKED claim path."""
    url = require_postgres()
    job_store = JobStore(url, clock=clock, base_backoff=1, max_backoff=10)
    job_store.purge()
    try:
        yield job_store
    finally:
        job_store.purge()
        job_store.close()
