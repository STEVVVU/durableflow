"""Backend selection, migration parity, and settings resolution."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from durableflow.config import Settings, load_settings
from durableflow.engine import (
    create_db_engine,
    create_schema,
    is_postgres,
    is_sqlite,
    normalize_url,
)


def test_bare_path_is_treated_as_sqlite() -> None:
    assert normalize_url("jobs.db") == "sqlite+pysqlite:///jobs.db"
    assert is_sqlite(normalize_url("jobs.db"))
    assert not is_postgres(normalize_url("jobs.db"))


def test_explicit_urls_are_preserved() -> None:
    postgres = "postgresql+psycopg://user:pw@host:5432/durableflow"
    assert normalize_url(postgres) == postgres
    assert is_postgres(postgres)
    assert not is_sqlite(postgres)


def test_bare_postgres_url_is_pinned_to_psycopg3() -> None:
    """psycopg2 is not a dependency, so the driver must be made explicit."""
    engine = create_db_engine("postgresql://user:pw@host:5432/durableflow")
    assert engine.url.drivername == "postgresql+psycopg"
    engine.dispose()


def test_sqlite_engine_applies_wal_and_immediate_transactions(tmp_path: Path) -> None:
    engine = create_db_engine(f"sqlite+pysqlite:///{tmp_path / 'wal.db'}")
    with engine.connect() as connection:
        mode = connection.exec_driver_sql("PRAGMA journal_mode").scalar()
        foreign_keys = connection.exec_driver_sql("PRAGMA foreign_keys").scalar()
    assert str(mode).lower() == "wal"
    assert foreign_keys == 1
    engine.dispose()


def test_begin_immediate_is_scoped_to_write_transactions(tmp_path: Path) -> None:
    """Read connections must not take SQLite's write lock.

    Regression guard: SQLAlchemy fires the ``begin`` event for read connections
    too. Emitting ``BEGIN IMMEDIATE`` unconditionally makes every read acquire
    the database-wide write lock, which serialises readers against writers,
    negates WAL mode, and produces "database is locked" under concurrency.
    """
    from sqlalchemy import event, text

    from durableflow.engine import begin_write

    engine = create_db_engine(f"sqlite+pysqlite:///{tmp_path / 'scoped.db'}")
    with begin_write(engine) as connection:
        connection.execute(text("CREATE TABLE t (id INTEGER PRIMARY KEY)"))

    immediate_begins: list[str] = []

    @event.listens_for(engine, "begin")
    def _spy(connection) -> None:  # noqa: ANN001
        if connection.get_execution_options().get("durableflow_write"):
            immediate_begins.append("write")
        else:
            immediate_begins.append("read")

    with engine.connect() as connection:
        connection.execute(text("SELECT 1"))
    assert immediate_begins == ["read"], "read path must not be marked as a write"

    immediate_begins.clear()
    with begin_write(engine) as connection:
        connection.execute(text("INSERT INTO t VALUES (1)"))
    assert immediate_begins == ["write"]

    # A reader must not block while a write transaction is open.
    with begin_write(engine) as writer:
        writer.execute(text("INSERT INTO t VALUES (2)"))
        with engine.connect() as reader:
            assert reader.execute(text("SELECT count(*) FROM t")).scalar_one() == 1

    engine.dispose()


def test_concurrent_writers_do_not_raise_database_is_locked(tmp_path: Path) -> None:
    """Concurrent write transactions must serialise via busy_timeout, not fail."""
    import threading

    from durableflow.store import JobStore

    store = JobStore(tmp_path / "contended.db")
    errors: list[str] = []

    def submit_many(worker: int) -> None:
        try:
            for number in range(10):
                store.submit(
                    {"task": "echo", "value": number},
                    idempotency_key=f"{worker}-{number}",
                )
        except Exception as error:  # noqa: BLE001 - recorded for the assertion
            errors.append(repr(error))

    threads = [threading.Thread(target=submit_many, args=(worker,)) for worker in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert errors == []
    assert len(store.list_jobs(limit=1000)) == 80
    store.close()


def test_sqlite_parent_directory_is_created(tmp_path: Path) -> None:
    target = tmp_path / "nested" / "deeper" / "jobs.db"
    engine = create_db_engine(f"sqlite+pysqlite:///{target}")
    create_schema(engine)
    engine.dispose()
    assert target.exists()


def test_partial_unique_index_allows_many_null_keys(tmp_path: Path) -> None:
    """Unlimited rows may omit an idempotency key; duplicates are still rejected."""
    from durableflow.store import JobStore

    store = JobStore(tmp_path / "idem.db")
    for _ in range(5):
        store.submit({"task": "echo"})
    assert len(store.list_jobs()) == 5

    store.submit({"task": "echo"}, idempotency_key="dup")
    duplicate = store.submit({"task": "echo"}, idempotency_key="dup")
    assert not duplicate.created

    # Same key in a different queue is a distinct job: the index is queue-scoped.
    other_queue = store.submit({"task": "echo"}, queue="other", idempotency_key="dup")
    assert other_queue.created
    store.close()


def test_event_sequence_is_monotonic_and_never_reused(tmp_path: Path) -> None:
    """AUTOINCREMENT semantics keep the audit trail strictly ordered."""
    from durableflow.store import JobStore

    store = JobStore(tmp_path / "events.db")
    job = store.submit({"task": "echo"}).job
    lease = store.claim("worker")[0]
    store.complete(job.id, "worker", lease.lease_token or "", {"ok": True})
    sequences = [event["sequence"] for event in store.events(job.id)]
    assert sequences == sorted(sequences)
    assert len(set(sequences)) == len(sequences)
    store.close()


def test_settings_read_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DURABLEFLOW_DATABASE_URL", "postgresql+psycopg://h/db")
    monkeypatch.setenv("DURABLEFLOW_PORT", "9999")
    settings = load_settings()
    assert settings.database_url == "postgresql+psycopg://h/db"
    assert settings.port == 9999


def test_explicit_overrides_beat_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DURABLEFLOW_LOG_LEVEL", "WARNING")
    assert load_settings(log_level="DEBUG").log_level == "DEBUG"


def test_sweep_interval_must_be_positive() -> None:
    with pytest.raises(ValueError):
        Settings(sweep_interval=0)


def test_migrations_match_orm_metadata(tmp_path: Path) -> None:
    """``alembic upgrade head`` must produce the same schema as ``create_all``.

    Without this, production (migrated) and tests (metadata-created) could drift
    apart silently.
    """
    from alembic.config import Config

    from alembic import command

    root = Path(__file__).resolve().parent.parent
    migrated = tmp_path / "migrated.db"
    metadata_built = tmp_path / "metadata.db"

    config = Config(str(root / "alembic.ini"))
    config.set_main_option("script_location", str(root / "alembic"))
    config.set_main_option("sqlalchemy.url", f"sqlite+pysqlite:///{migrated}")
    command.upgrade(config, "head")

    engine = create_db_engine(f"sqlite+pysqlite:///{metadata_built}")
    create_schema(engine)
    engine.dispose()

    def schema_of(path: Path) -> dict[tuple[str, str], list[str]]:
        connection = sqlite3.connect(path)
        try:
            rows = connection.execute(
                "SELECT type, name, sql FROM sqlite_master "
                "WHERE name NOT LIKE 'sqlite_%' AND name != 'alembic_version' "
                "ORDER BY type, name"
            ).fetchall()
        finally:
            connection.close()
        # Compare token sequences so incidental whitespace does not matter.
        return {(kind, name): (sql or "").split() for kind, name, sql in rows}

    assert schema_of(migrated) == schema_of(metadata_built)
