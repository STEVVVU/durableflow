"""Engine construction and dialect-specific transaction semantics.

Two backends are supported and they claim work differently:

* **SQLite** keeps the original protocol: ``BEGIN IMMEDIATE`` takes the database
  write lock up front, so the claim ``SELECT`` and the conditional ``UPDATE``
  cannot interleave with another claimant. Correct, but claims serialise
  globally.
* **PostgreSQL** uses ``SELECT ... FOR UPDATE SKIP LOCKED``, the industry
  standard queue-claim primitive. Row locks let concurrent claimants pass over
  each other's in-flight rows instead of blocking, so claim throughput scales
  with claimants rather than serialising.

Both paths issue the same fenced ``UPDATE`` afterwards, so the lease/fencing
guarantee is identical; only the contention behaviour differs.
"""

from __future__ import annotations

from pathlib import Path

from sqlalchemy import Engine, create_engine, event, text
from sqlalchemy.pool import QueuePool

SQLITE_PREFIXES = ("sqlite:", "sqlite+pysqlite:")


def normalize_url(url: str | Path) -> str:
    """Accept a bare filesystem path as shorthand for a local SQLite database."""
    candidate = str(url)
    if "://" in candidate:
        return candidate
    return f"sqlite+pysqlite:///{candidate}"


def is_sqlite(url: str) -> bool:
    return url.startswith(SQLITE_PREFIXES)


def is_postgres(url: str) -> bool:
    return url.startswith(("postgresql:", "postgresql+psycopg:"))


def _sqlite_path(url: str) -> str | None:
    """Extract the on-disk path from a SQLite URL, or None for in-memory."""
    _, _, remainder = url.partition(":///")
    if not remainder or remainder == ":memory:":
        return None
    return remainder


def create_db_engine(url: str | Path, *, pool_size: int = 16, echo: bool = False) -> Engine:
    """Build a configured engine, applying the dialect's durability settings."""
    resolved = normalize_url(url)

    if is_sqlite(resolved):
        path = _sqlite_path(resolved)
        if path is not None:
            parent = Path(path).parent
            if str(parent) not in ("", "."):
                parent.mkdir(parents=True, exist_ok=True)
        engine = create_engine(
            resolved,
            echo=echo,
            future=True,
            poolclass=QueuePool,
            pool_size=pool_size,
            max_overflow=pool_size,
            connect_args={"timeout": 10, "check_same_thread": False},
        )
        _install_sqlite_semantics(engine)
        return engine

    if resolved.startswith("postgresql://"):
        # Pin the driver explicitly so psycopg3 is used rather than psycopg2.
        resolved = resolved.replace("postgresql://", "postgresql+psycopg://", 1)

    return create_engine(
        resolved,
        echo=echo,
        future=True,
        pool_size=pool_size,
        max_overflow=pool_size,
        pool_pre_ping=True,
    )


def _install_sqlite_semantics(engine: Engine) -> None:
    """Restore WAL, busy-timeout, and explicit ``BEGIN IMMEDIATE`` write locking.

    pysqlite's driver-level transaction handling both opens transactions lazily
    and emits a plain deferred ``BEGIN``. A deferred begin would upgrade to a
    write lock only at the first ``UPDATE``, which reintroduces exactly the
    read-then-write race the original ``BEGIN IMMEDIATE`` was chosen to exclude.
    Disabling the driver's implicit transaction and emitting our own keeps the
    claim protocol unchanged.
    """

    @event.listens_for(engine, "connect")
    def _configure_connection(dbapi_connection, connection_record):  # noqa: ANN001, ARG001
        dbapi_connection.isolation_level = None
        cursor = dbapi_connection.cursor()
        try:
            cursor.execute("PRAGMA journal_mode=WAL")
            cursor.execute("PRAGMA foreign_keys=ON")
            cursor.execute("PRAGMA busy_timeout=10000")
            cursor.execute("PRAGMA synchronous=NORMAL")
        finally:
            cursor.close()

    @event.listens_for(engine, "begin")
    def _begin_immediate(connection):  # noqa: ANN001
        connection.exec_driver_sql("BEGIN IMMEDIATE")


def create_schema(engine: Engine) -> None:
    """Create tables directly, for tests and the zero-setup local SQLite path.

    Production deployments run ``alembic upgrade head`` instead; both produce the
    same schema because alembic's initial revision is generated from this
    metadata.
    """
    from durableflow.orm import Base

    Base.metadata.create_all(engine)


def ping(engine: Engine) -> bool:
    """Return True when the database answers a trivial query."""
    with engine.connect() as connection:
        connection.execute(text("SELECT 1"))
    return True
