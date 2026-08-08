"""Alembic environment.

The database URL comes from DurableFlow settings (``DURABLEFLOW_DATABASE_URL``)
rather than ``alembic.ini``, so migrations always target the same database the
application does. ``render_as_batch`` is enabled for SQLite because it lacks
most forms of ``ALTER TABLE``.
"""

from __future__ import annotations

from logging.config import fileConfig

from alembic import context
from durableflow.config import load_settings
from durableflow.engine import create_db_engine, is_sqlite, normalize_url
from durableflow.orm import Base

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = Base.metadata


def _database_url() -> str:
    """Prefer an explicit ``-x url=...`` argument, then settings."""
    override = context.get_x_argument(as_dictionary=True).get("url")
    if override:
        return normalize_url(override)
    configured = config.get_main_option("sqlalchemy.url", default=None)
    if configured:
        return normalize_url(configured)
    return normalize_url(load_settings().database_url)


def run_migrations_offline() -> None:
    """Emit SQL to stdout without connecting."""
    url = _database_url()
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        render_as_batch=is_sqlite(url),
        compare_type=True,
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations against a live connection."""
    url = _database_url()
    engine = create_db_engine(url)
    with engine.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            render_as_batch=is_sqlite(url),
            compare_type=True,
        )
        with context.begin_transaction():
            context.run_migrations()
    engine.dispose()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
