from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

DEFAULT_SQLITE_URL = "sqlite+pysqlite:///.durableflow/durableflow.db"


class Settings(BaseSettings):
    """Process configuration, overridable by ``DURABLEFLOW_*`` environment variables."""

    model_config = SettingsConfigDict(
        env_prefix="DURABLEFLOW_",
        env_file=".env",
        extra="ignore",
    )

    database_url: str = DEFAULT_SQLITE_URL
    host: str = "127.0.0.1"
    port: int = 8080

    base_backoff: float = 1.0
    max_backoff: float = 60.0

    log_level: str = "INFO"
    log_json: bool = True

    # Traces are exported only when an OTLP endpoint is configured; otherwise the
    # tracer provider stays local so nothing blocks on a missing collector.
    otel_exporter_otlp_endpoint: str | None = None
    otel_service_name: str = "durableflow"
    otel_console_export: bool = False

    sweep_interval: float = Field(default=1.0, gt=0)


def load_settings(**overrides: object) -> Settings:
    """Build settings from the environment, applying explicit overrides last."""
    return Settings(**overrides)  # type: ignore[arg-type]
