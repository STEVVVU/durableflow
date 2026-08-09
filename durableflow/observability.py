"""Structured logging, Prometheus metrics, and OpenTelemetry tracing.

Metrics are split by intent:

* **Counters** are incremented at the point a transition is committed, so they
  record flow over time (retries scheduled, jobs dead-lettered, leases expired).
* **Gauges** describe current depth and are refreshed by a collector callback
  that reads the database when ``/metrics`` is scraped, so a freshly restarted
  process reports true queue depth rather than zero.
"""

from __future__ import annotations

import logging
import sys
from collections.abc import Callable
from typing import Any

import structlog
from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram

# A dedicated registry keeps test processes isolated from the default global one
# and makes duplicate registration errors impossible across repeated app builds.
REGISTRY = CollectorRegistry()

JOBS_SUBMITTED = Counter(
    "durableflow_jobs_submitted_total",
    "Jobs accepted by the API, excluding idempotent duplicates.",
    ["queue"],
    registry=REGISTRY,
)
JOBS_DUPLICATE_SUBMISSIONS = Counter(
    "durableflow_duplicate_submissions_total",
    "Submissions short-circuited by an existing idempotency key.",
    ["queue"],
    registry=REGISTRY,
)
JOBS_CLAIMED = Counter(
    "durableflow_jobs_claimed_total",
    "Job claims granted with a fenced lease.",
    ["queue"],
    registry=REGISTRY,
)
JOBS_SUCCEEDED = Counter(
    "durableflow_jobs_succeeded_total",
    "Jobs that reached a successful terminal state.",
    registry=REGISTRY,
)
RETRIES_SCHEDULED = Counter(
    "durableflow_retries_scheduled_total",
    "Failed attempts rescheduled with exponential backoff.",
    registry=REGISTRY,
)
JOBS_DEAD_LETTERED = Counter(
    "durableflow_jobs_dead_lettered_total",
    "Jobs that exhausted their attempt budget and await operator redrive.",
    registry=REGISTRY,
)
JOBS_REDRIVEN = Counter(
    "durableflow_jobs_redriven_total",
    "Dead-letter jobs reset and requeued by an operator.",
    registry=REGISTRY,
)
JOBS_CANCELLED = Counter(
    "durableflow_jobs_cancelled_total",
    "Jobs that reached the cancelled terminal state.",
    registry=REGISTRY,
)
LEASES_EXPIRED = Counter(
    "durableflow_leases_expired_total",
    "Leases reclaimed by the coordinator after a worker stopped heartbeating.",
    registry=REGISTRY,
)
STALE_FENCE_REJECTIONS = Counter(
    "durableflow_stale_fence_rejections_total",
    "Writes rejected because the lease token or expiry no longer matched.",
    ["operation"],
    registry=REGISTRY,
)
CLAIM_LATENCY = Histogram(
    "durableflow_claim_seconds",
    "Time spent inside a claim transaction.",
    registry=REGISTRY,
)

QUEUE_DEPTH = Gauge(
    "durableflow_jobs",
    "Current jobs by state.",
    ["status"],
    registry=REGISTRY,
)
ACTIVE_WORKERS = Gauge(
    "durableflow_active_workers",
    "Workers seen in the last 30 seconds.",
    registry=REGISTRY,
)
OLDEST_QUEUED_AGE = Gauge(
    "durableflow_oldest_queued_age_seconds",
    "Age of the oldest queued job.",
    registry=REGISTRY,
)


def configure_logging(*, level: str = "INFO", json_logs: bool = True) -> None:
    """Route stdlib logging through structlog with one consistent renderer."""
    renderer: Any = (
        structlog.processors.JSONRenderer(sort_keys=True)
        if json_logs
        else structlog.dev.ConsoleRenderer(colors=False)
    )
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            renderer,
        ],
        wrapper_class=structlog.make_filtering_bound_logger(
            logging.getLevelNamesMapping().get(level.upper(), logging.INFO)
        ),
        logger_factory=structlog.PrintLoggerFactory(file=sys.stdout),
        cache_logger_on_first_use=True,
    )


def get_logger(component: str) -> Any:
    """Return a logger bound to a component name, matching the previous log shape.

    Passing the binding to ``get_logger`` rather than calling ``.bind()`` returns a
    lazy proxy that resolves configuration on first use. Modules bind their logger
    at import time, which happens before ``configure_logging`` runs; ``.bind()``
    would materialise the logger immediately and freeze the default renderer,
    silently ignoring the configured one.
    """
    return structlog.get_logger(component=component)


def refresh_gauges(snapshot: dict[str, Any]) -> None:
    """Publish a store metrics snapshot into the Prometheus gauges."""
    for status, count in snapshot.get("jobs", {}).items():
        QUEUE_DEPTH.labels(status=status).set(count)
    ACTIVE_WORKERS.set(snapshot.get("active_workers", 0))
    OLDEST_QUEUED_AGE.set(snapshot.get("oldest_queued_age_seconds", 0))


def configure_tracing(
    *,
    service_name: str = "durableflow",
    endpoint: str | None = None,
    console_export: bool = False,
) -> Any:
    """Install a tracer provider, exporting only when a destination is configured.

    With no OTLP endpoint and no console exporter the provider records spans
    without shipping them anywhere. That keeps instrumentation always-on and
    free of a hard dependency on a running collector.
    """
    from opentelemetry import trace
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter

    provider = TracerProvider(resource=Resource.create({"service.name": service_name}))
    if endpoint:
        from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter

        provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint=endpoint)))
    if console_export:
        provider.add_span_processor(BatchSpanProcessor(ConsoleSpanExporter()))
    trace.set_tracer_provider(provider)
    return provider


def get_tracer(name: str = "durableflow") -> Any:
    from opentelemetry import trace

    return trace.get_tracer(name)


# Counter dispatch for persisted event types. The store records one event per
# committed transition, so mapping event type -> counter keeps metrics and the
# durable audit trail from drifting apart.
_EVENT_COUNTERS: dict[str, Callable[[], None]] = {
    "succeeded": JOBS_SUCCEEDED.inc,
    "retry_scheduled": RETRIES_SCHEDULED.inc,
    "dead_lettered": JOBS_DEAD_LETTERED.inc,
    "lease_expired_requeued": LEASES_EXPIRED.inc,
    "cancelled": JOBS_CANCELLED.inc,
    "redriven": JOBS_REDRIVEN.inc,
}


def record_event_metric(event_type: str) -> None:
    """Increment the counter matching a persisted transition event."""
    if event_type == "lease_expired_dead_lettered":
        # Counts in both families: a lease was lost and the job also exhausted
        # its attempts, so neither counter alone would tell the whole story.
        LEASES_EXPIRED.inc()
        JOBS_DEAD_LETTERED.inc()
        return
    handler = _EVENT_COUNTERS.get(event_type)
    if handler is not None:
        handler()
