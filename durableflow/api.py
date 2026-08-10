from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Annotated, Any

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request, Response, status
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, PlainTextResponse

from durableflow import observability
from durableflow.config import Settings, load_settings
from durableflow.engine import ping
from durableflow.models import Job, JobStatus
from durableflow.schemas import (
    ErrorResponse,
    HealthResponse,
    JobEventsResponse,
    JobListResponse,
    JobResponse,
    ReadinessResponse,
    SubmitJobRequest,
    SubmitJobResponse,
)
from durableflow.store import JobStore

LOGGER = observability.get_logger("api")

PROMETHEUS_CONTENT_TYPE = "text/plain; version=0.0.4; charset=utf-8"

# Body size cap. Starlette streams the request body, so this is enforced from the
# Content-Length header before the payload is buffered and parsed.
MAX_BODY_BYTES = 1_048_576


def job_document(job: Job) -> dict[str, Any]:
    """Serialise a job to the wire document shape."""
    return JobResponse.from_job(job).model_dump()


def get_store(request: Request) -> JobStore:
    """Resolve the process-wide store from application state."""
    store: JobStore | None = getattr(request.app.state, "store", None)
    if store is None:  # pragma: no cover - defensive
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="store unavailable"
        )
    return store


StoreDep = Annotated[JobStore, Depends(get_store)]


def create_app(
    store: JobStore | None = None,
    *,
    settings: Settings | None = None,
    instrument: bool = True,
) -> FastAPI:
    """Build the ASGI application.

    Passing a ``store`` hands ownership to the caller (tests, embedded use); the
    app will not close it. Without one, the lifespan builds and disposes a store
    from settings.
    """
    resolved_settings = settings or load_settings()
    external_store = store is not None

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        owned: JobStore | None = None
        if external_store:
            app.state.store = store
        else:
            owned = JobStore(
                resolved_settings.database_url,
                base_backoff=resolved_settings.base_backoff,
                max_backoff=resolved_settings.max_backoff,
            )
            app.state.store = owned
        LOGGER.info(
            "api starting",
            database=app.state.store.engine.url.render_as_string(hide_password=True),
            skip_locked=app.state.store.uses_skip_locked,
        )
        try:
            yield
        finally:
            if owned is not None:
                owned.close()
            LOGGER.info("api stopped")

    app = FastAPI(
        title="DurableFlow",
        version="0.2.0",
        description=(
            "Durable job orchestration with fenced worker leases, abandoned-work "
            "recovery, exponential backoff, and dead-letter redrive."
        ),
        lifespan=lifespan,
    )

    if instrument:
        _instrument(app, resolved_settings)

    _register_routes(app)
    return app


def _instrument(app: FastAPI, settings: Settings) -> None:
    """Attach OpenTelemetry tracing to the ASGI app."""
    from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

    observability.configure_tracing(
        service_name=settings.otel_service_name,
        endpoint=settings.otel_exporter_otlp_endpoint,
        console_export=settings.otel_console_export,
    )
    # Health and metrics scrapes would otherwise dominate span volume.
    FastAPIInstrumentor.instrument_app(app, excluded_urls="healthz,readyz,metrics")


def _register_routes(app: FastAPI) -> None:
    @app.middleware("http")
    async def enforce_body_limit(request: Request, call_next):  # noqa: ANN001, ANN202
        """Reject oversized bodies up front, as the previous handler did."""
        content_length = request.headers.get("content-length")
        if content_length is not None:
            try:
                declared = int(content_length)
            except ValueError:
                return JSONResponse(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    content={"error": "invalid Content-Length"},
                )
            if declared > MAX_BODY_BYTES:
                return JSONResponse(
                    status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
                    content={"error": f"body must be at most {MAX_BODY_BYTES} bytes"},
                )
        return await call_next(request)

    @app.exception_handler(HTTPException)
    async def http_exception_handler(request: Request, exc: HTTPException) -> JSONResponse:
        """Render errors as ``{"error": ...}``, preserving the original contract."""
        return JSONResponse(status_code=exc.status_code, content={"error": exc.detail})

    @app.exception_handler(RequestValidationError)
    async def validation_exception_handler(
        request: Request, exc: RequestValidationError
    ) -> JSONResponse:
        """Keep the ``{"error": ...}`` envelope on schema validation failures.

        The status code is pydantic/FastAPI's 422 rather than the previous 400 --
        a deliberate move to the framework convention -- but the body shape stays
        uniform with every other error this API returns, and ``details`` carries
        the per-field diagnostics the old hand-rolled validator never produced.
        """
        return JSONResponse(
            # Literal 422: starlette renamed the constant
            # (UNPROCESSABLE_ENTITY -> UNPROCESSABLE_CONTENT), so referencing
            # either name emits a deprecation warning on one version or breaks on
            # the other.
            status_code=422,
            content={
                "error": "request validation failed",
                "details": jsonable_encoder(exc.errors()),
            },
        )

    @app.get("/healthz", response_model=HealthResponse, tags=["ops"])
    async def healthz() -> HealthResponse:
        """Liveness: the process is running and serving requests."""
        return HealthResponse(status="ok")

    @app.get("/readyz", response_model=ReadinessResponse, tags=["ops"])
    async def readyz(store: StoreDep, response: Response) -> ReadinessResponse:
        """Readiness: the database is reachable, so work can actually be accepted.

        This is the container healthcheck target. It fails closed with 503 so an
        orchestrator replaces a replica that has lost its database.
        """
        backend = store.engine.dialect.name
        try:
            ping(store.engine)
        except Exception as error:  # noqa: BLE001 - reported, not raised
            LOGGER.error("readiness probe failed", error=str(error), backend=backend)
            response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
            return ReadinessResponse(status="unready", database="unreachable", backend=backend)
        return ReadinessResponse(status="ready", database="ok", backend=backend)

    @app.get("/metrics", response_class=PlainTextResponse, tags=["ops"])
    async def metrics(store: StoreDep) -> Response:
        """Prometheus text exposition for queue depth, retries, DLQ, and leases."""
        return PlainTextResponse(store.prometheus_metrics(), media_type=PROMETHEUS_CONTENT_TYPE)

    @app.post(
        "/v1/jobs",
        response_model=SubmitJobResponse,
        status_code=status.HTTP_201_CREATED,
        responses={400: {"model": ErrorResponse}},
        tags=["jobs"],
    )
    async def submit_job(
        body: SubmitJobRequest,
        store: StoreDep,
        response: Response,
        idempotency_key: Annotated[str | None, Header(alias="Idempotency-Key")] = None,
    ) -> SubmitJobResponse:
        """Submit a job.

        Returns 201 for a new job and 200 when an idempotency key matched an
        existing one. The ``Idempotency-Key`` header takes precedence over the
        body field, matching the previous behaviour.
        """
        key = idempotency_key if idempotency_key is not None else body.idempotency_key
        try:
            submission = store.submit(
                body.payload,
                queue=body.queue,
                idempotency_key=key,
                max_attempts=body.max_attempts,
                delay_seconds=body.delay_seconds,
            )
        except (TypeError, ValueError) as error:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST, detail=str(error)
            ) from error
        if not submission.created:
            response.status_code = status.HTTP_200_OK
        return SubmitJobResponse(
            created=submission.created,
            job=JobResponse.from_job(submission.job),
        )

    @app.get("/v1/jobs", response_model=JobListResponse, tags=["jobs"])
    async def list_jobs(
        store: StoreDep,
        job_status: Annotated[JobStatus | None, Query(alias="status")] = None,
        limit: Annotated[int, Query(ge=1, le=1000)] = 100,
    ) -> JobListResponse:
        """List jobs, most recently created first, optionally filtered by status."""
        jobs = store.list_jobs(status=job_status, limit=limit)
        return JobListResponse(jobs=[JobResponse.from_job(job) for job in jobs])

    @app.get(
        "/v1/jobs/{job_id}",
        response_model=JobResponse,
        responses={404: {"model": ErrorResponse}},
        tags=["jobs"],
    )
    async def get_job(job_id: str, store: StoreDep) -> JobResponse:
        """Read current durable state."""
        job = store.get(job_id)
        if job is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="job not found")
        return JobResponse.from_job(job)

    @app.get(
        "/v1/jobs/{job_id}/events",
        response_model=JobEventsResponse,
        responses={404: {"model": ErrorResponse}},
        tags=["jobs"],
    )
    async def get_job_events(job_id: str, store: StoreDep) -> JobEventsResponse:
        """Read the ordered transition history."""
        job = store.get(job_id)
        if job is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="job not found")
        return JobEventsResponse.model_validate(
            {"job_id": job.id, "events": store.events(job.id)}
        )

    @app.post(
        "/v1/jobs/{job_id}/cancel",
        response_model=JobResponse,
        status_code=status.HTTP_202_ACCEPTED,
        responses={409: {"model": ErrorResponse}},
        tags=["jobs"],
    )
    async def cancel_job(job_id: str, store: StoreDep) -> JobResponse:
        """Cancel queued work outright, or request cooperative cancellation."""
        return _apply_transition(store, job_id, "cancel")

    @app.post(
        "/v1/jobs/{job_id}/redrive",
        response_model=JobResponse,
        status_code=status.HTTP_202_ACCEPTED,
        responses={409: {"model": ErrorResponse}},
        tags=["jobs"],
    )
    async def redrive_job(job_id: str, store: StoreDep) -> JobResponse:
        """Reset attempts and requeue a dead-letter job."""
        return _apply_transition(store, job_id, "redrive")


def _apply_transition(store: JobStore, job_id: str, operation: str) -> JobResponse:
    """Run cancel/redrive, mapping a refused transition to 409."""
    changed = store.cancel(job_id) if operation == "cancel" else store.redrive(job_id)
    if not changed:
        verb = "cancelled" if operation == "cancel" else "redriven"
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"job cannot be {verb} from its current state",
        )
    job = store.get(job_id)
    if job is None:  # pragma: no cover - the transition just succeeded
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="job not found")
    return JobResponse.from_job(job)


def serve(store: JobStore, host: str, port: int, *, settings: Settings | None = None) -> None:
    """Run the API with uvicorn, serving the supplied store."""
    import uvicorn

    app = create_app(store, settings=settings)
    LOGGER.info("DurableFlow API listening", host=host, port=port)
    # log_config=None keeps uvicorn from replacing the structlog-backed handlers.
    uvicorn.run(app, host=host, port=port, log_config=None)
