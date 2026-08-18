# syntax=docker/dockerfile:1

# Build the wheel in a throwaway stage so the runtime image carries neither the
# build toolchain nor the source tree.
FROM python:3.12-slim AS build

WORKDIR /src
ENV PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1

RUN python -m pip install --upgrade pip build

COPY pyproject.toml README.md LICENSE ./
COPY durableflow ./durableflow
RUN python -m build --wheel --outdir /dist


FROM python:3.12-slim AS runtime

# PYTHONUNBUFFERED keeps structured logs flowing to the container log driver
# instead of sitting in a block buffer.
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1 \
    DURABLEFLOW_HOST=0.0.0.0 \
    DURABLEFLOW_PORT=8080

# curl is the healthcheck client. --no-install-recommends keeps the layer small.
RUN apt-get update \
    && apt-get install --no-install-recommends -y curl \
    && rm -rf /var/lib/apt/lists/*

# Run as an unprivileged user with no login shell.
RUN groupadd --system --gid 1001 durableflow \
    && useradd --system --uid 1001 --gid durableflow --create-home \
       --shell /usr/sbin/nologin durableflow

WORKDIR /app

RUN --mount=type=bind,from=build,source=/dist,target=/dist \
    python -m pip install /dist/*.whl "psycopg[binary]>=3.2"

# Migrations are not part of the wheel, so copy the scripts the api/migrate
# commands need at runtime.
COPY alembic.ini ./alembic.ini
COPY alembic ./alembic

# A writable location for the local SQLite fallback; Postgres deployments ignore it.
RUN mkdir -p /app/.durableflow && chown -R durableflow:durableflow /app

USER durableflow

EXPOSE 8080

# Readiness, not liveness: /readyz fails closed when the database is unreachable,
# so an orchestrator replaces a replica that cannot actually accept work.
HEALTHCHECK --interval=10s --timeout=3s --start-period=10s --retries=3 \
    CMD curl --fail --silent http://127.0.0.1:8080/readyz || exit 1

ENTRYPOINT ["durableflow"]
CMD ["api"]
