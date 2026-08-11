from __future__ import annotations

import json
import threading
import time
import urllib.request
from collections.abc import Iterator
from pathlib import Path

import pytest
import uvicorn
from fastapi.testclient import TestClient

from durableflow.api import create_app
from durableflow.store import JobStore


@pytest.fixture
def api_store(tmp_path: Path) -> Iterator[JobStore]:
    store = JobStore(tmp_path / "api.db")
    try:
        yield store
    finally:
        store.close()


@pytest.fixture
def client(api_store: JobStore) -> Iterator[TestClient]:
    # instrument=False keeps OpenTelemetry out of the per-test app build.
    with TestClient(create_app(api_store, instrument=False)) as test_client:
        yield test_client


def test_submit_is_idempotent_and_job_is_observable(client: TestClient) -> None:
    body = {"payload": {"task": "echo", "value": "hello"}}
    headers = {"Idempotency-Key": "api-request-1"}

    first = client.post("/v1/jobs", json=body, headers=headers)
    assert first.status_code == 201
    duplicate = client.post("/v1/jobs", json=body, headers=headers)
    assert duplicate.status_code == 200

    first_document = first.json()
    duplicate_document = duplicate.json()
    assert first_document["created"]
    assert not duplicate_document["created"]
    assert first_document["job"]["id"] == duplicate_document["job"]["id"]

    job_id = first_document["job"]["id"]
    events = client.get(f"/v1/jobs/{job_id}/events").json()["events"]
    assert [event["event_type"] for event in events] == ["submitted"]

    metrics = client.get("/metrics").text
    assert 'durableflow_jobs{status="queued"} 1.0' in metrics


def test_metrics_exposes_queue_and_retry_families(client: TestClient) -> None:
    client.post("/v1/jobs", json={"payload": {"task": "echo"}})
    metrics = client.get("/metrics").text
    for family in (
        "durableflow_jobs",
        "durableflow_jobs_submitted_total",
        "durableflow_retries_scheduled_total",
        "durableflow_jobs_dead_lettered_total",
        "durableflow_leases_expired_total",
        "durableflow_oldest_queued_age_seconds",
    ):
        assert family in metrics


def test_health_and_readiness(client: TestClient) -> None:
    assert client.get("/healthz").json() == {"status": "ok"}
    readiness = client.get("/readyz")
    assert readiness.status_code == 200
    assert readiness.json() == {"status": "ready", "database": "ok", "backend": "sqlite"}


def test_unknown_job_returns_error_envelope(client: TestClient) -> None:
    response = client.get("/v1/jobs/missing")
    assert response.status_code == 404
    assert response.json() == {"error": "job not found"}


def test_invalid_payload_is_rejected(client: TestClient) -> None:
    response = client.post("/v1/jobs", json={"payload": "not-an-object"})
    assert response.status_code == 422
    assert response.json()["error"] == "request validation failed"


def test_body_field_idempotency_key_is_honoured(client: TestClient) -> None:
    body = {"payload": {"task": "echo"}, "idempotency_key": "from-body"}
    assert client.post("/v1/jobs", json=body).status_code == 201
    assert client.post("/v1/jobs", json=body).status_code == 200


def test_header_overrides_body_idempotency_key(client: TestClient) -> None:
    """The header wins, matching the pre-migration precedence."""
    body = {"payload": {"task": "echo"}, "idempotency_key": "body-key"}
    first = client.post("/v1/jobs", json=body, headers={"Idempotency-Key": "header-key"})
    assert first.status_code == 201
    # Re-submitting with only the body key must create a distinct job, proving
    # the first submission was stored under the header's key.
    second = client.post("/v1/jobs", json=body)
    assert second.status_code == 201
    assert second.json()["job"]["id"] != first.json()["job"]["id"]


def test_cancel_then_redrive_conflicts(client: TestClient) -> None:
    job_id = client.post("/v1/jobs", json={"payload": {"task": "echo"}}).json()["job"]["id"]
    assert client.post(f"/v1/jobs/{job_id}/cancel").status_code == 202
    conflict = client.post(f"/v1/jobs/{job_id}/cancel")
    assert conflict.status_code == 409
    assert conflict.json() == {"error": "job cannot be cancelled from its current state"}
    assert client.post(f"/v1/jobs/{job_id}/redrive").status_code == 409


def test_list_jobs_filters_by_status(client: TestClient) -> None:
    client.post("/v1/jobs", json={"payload": {"task": "echo"}})
    assert len(client.get("/v1/jobs", params={"status": "queued"}).json()["jobs"]) == 1
    assert client.get("/v1/jobs", params={"status": "succeeded"}).json()["jobs"] == []
    # An unknown status is a schema violation, not a silent empty list.
    assert client.get("/v1/jobs", params={"status": "bogus"}).status_code == 422


def test_openapi_schema_is_published(client: TestClient) -> None:
    schema = client.get("/openapi.json").json()
    assert "/v1/jobs" in schema["paths"]
    assert "SubmitJobRequest" in schema["components"]["schemas"]


def test_live_uvicorn_server_serves_the_contract(api_store: JobStore) -> None:
    """End-to-end check over a real socket, as the pre-migration suite did.

    The in-process TestClient bypasses the ASGI server, so this is the only test
    that proves uvicorn actually serves the app.
    """
    app = create_app(api_store, instrument=False)
    config = uvicorn.Config(app, host="127.0.0.1", port=0, log_config=None, lifespan="on")
    server = uvicorn.Server(config)
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()

    deadline = time.monotonic() + 15
    port = None
    while time.monotonic() < deadline:
        if server.started and server.servers:
            port = server.servers[0].sockets[0].getsockname()[1]
            break
        time.sleep(0.05)
    assert port is not None, "uvicorn did not start in time"

    try:
        base_url = f"http://127.0.0.1:{port}"
        request = urllib.request.Request(
            f"{base_url}/v1/jobs",
            data=json.dumps({"payload": {"task": "echo", "value": "live"}}).encode(),
            headers={
                "Content-Type": "application/json",
                "Idempotency-Key": "live-request-1",
            },
            method="POST",
        )
        with urllib.request.urlopen(request, timeout=10) as response:
            assert response.status == 201
            document = json.load(response)
        assert document["created"]

        with urllib.request.urlopen(f"{base_url}/metrics", timeout=10) as response:
            assert 'durableflow_jobs{status="queued"} 1.0' in response.read().decode()
    finally:
        server.should_exit = True
        thread.join(timeout=15)
