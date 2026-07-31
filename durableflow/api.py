from __future__ import annotations

import json
from dataclasses import asdict
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlparse

from durableflow.models import Job, JobStatus
from durableflow.store import JobStore


MAX_BODY_BYTES = 1_048_576


def job_document(job: Job) -> dict[str, Any]:
    document = asdict(job)
    document["status"] = job.status.value
    return document


def handler_for(store: JobStore) -> type[BaseHTTPRequestHandler]:
    class DurableFlowHandler(BaseHTTPRequestHandler):
        server_version = "DurableFlow/0.1"

        def do_GET(self) -> None:
            parsed = urlparse(self.path)
            if parsed.path == "/healthz":
                self._send_json(HTTPStatus.OK, {"status": "ok"})
                return
            if parsed.path == "/metrics":
                body = store.prometheus_metrics().encode()
                self.send_response(HTTPStatus.OK)
                self.send_header("Content-Type", "text/plain; version=0.0.4")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return
            if parsed.path == "/v1/jobs":
                query = parse_qs(parsed.query)
                try:
                    status = (
                        JobStatus(query["status"][0])
                        if query.get("status")
                        else None
                    )
                    limit = int(query.get("limit", ["100"])[0])
                except (ValueError, KeyError):
                    self._error(HTTPStatus.BAD_REQUEST, "invalid query parameters")
                    return
                jobs = store.list_jobs(status=status, limit=limit)
                self._send_json(
                    HTTPStatus.OK, {"jobs": [job_document(job) for job in jobs]}
                )
                return
            parts = parsed.path.strip("/").split("/")
            if len(parts) in {3, 4} and parts[:2] == ["v1", "jobs"]:
                job = store.get(parts[2])
                if job is None:
                    self._error(HTTPStatus.NOT_FOUND, "job not found")
                    return
                if len(parts) == 4 and parts[3] == "events":
                    self._send_json(
                        HTTPStatus.OK,
                        {"job_id": job.id, "events": store.events(job.id)},
                    )
                    return
                if len(parts) == 3:
                    self._send_json(HTTPStatus.OK, job_document(job))
                    return
            self._error(HTTPStatus.NOT_FOUND, "route not found")

        def do_POST(self) -> None:
            parsed = urlparse(self.path)
            if parsed.path == "/v1/jobs":
                body = self._read_json()
                if body is None:
                    return
                payload = body.get("payload")
                if not isinstance(payload, dict):
                    self._error(
                        HTTPStatus.BAD_REQUEST, "payload must be a JSON object"
                    )
                    return
                idempotency_key = self.headers.get("Idempotency-Key")
                if idempotency_key is None:
                    idempotency_key = body.get("idempotency_key")
                try:
                    submission = store.submit(
                        payload,
                        queue=str(body.get("queue", "default")),
                        idempotency_key=idempotency_key,
                        max_attempts=int(body.get("max_attempts", 3)),
                        delay_seconds=float(body.get("delay_seconds", 0)),
                    )
                except (TypeError, ValueError) as error:
                    self._error(HTTPStatus.BAD_REQUEST, str(error))
                    return
                self._send_json(
                    HTTPStatus.CREATED if submission.created else HTTPStatus.OK,
                    {
                        "created": submission.created,
                        "job": job_document(submission.job),
                    },
                )
                return

            parts = parsed.path.strip("/").split("/")
            if len(parts) == 4 and parts[:2] == ["v1", "jobs"]:
                job_id, operation = parts[2], parts[3]
                if operation == "cancel":
                    changed = store.cancel(job_id)
                elif operation == "redrive":
                    changed = store.redrive(job_id)
                else:
                    self._error(HTTPStatus.NOT_FOUND, "route not found")
                    return
                if not changed:
                    verb = "cancelled" if operation == "cancel" else "redriven"
                    self._error(
                        HTTPStatus.CONFLICT,
                        f"job cannot be {verb} from its current state",
                    )
                    return
                job = store.get(job_id)
                self._send_json(HTTPStatus.ACCEPTED, job_document(job))
                return
            self._error(HTTPStatus.NOT_FOUND, "route not found")

        def _read_json(self) -> dict[str, Any] | None:
            try:
                length = int(self.headers.get("Content-Length", "0"))
            except ValueError:
                self._error(HTTPStatus.BAD_REQUEST, "invalid Content-Length")
                return None
            if length <= 0 or length > MAX_BODY_BYTES:
                self._error(
                    HTTPStatus.REQUEST_ENTITY_TOO_LARGE,
                    f"body must be between 1 and {MAX_BODY_BYTES} bytes",
                )
                return None
            try:
                body = json.loads(self.rfile.read(length))
            except (json.JSONDecodeError, UnicodeDecodeError):
                self._error(HTTPStatus.BAD_REQUEST, "invalid JSON body")
                return None
            if not isinstance(body, dict):
                self._error(HTTPStatus.BAD_REQUEST, "body must be a JSON object")
                return None
            return body

        def _error(self, status: HTTPStatus, message: str) -> None:
            self._send_json(status, {"error": message})

        def _send_json(self, status: HTTPStatus, document: Any) -> None:
            body = json.dumps(
                document, sort_keys=True, separators=(",", ":")
            ).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format: str, *args: Any) -> None:
            print(
                json.dumps(
                    {
                        "component": "api",
                        "client": self.client_address[0],
                        "message": format % args,
                    },
                    sort_keys=True,
                )
            )

    return DurableFlowHandler


def serve(store: JobStore, host: str, port: int) -> None:
    server = ThreadingHTTPServer((host, port), handler_for(store))
    print(f"DurableFlow API listening on http://{host}:{port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
