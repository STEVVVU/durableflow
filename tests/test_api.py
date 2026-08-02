from __future__ import annotations

import json
import tempfile
import threading
import unittest
import urllib.request
from http.server import ThreadingHTTPServer
from pathlib import Path

from durableflow.api import handler_for
from durableflow.store import JobStore


class ApiTestCase(unittest.TestCase):
    def test_submit_is_idempotent_and_job_is_observable(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = JobStore(Path(directory) / "api.db")
            server = ThreadingHTTPServer(("127.0.0.1", 0), handler_for(store))
            thread = threading.Thread(target=server.serve_forever)
            thread.start()
            base_url = f"http://127.0.0.1:{server.server_port}"
            try:
                body = json.dumps(
                    {"payload": {"task": "echo", "value": "hello"}}
                ).encode()
                first = urllib.request.Request(
                    f"{base_url}/v1/jobs",
                    data=body,
                    headers={
                        "Content-Type": "application/json",
                        "Idempotency-Key": "api-request-1",
                    },
                    method="POST",
                )
                with urllib.request.urlopen(first) as response:
                    first_document = json.load(response)
                    self.assertEqual(201, response.status)

                duplicate = urllib.request.Request(
                    f"{base_url}/v1/jobs",
                    data=body,
                    headers={
                        "Content-Type": "application/json",
                        "Idempotency-Key": "api-request-1",
                    },
                    method="POST",
                )
                with urllib.request.urlopen(duplicate) as response:
                    duplicate_document = json.load(response)
                    self.assertEqual(200, response.status)

                self.assertTrue(first_document["created"])
                self.assertFalse(duplicate_document["created"])
                self.assertEqual(
                    first_document["job"]["id"],
                    duplicate_document["job"]["id"],
                )
                job_id = first_document["job"]["id"]
                with urllib.request.urlopen(
                    f"{base_url}/v1/jobs/{job_id}/events"
                ) as response:
                    events = json.load(response)["events"]
                self.assertEqual(["submitted"], [event["event_type"] for event in events])

                with urllib.request.urlopen(f"{base_url}/metrics") as response:
                    metrics = response.read().decode()
                self.assertIn(
                    'durableflow_jobs{status="queued"} 1', metrics
                )
            finally:
                server.shutdown()
                server.server_close()
                thread.join()
                store.close()


if __name__ == "__main__":
    unittest.main()
