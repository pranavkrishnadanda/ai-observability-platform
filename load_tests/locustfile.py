"""
Load tests for the AI Observability Platform.

Scenarios:
  LogIngestionUser   — mixed workload (ingest + query)
  AnomalySimulator   — triggers anomaly detection via burst errors

Run with: locust -f locustfile.py --headless -u 50 -r 10 --run-time 60s --host http://localhost:8000
"""
import json
import random
import string
import time
import uuid
from locust import HttpUser, task, between, constant_pacing

# ── Realistic test data ────────────────────────────────────────────────────────

SERVICES = [
    "api-gateway",
    "auth-service",
    "payment-service",
    "notification-service",
    "user-service",
]

SEVERITIES = ["DEBUG", "INFO", "INFO", "INFO", "WARNING", "ERROR", "CRITICAL"]

MESSAGES = {
    "DEBUG":    ["Cache hit for key user:{uid}", "DB query took {ms}ms", "Span {span} started"],
    "INFO":     ["Request processed in {ms}ms", "User {uid} authenticated", "Payment {pid} initiated"],
    "WARNING":  ["Response time {ms}ms exceeds SLA", "Retry attempt {n} for request {rid}"],
    "ERROR":    ["Connection refused to postgres:5432", "JWT token validation failed for user {uid}",
                 "Timeout calling {svc} after {ms}ms", "Database query failed: deadlock detected",
                 "Payment declined for order {pid}: insufficient funds"],
    "CRITICAL": ["OOM killed — heap exhausted", "Database connection pool exhausted (max=20)",
                 "Circuit breaker OPEN for {svc}", "Disk full on /data partition"],
}

STACK_TRACE = """Traceback (most recent call last):
  File "/app/services/{svc}.py", line {line}, in handle_request
    result = await db.execute(query)
  File "/app/core/database.py", line 87, in execute
    raise ConnectionError("Connection pool exhausted")
ConnectionError: Connection pool exhausted after 30s timeout"""


def make_log_event(service: str = None, severity: str = None, error_burst: bool = False):
    svc = service or random.choice(SERVICES)
    if error_burst:
        sev = random.choice(["ERROR", "CRITICAL"])
    else:
        sev = severity or random.choice(SEVERITIES)

    templates = MESSAGES[sev]
    msg = random.choice(templates).format(
        uid=str(uuid.uuid4())[:8],
        ms=random.randint(10, 5000),
        n=random.randint(1, 5),
        rid=str(uuid.uuid4())[:8],
        pid=str(uuid.uuid4())[:8],
        svc=random.choice(SERVICES),
        span=str(uuid.uuid4())[:8],
        line=random.randint(50, 300),
    )

    if sev in ("ERROR", "CRITICAL") and random.random() < 0.4:
        msg += "\n" + STACK_TRACE.format(svc=svc, line=random.randint(50, 300))

    return {
        "service_name": svc,
        "severity": sev,
        "message": msg,
        "metadata": {
            "request_id": str(uuid.uuid4()),
            "user_id": str(uuid.uuid4())[:8],
            "version": "1.2.3",
        },
        "trace_id": str(uuid.uuid4()).replace("-", ""),
        "span_id": str(uuid.uuid4())[:16].replace("-", ""),
        "environment": random.choice(["prod", "staging"]),
    }


# ── User classes ───────────────────────────────────────────────────────────────

class LogIngestionUser(HttpUser):
    """
    Mixed workload: ingest (bulk) + query.
    Task weights: 10x single ingest, 3x batch ingest, 2x query logs,
                  2x analytics overview, 1x anomalies list, 1x health check.
    """
    wait_time = between(0.01, 0.05)  # 20-100 req/sec per user

    # Set this via environment or hardcode for local testing
    api_key = "aiobs_test_key_replace_me"

    def on_start(self):
        self.headers = {"X-API-Key": self.api_key}

    @task(10)
    def ingest_single(self):
        event = make_log_event()
        with self.client.post(
            "/api/v1/logs/ingest",
            json=event,
            headers=self.headers,
            catch_response=True,
            name="POST /logs/ingest (single)",
        ) as resp:
            if resp.status_code == 202:
                resp.success()
            elif resp.status_code == 429:
                resp.success()  # Rate limited — expected under load
            else:
                resp.failure(f"Unexpected {resp.status_code}: {resp.text[:100]}")

    @task(3)
    def ingest_batch(self):
        events = [make_log_event() for _ in range(100)]
        with self.client.post(
            "/api/v1/logs/ingest/batch",
            json={"events": events},
            headers=self.headers,
            catch_response=True,
            name="POST /logs/ingest/batch (100)",
        ) as resp:
            if resp.status_code == 202:
                resp.success()
            elif resp.status_code == 429:
                resp.success()
            else:
                resp.failure(f"Unexpected {resp.status_code}: {resp.text[:100]}")

    @task(2)
    def query_recent_logs(self):
        service = random.choice(SERVICES)
        with self.client.get(
            f"/api/v1/logs?service={service}&limit=50",
            headers=self.headers,
            catch_response=True,
            name="GET /logs (filter by service)",
        ) as resp:
            if resp.status_code in (200, 401):
                resp.success()
            else:
                resp.failure(f"Unexpected {resp.status_code}")

    @task(2)
    def analytics_overview(self):
        with self.client.get(
            "/api/v1/analytics/overview",
            headers=self.headers,
            catch_response=True,
            name="GET /analytics/overview",
        ) as resp:
            if resp.status_code in (200, 401):
                resp.success()
            else:
                resp.failure(f"Unexpected {resp.status_code}")

    @task(1)
    def list_anomalies(self):
        with self.client.get(
            "/api/v1/anomalies",
            headers=self.headers,
            catch_response=True,
            name="GET /anomalies",
        ) as resp:
            if resp.status_code in (200, 401):
                resp.success()
            else:
                resp.failure(f"Unexpected {resp.status_code}")

    @task(1)
    def health_check(self):
        with self.client.get(
            "/health",
            catch_response=True,
            name="GET /health",
        ) as resp:
            if resp.status_code in (200, 503):
                resp.success()
            else:
                resp.failure(f"Unexpected {resp.status_code}")


class AnomalySimulator(HttpUser):
    """
    Sends bursts of error logs to trigger anomaly detection.
    Waits 5-10 seconds between bursts to simulate incident patterns.
    """
    wait_time = between(5, 10)
    api_key = "aiobs_test_key_replace_me"

    def on_start(self):
        self.headers = {"X-API-Key": self.api_key}

    @task(1)
    def burst_error_logs(self):
        """Send 500 error logs in one batch to trigger volume/error rate anomaly."""
        events = [make_log_event(service="payment-service", error_burst=True) for _ in range(500)]
        with self.client.post(
            "/api/v1/logs/ingest/batch",
            json={"events": events},
            headers=self.headers,
            catch_response=True,
            name="POST /logs/ingest/batch (anomaly burst 500)",
        ) as resp:
            if resp.status_code in (202, 429):
                resp.success()
            else:
                resp.failure(f"Unexpected {resp.status_code}: {resp.text[:100]}")
