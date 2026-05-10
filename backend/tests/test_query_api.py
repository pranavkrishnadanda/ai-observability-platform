"""
Tests for the query API endpoints:
  GET /api/v1/logs
  GET /api/v1/logs/{log_id}
  GET /api/v1/analytics/overview
  GET /api/v1/analytics/services/{name}/timeline
"""
import uuid
from datetime import datetime, timezone
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import insert

from app.models.logs import Log

pytestmark = pytest.mark.asyncio


# ── Helpers ───────────────────────────────────────────────────────────────────

async def _insert_log(db_session, tenant_id: str, service: str = "api-gateway",
                      severity: str = "ERROR") -> Log:
    """Insert a Log row and return it."""
    log = Log(
        id=uuid.uuid4(),
        tenant_id=uuid.UUID(tenant_id),
        service_name=service,
        severity=severity,
        message="Test log message for query tests",
        log_metadata={},
        environment="prod",
        created_at=datetime.now(timezone.utc),
        ingested_at=datetime.now(timezone.utc),
    )
    db_session.add(log)
    await db_session.commit()
    await db_session.refresh(log)
    return log


# ── GET /logs ─────────────────────────────────────────────────────────────────

async def test_get_logs_with_filters_returns_200(client, api_headers, db_session, test_tenant):
    """Service + severity filters should return 200 with data and total."""
    tenant, _ = test_tenant
    tenant_id = str(tenant.id)

    # Insert a matching log
    await _insert_log(db_session, tenant_id, service="payment-svc", severity="ERROR")

    response = await client.get(
        "/api/v1/logs",
        params={"service": "payment-svc", "severity": "ERROR"},
        headers=api_headers,
    )
    assert response.status_code == 200, response.text
    body = response.json()
    assert "data" in body
    assert "total" in body
    assert body["total"] >= 1
    assert len(body["data"]) >= 1


async def test_get_logs_no_match_returns_empty(client, api_headers):
    """Filtering for a non-existent service should return empty list with total=0."""
    response = await client.get(
        "/api/v1/logs",
        params={"service": "nonexistent-service-xyz"},
        headers=api_headers,
    )
    assert response.status_code == 200
    body = response.json()
    assert body["total"] == 0
    assert body["data"] == []


# ── GET /logs/{log_id} ────────────────────────────────────────────────────────

async def test_get_log_nonexistent_uuid_returns_404(client, api_headers):
    """A valid UUID that doesn't exist in the DB should return 404."""
    nonexistent_id = str(uuid.uuid4())
    response = await client.get(
        f"/api/v1/logs/{nonexistent_id}",
        headers=api_headers,
    )
    assert response.status_code == 404


async def test_get_log_invalid_uuid_returns_422(client, api_headers):
    """A path segment that is not a valid UUID should return 422."""
    response = await client.get(
        "/api/v1/logs/not-a-uuid",
        headers=api_headers,
    )
    assert response.status_code == 422


async def test_get_log_existing_returns_200(client, api_headers, db_session, test_tenant):
    """A real log ID should return 200 with log data."""
    tenant, _ = test_tenant
    tenant_id = str(tenant.id)
    log = await _insert_log(db_session, tenant_id)

    response = await client.get(
        f"/api/v1/logs/{str(log.id)}",
        headers=api_headers,
    )
    assert response.status_code == 200
    body = response.json()
    assert body["id"] == str(log.id)


# ── GET /analytics/overview ───────────────────────────────────────────────────

async def test_analytics_overview_returns_required_fields(client, api_headers, mock_redis):
    """Overview endpoint must return all required top-level fields."""
    # Ensure cache miss so the endpoint runs DB queries
    mock_redis.get = AsyncMock(return_value=None)

    response = await client.get(
        "/api/v1/analytics/overview",
        headers=api_headers,
    )
    assert response.status_code == 200, response.text
    body = response.json()

    all_keys = set(body.keys())
    assert "total_logs_today" in all_keys, f"Missing total_logs_today in {all_keys}"
    assert "total_logs_week" in all_keys, f"Missing total_logs_week in {all_keys}"
    assert "error_rate_today" in all_keys, f"Missing error_rate_today in {all_keys}"
    assert "active_anomalies" in all_keys, f"Missing active_anomalies in {all_keys}"
    assert "system_health_score" in all_keys, f"Missing system_health_score in {all_keys}"
    assert "top_5_error_services" in all_keys, f"Missing top_5_error_services in {all_keys}"
    # alerts_sent or alerts_sent_today
    assert ("alerts_sent" in all_keys or "alerts_sent_today" in all_keys), \
        f"Missing alerts_sent field in {all_keys}"


# ── GET /analytics/services/{name}/timeline ───────────────────────────────────

async def test_service_timeline_returns_24_hourly_entries(client, api_headers, mock_redis):
    """Timeline endpoint should return exactly 24 hourly buckets."""
    # Ensure cache miss
    mock_redis.get = AsyncMock(return_value=None)

    response = await client.get(
        "/api/v1/analytics/services/my-service/timeline",
        headers=api_headers,
    )
    assert response.status_code == 200, response.text
    body = response.json()

    assert "timeline" in body
    timeline = body["timeline"]
    assert len(timeline) == 24, f"Expected 24 entries, got {len(timeline)}"

    # Each entry must have the required fields
    for entry in timeline:
        assert "hour" in entry, f"Missing 'hour' in entry: {entry}"
        assert "total_logs" in entry, f"Missing 'total_logs' in entry: {entry}"
        assert "errors" in entry, f"Missing 'errors' in entry: {entry}"
