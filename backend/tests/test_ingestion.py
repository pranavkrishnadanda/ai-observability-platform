"""
Tests for the log ingestion API endpoints:
  POST /api/v1/logs/ingest        — single event
  POST /api/v1/logs/ingest/batch  — batch events
"""
import uuid
from unittest.mock import AsyncMock, patch

import pytest

pytestmark = pytest.mark.asyncio

# ── Helpers ───────────────────────────────────────────────────────────────────

_VALID_EVENT = {
    "service_name": "auth-service",
    "severity": "INFO",
    "message": "User logged in successfully",
    "environment": "prod",
}


# ── Single ingest ─────────────────────────────────────────────────────────────

async def test_single_ingest_returns_202_with_event_id(client, api_headers):
    """Happy-path: single event ingest should return 202 and a UUID event_id."""
    response = await client.post(
        "/api/v1/logs/ingest",
        json=_VALID_EVENT,
        headers=api_headers,
    )
    assert response.status_code == 202, response.text
    body = response.json()
    assert "event_id" in body
    # event_id must be a valid UUID
    uuid.UUID(body["event_id"])
    assert body.get("status") == "accepted"


# ── Batch ingest ──────────────────────────────────────────────────────────────

async def test_batch_ingest_100_returns_accepted_100(client, api_headers):
    """Batch of 100 events should all be accepted."""
    events = [_VALID_EVENT.copy() for _ in range(100)]
    response = await client.post(
        "/api/v1/logs/ingest/batch",
        json={"events": events},
        headers=api_headers,
    )
    assert response.status_code == 202, response.text
    body = response.json()
    assert body["accepted"] == 100
    assert body["failed"] == 0


# ── Validation errors ─────────────────────────────────────────────────────────

async def test_invalid_severity_returns_422(client, api_headers):
    """Severity must be one of the Severity enum values."""
    bad_event = {**_VALID_EVENT, "severity": "VERBOSE"}
    response = await client.post(
        "/api/v1/logs/ingest",
        json=bad_event,
        headers=api_headers,
    )
    assert response.status_code == 422


async def test_batch_over_1000_returns_422(client, api_headers):
    """Batch validator should reject more than 1000 events."""
    events = [_VALID_EVENT.copy() for _ in range(1001)]
    response = await client.post(
        "/api/v1/logs/ingest/batch",
        json={"events": events},
        headers=api_headers,
    )
    assert response.status_code == 422


# ── Authentication ────────────────────────────────────────────────────────────

async def test_missing_api_key_returns_4xx(client):
    """
    When `get_current_tenant` is overridden in the client fixture,
    requests still get processed — but FastAPI's Header(...) requires
    X-API-Key or it returns 422. Test that this schema check fires.

    We test this independently by calling with no override in place.
    The client fixture overrides get_current_tenant, so we use a direct
    approach: call the raw ASGI app without the header and check that
    FastAPI returns 422 (missing required header) before the override fires.
    Note: In our test setup, dependency overrides bypass the header check.
    So we verify the schema validator behavior by checking 422 from Pydantic
    when the required header is absent.
    """
    from main import app as _app
    from app.core.auth import get_current_tenant as gct

    # Temporarily remove the override so real Header validation fires
    saved_override = _app.dependency_overrides.pop(gct, None)
    try:
        response = await client.post(
            "/api/v1/logs/ingest",
            json=_VALID_EVENT,
            # No X-API-Key header
        )
        # FastAPI Header(...) dependency raises 422 for missing header
        # or 401 if our override is gone and auth runs
        assert response.status_code in (401, 422)
    finally:
        if saved_override is not None:
            _app.dependency_overrides[gct] = saved_override


async def test_invalid_api_key_returns_401(client, db_session):
    """
    When the real auth logic runs (no override) and the API key is wrong,
    should return 401.
    """
    from main import app as _app
    from app.core.auth import get_current_tenant as gct

    # Remove the tenant override so real auth runs
    saved_override = _app.dependency_overrides.pop(gct, None)
    try:
        with patch("app.core.redis_client.cache_get", new=AsyncMock(return_value=None)):
            response = await client.post(
                "/api/v1/logs/ingest",
                json=_VALID_EVENT,
                headers={"X-API-Key": "aiobs_invalid_key_that_does_not_exist"},
            )
        assert response.status_code == 401
    finally:
        if saved_override is not None:
            _app.dependency_overrides[gct] = saved_override


# ── Rate limiting ─────────────────────────────────────────────────────────────

async def test_rate_limit_exceeded_returns_429(client, api_headers, mock_redis):
    """When Redis INCR returns > rate_limit the endpoint must return 429."""
    # Override incr to simulate hitting the limit
    mock_redis.incr = AsyncMock(return_value=99999)
    response = await client.post(
        "/api/v1/logs/ingest",
        json=_VALID_EVENT,
        headers=api_headers,
    )
    assert response.status_code == 429
    # Restore for subsequent tests in the same fixture scope
    mock_redis.incr = AsyncMock(return_value=1)
