"""
Integration tests — exercise multiple layers together.
"""
import uuid
from unittest.mock import AsyncMock, MagicMock

import pytest

from app.services.alert_engine import generate_dedup_key, is_duplicate
from app.services.anomaly_detector import check_volume_anomaly

pytestmark = pytest.mark.asyncio

_VALID_EVENT = {
    "service_name": "order-service",
    "severity": "INFO",
    "message": "Order placed",
    "environment": "prod",
}


# ── Batch ingest → Kafka calls ────────────────────────────────────────────────

async def test_batch_100_logs_kafka_send_called_100_times(client, api_headers, mock_kafka_producer):
    """
    Posting 100 events via the batch endpoint should result in
    mock_kafka_producer.send being called exactly 100 times.
    """
    events = [_VALID_EVENT.copy() for _ in range(100)]
    response = await client.post(
        "/api/v1/logs/ingest/batch",
        json={"events": events},
        headers=api_headers,
    )
    assert response.status_code == 202, response.text
    assert mock_kafka_producer.send.call_count == 100


# ── Volume spike → correct deviation_pct ─────────────────────────────────────

async def test_volume_spike_4x_baseline_deviation_pct_300():
    """
    With baseline=100 and current=400 (4× baseline), deviation_pct should be
    exactly 300.0 and anomaly_type should be volume_spike.
    """
    TENANT_ID = "cccccccc-cccc-cccc-cccc-cccccccccccc"
    SERVICE = "checkout-service"
    baseline = 100.0
    current = 400  # 4× > 2.5× threshold

    redis = AsyncMock()

    async def fake_get(key):
        if "baseline:volume" in key:
            return str(baseline)
        if ":vol:" in key:
            return str(current)
        return None

    redis.get = fake_get

    pipe = AsyncMock()
    pipe.execute = AsyncMock(return_value=[str(current), None])
    pipe.__aenter__ = AsyncMock(return_value=pipe)
    pipe.__aexit__ = AsyncMock(return_value=False)
    redis.pipeline = MagicMock(return_value=pipe)

    db = MagicMock()
    result = await check_volume_anomaly(TENANT_ID, SERVICE, redis, db)

    assert result is not None
    assert result["anomaly_type"] == "volume_spike"
    assert result["deviation_pct"] == pytest.approx(300.0, rel=1e-3)


# ── Dedup: first call False, second call True ────────────────────────────────

async def test_dedup_first_false_second_true():
    """
    is_duplicate should return False the first time (SADD=1) and
    True the second time (SADD=0) for the same key.
    """
    TENANT_ID = "dddddddd-dddd-dddd-dddd-dddddddddddd"
    dedup_key = generate_dedup_key(TENANT_ID, "reporting-svc", "volume_spike")

    # Simulate a "memory" — track what's been added
    added_keys: set = set()

    async def fake_sadd(set_key, member):
        if member in added_keys:
            return 0  # already exists
        added_keys.add(member)
        return 1  # newly added

    redis = AsyncMock()
    redis.sadd = fake_sadd
    redis.expire = AsyncMock(return_value=True)

    first = await is_duplicate(TENANT_ID, dedup_key, redis)
    assert first is False, "First call should NOT be duplicate"

    second = await is_duplicate(TENANT_ID, dedup_key, redis)
    assert second is True, "Second call with same key SHOULD be duplicate"


# ── Health endpoint ───────────────────────────────────────────────────────────

async def test_health_returns_status_and_components(client):
    """GET /health should return JSON with 'status' and 'components' keys."""
    response = await client.get("/health")
    # Health check may return 200 (healthy) or 503 (degraded) — both are valid
    assert response.status_code in (200, 503)
    body = response.json()
    assert "status" in body, f"Missing 'status' in health response: {body}"
    assert "components" in body, f"Missing 'components' in health response: {body}"
    assert isinstance(body["components"], dict)
