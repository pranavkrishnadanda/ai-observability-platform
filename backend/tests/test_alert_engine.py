"""
Tests for the alert engine:
  - Deduplication (is_duplicate)
  - Rate limiting (check_rate_limit)
  - Webhook delivery with HMAC signing
  - Retry logic in webhook deliverer
"""
import asyncio
import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.services.alert_engine import (
    check_rate_limit,
    is_duplicate,
    generate_dedup_key,
    score_severity,
)
from app.services.webhook_deliverer import deliver_webhook, sign_payload

pytestmark = pytest.mark.asyncio

TENANT_ID = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
DEDUP_KEY = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:payment-service:volume_spike:12345"


# ── Deduplication ─────────────────────────────────────────────────────────────

async def test_new_alert_is_not_duplicate():
    """SADD returns 1 (added) → is_duplicate must return False."""
    redis = AsyncMock()
    redis.sadd = AsyncMock(return_value=1)
    redis.expire = AsyncMock(return_value=True)

    result = await is_duplicate(TENANT_ID, DEDUP_KEY, redis)
    assert result is False


async def test_existing_alert_is_duplicate():
    """SADD returns 0 (already existed) → is_duplicate must return True."""
    redis = AsyncMock()
    redis.sadd = AsyncMock(return_value=0)
    redis.expire = AsyncMock(return_value=True)

    result = await is_duplicate(TENANT_ID, DEDUP_KEY, redis)
    assert result is True


# ── Rate limiting ─────────────────────────────────────────────────────────────

async def test_within_rate_limit_allowed():
    """incr returns 5 (≤ 10 default limit) → check_rate_limit returns True."""
    redis = AsyncMock()
    redis.incr = AsyncMock(return_value=5)
    redis.expire = AsyncMock(return_value=True)

    result = await check_rate_limit(TENANT_ID, redis)
    assert result is True


async def test_over_rate_limit_blocked():
    """incr returns 11 (> 10 default limit) → check_rate_limit returns False."""
    redis = AsyncMock()
    redis.incr = AsyncMock(return_value=11)
    redis.expire = AsyncMock(return_value=True)

    result = await check_rate_limit(TENANT_ID, redis)
    assert result is False


# ── Score severity ────────────────────────────────────────────────────────────

def test_score_severity_critical():
    """High deviation + high Claude score → critical label."""
    score, label = score_severity(
        deviation_pct=200.0,
        error_rate_increase=0.5,
        claude_analysis={"severity_assessment": "CRITICAL"},
    )
    assert label == "critical"
    assert score >= 75


def test_score_severity_low():
    """No deviation and LOW Claude score → low label."""
    score, label = score_severity(
        deviation_pct=5.0,
        error_rate_increase=0.001,
        claude_analysis={"severity_assessment": "LOW"},
    )
    assert label in ("low", "medium")


# ── Webhook delivery ──────────────────────────────────────────────────────────

async def test_webhook_delivery_success():
    """Successful HTTP 200 response → (True, None)."""
    import httpx

    payload = {"alert_id": "test-123", "severity": "high"}
    webhook_url = "https://hooks.example.com/alert"
    alert_id = "alert-uuid-001"

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.raise_for_status = MagicMock()  # no-op

    mock_client = AsyncMock()
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)
    mock_client.post = AsyncMock(return_value=mock_response)

    with patch("httpx.AsyncClient", return_value=mock_client):
        success, error = await deliver_webhook(webhook_url, payload, alert_id)

    assert success is True
    assert error is None


async def test_webhook_retry_on_failure_4_total_attempts():
    """
    Persistent HTTP errors should result in 4 total attempts
    (1 initial + 3 retries from RETRY_DELAYS) and return (False, error_msg).
    """
    import httpx
    from app.services.webhook_deliverer import RETRY_DELAYS

    call_count = 0
    payload = {"alert_id": "retry-test"}
    webhook_url = "https://hooks.example.com/fail"
    alert_id = "alert-retry-001"

    async def failing_post(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        mock_resp = MagicMock()
        mock_resp.status_code = 500
        mock_resp.text = "Internal Server Error"
        raise httpx.HTTPStatusError(
            "500 error",
            request=MagicMock(),
            response=mock_resp,
        )

    mock_client = AsyncMock()
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)
    mock_client.post = failing_post

    with (
        patch("httpx.AsyncClient", return_value=mock_client),
        patch("asyncio.sleep", new=AsyncMock()),  # skip real delays
    ):
        success, error_msg = await deliver_webhook(webhook_url, payload, alert_id)

    expected_attempts = len(RETRY_DELAYS) + 1  # 1 + 3 = 4
    assert call_count == expected_attempts
    assert success is False
    assert error_msg is not None
    assert "500" in error_msg


async def test_hmac_signature_present_in_headers():
    """Webhook POST must include X-Webhook-Signature header with sha256= prefix."""
    import httpx

    captured_headers = {}
    payload = {"alert_id": "hmac-test"}
    webhook_url = "https://hooks.example.com/verify"
    alert_id = "alert-hmac-001"

    async def capture_post(*args, **kwargs):
        captured_headers.update(kwargs.get("headers", {}))
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.raise_for_status = MagicMock()
        return mock_resp

    mock_client = AsyncMock()
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)
    mock_client.post = capture_post

    with patch("httpx.AsyncClient", return_value=mock_client):
        await deliver_webhook(webhook_url, payload, alert_id, secret="test-secret")

    assert "X-Webhook-Signature" in captured_headers
    sig = captured_headers["X-Webhook-Signature"]
    assert sig.startswith("sha256=")
    # Verify the signature matches
    payload_bytes = json.dumps(payload, default=str).encode("utf-8")
    expected_sig = f"sha256={sign_payload(payload_bytes, 'test-secret')}"
    assert sig == expected_sig


# ── generate_dedup_key ────────────────────────────────────────────────────────

def test_generate_dedup_key_format():
    """Dedup key should be tenant:service:type:hour."""
    import time
    key = generate_dedup_key("tenant-1", "svc", "volume_spike")
    parts = key.split(":")
    assert parts[0] == "tenant-1"
    assert parts[1] == "svc"
    assert parts[2] == "volume_spike"
    # Hour part must be an integer
    assert parts[3].isdigit()
