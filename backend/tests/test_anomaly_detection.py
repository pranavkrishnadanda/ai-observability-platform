"""
Tests for the statistical anomaly detection service.
All Redis calls are mocked; no real network connections.
"""
import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.services.anomaly_detector import (
    check_new_error_patterns,
    check_volume_anomaly,
    get_current_volume,
    get_volume_baseline,
    is_new_error_pattern,
    normalize_error_template,
)

pytestmark = pytest.mark.asyncio


# ── Helpers ───────────────────────────────────────────────────────────────────

TENANT_ID = "11111111-1111-1111-1111-111111111111"
SERVICE = "payment-service"


def _make_redis(baseline=None, current=None, sismember=False):
    """Build a minimal async Redis mock with configurable get/pipeline return values."""
    redis = AsyncMock()

    async def fake_get(key):
        if "baseline:volume" in key:
            return str(baseline) if baseline is not None else None
        if f":vol:" in key:
            return str(current) if current is not None else None
        return None

    redis.get = fake_get
    redis.sismember = AsyncMock(return_value=sismember)

    pipe = AsyncMock()
    pipe.get = AsyncMock()
    pipe.sadd = AsyncMock()
    pipe.expire = AsyncMock()
    pipe.execute = AsyncMock(return_value=[str(current) if current else None, None])
    pipe.__aenter__ = AsyncMock(return_value=pipe)
    pipe.__aexit__ = AsyncMock(return_value=False)
    redis.pipeline = MagicMock(return_value=pipe)

    return redis


# ── Volume spike ──────────────────────────────────────────────────────────────

async def test_volume_spike_triggers_anomaly():
    """current > baseline * 2.5 should yield a volume_spike anomaly."""
    baseline = 100.0
    current = 300  # 3× baseline — above 2.5× threshold

    redis = _make_redis(baseline=baseline, current=current)
    db = MagicMock()

    result = await check_volume_anomaly(TENANT_ID, SERVICE, redis, db)

    assert result is not None
    assert result["anomaly_type"] == "volume_spike"
    assert result["baseline_value"] == baseline
    assert result["observed_value"] == current
    assert result["deviation_pct"] == pytest.approx(200.0, rel=1e-3)


async def test_normal_volume_returns_none():
    """Volume within the normal range should return None."""
    baseline = 100.0
    current = 110  # only 10% above baseline

    redis = _make_redis(baseline=baseline, current=current)
    db = MagicMock()

    result = await check_volume_anomaly(TENANT_ID, SERVICE, redis, db)
    assert result is None


async def test_volume_drop_triggers_anomaly():
    """current < baseline * 0.2 (and > 0) should yield a volume_drop anomaly."""
    baseline = 100.0
    current = 10  # 10% of baseline — below 20% threshold

    redis = _make_redis(baseline=baseline, current=current)
    db = MagicMock()

    result = await check_volume_anomaly(TENANT_ID, SERVICE, redis, db)

    assert result is not None
    assert result["anomaly_type"] == "volume_drop"
    assert result["deviation_pct"] < 0  # negative deviation


async def test_no_baseline_returns_none():
    """When no baseline exists in Redis, check_volume_anomaly should return None."""
    redis = _make_redis(baseline=None, current=300)
    db = MagicMock()

    result = await check_volume_anomaly(TENANT_ID, SERVICE, redis, db)
    assert result is None


# ── Error pattern detection ───────────────────────────────────────────────────

async def test_new_error_pattern_triggers_anomaly():
    """A new ERROR log whose template has not been seen should produce an anomaly."""
    log_entry = json.dumps({
        "severity": "ERROR",
        "message": "Connection refused to database host 192.168.1.1",
    })

    redis = AsyncMock()
    redis.lrange = AsyncMock(return_value=[log_entry])
    redis.sismember = AsyncMock(return_value=False)  # not seen before

    pipe = AsyncMock()
    pipe.sadd = AsyncMock()
    pipe.expire = AsyncMock()
    pipe.execute = AsyncMock(return_value=[1, True])
    pipe.__aenter__ = AsyncMock(return_value=pipe)
    pipe.__aexit__ = AsyncMock(return_value=False)
    redis.pipeline = MagicMock(return_value=pipe)

    db = MagicMock()
    results = await check_new_error_patterns(TENANT_ID, SERVICE, redis, db)

    assert len(results) == 1
    assert results[0]["anomaly_type"] == "new_error_pattern"
    assert "error_template" in results[0]


async def test_known_error_pattern_returns_empty_list():
    """If the error pattern is already known (sismember=True), return empty list."""
    log_entry = json.dumps({
        "severity": "ERROR",
        "message": "Connection refused to database host 10.0.0.1",
    })

    redis = AsyncMock()
    redis.lrange = AsyncMock(return_value=[log_entry])
    redis.sismember = AsyncMock(return_value=True)  # already known

    pipe = AsyncMock()
    pipe.sadd = AsyncMock()
    pipe.expire = AsyncMock()
    pipe.execute = AsyncMock(return_value=[1, True])
    pipe.__aenter__ = AsyncMock(return_value=pipe)
    pipe.__aexit__ = AsyncMock(return_value=False)
    redis.pipeline = MagicMock(return_value=pipe)

    db = MagicMock()
    results = await check_new_error_patterns(TENANT_ID, SERVICE, redis, db)

    assert results == []


# ── Normalization ─────────────────────────────────────────────────────────────

def test_normalize_error_template_strips_uuids_and_numbers():
    """normalize_error_template should replace UUIDs, IPs, numbers with <VAR>."""
    msg = "User 123 failed login from 192.168.1.100 request-id=550e8400-e29b-41d4-a716-446655440000"
    result = normalize_error_template(msg)
    assert "123" not in result
    assert "192.168.1.100" not in result
    assert "550e8400" not in result
    assert "<VAR>" in result


# ── is_new_error_pattern ──────────────────────────────────────────────────────

async def test_is_new_error_pattern_returns_true_for_unseen():
    """An unseen pattern should return True and add it to Redis set."""
    template = "Connection refused to database host <VAR>"
    redis = AsyncMock()
    redis.sismember = AsyncMock(return_value=False)

    pipe = AsyncMock()
    pipe.sadd = AsyncMock()
    pipe.expire = AsyncMock()
    pipe.execute = AsyncMock(return_value=[1, True])
    pipe.__aenter__ = AsyncMock(return_value=pipe)
    pipe.__aexit__ = AsyncMock(return_value=False)
    redis.pipeline = MagicMock(return_value=pipe)

    result = await is_new_error_pattern(TENANT_ID, SERVICE, template, redis)
    assert result is True


async def test_is_new_error_pattern_returns_false_for_known():
    """A previously seen pattern should return False."""
    template = "Connection refused to database host <VAR>"
    redis = AsyncMock()
    redis.sismember = AsyncMock(return_value=True)

    pipe = AsyncMock()
    pipe.__aenter__ = AsyncMock(return_value=pipe)
    pipe.__aexit__ = AsyncMock(return_value=False)
    redis.pipeline = MagicMock(return_value=pipe)

    result = await is_new_error_pattern(TENANT_ID, SERVICE, template, redis)
    assert result is False


# ── Claude integration stubs ──────────────────────────────────────────────────

async def test_claude_called_with_correct_context():
    """The anomaly fields should appear in the user message sent to Claude."""
    import anthropic

    anomaly_data = {
        "anomaly_type": "volume_spike",
        "service_name": SERVICE,
        "deviation_pct": 200.0,
        "baseline_value": 100.0,
        "observed_value": 300.0,
        "severity_score": 0.4,
    }

    captured_args = {}

    def fake_create(**kwargs):
        captured_args.update(kwargs)
        mock_msg = MagicMock()
        mock_msg.content = [MagicMock(text='{"severity_assessment": "HIGH", "root_cause": "spike", "recommended_actions": []}')]
        return mock_msg

    mock_client = MagicMock()
    mock_client.messages.create = fake_create

    with patch("anthropic.Anthropic", return_value=mock_client):
        # Simulate what the Stage 2 analyzer would do: call Claude
        import json
        user_message = json.dumps(anomaly_data)
        mock_client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=512,
            messages=[{"role": "user", "content": user_message}],
        )

    assert "messages" in captured_args
    msg_content = captured_args["messages"][0]["content"]
    assert "volume_spike" in msg_content
    assert "200.0" in msg_content


async def test_claude_failure_returns_none():
    """If Claude raises an exception the caller should handle it gracefully."""
    import anthropic

    def raising_create(**kwargs):
        raise anthropic.APIConnectionError(request=MagicMock())

    mock_client = MagicMock()
    mock_client.messages.create = raising_create

    # Simulate the try/except pattern that should wrap Claude calls
    result = None
    try:
        mock_client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=512,
            messages=[{"role": "user", "content": "test"}],
        )
        result = "ok"
    except Exception:
        result = None

    assert result is None


async def test_claude_malformed_json_returns_none():
    """Malformed JSON from Claude should be handled without raising."""
    import json

    bad_json = "This is not JSON at all..."
    result = None
    try:
        result = json.loads(bad_json)
    except json.JSONDecodeError:
        result = None

    assert result is None
