"""
Stage 1: Statistical anomaly detection.
Runs every ANOMALY_CHECK_INTERVAL seconds for each tenant×service combination.
Detected anomalies are published to Kafka logs.anomalies for Stage 2 (Claude analysis).
"""
import asyncio
import json
import logging
import re
import time
from datetime import datetime, timezone, timedelta
from typing import Optional

import redis.asyncio as aioredis
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, and_

from app.core.config import settings
from app.core.kafka_client import TOPICS, get_producer
from app.models.anomalies import Anomaly

logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────────────────────────────────────
# Baseline helpers
# ──────────────────────────────────────────────────────────────────────────────

async def get_volume_baseline(tenant_id: str, service_name: str, redis: aioredis.Redis) -> Optional[float]:
    """Read pre-computed rolling 7-day baseline volume from Redis."""
    key = f"tenant:{tenant_id}:service:{service_name}:baseline:volume"
    val = await redis.get(key)
    return float(val) if val else None


async def get_error_rate_baseline(tenant_id: str, service_name: str, redis: aioredis.Redis) -> Optional[float]:
    """Read pre-computed rolling 7-day baseline error rate from Redis."""
    key = f"tenant:{tenant_id}:service:{service_name}:baseline:error_rate"
    val = await redis.get(key)
    return float(val) if val else None


async def get_current_volume(tenant_id: str, service_name: str, redis: aioredis.Redis) -> int:
    """Sum 5-minute volume buckets for the last 5 minutes."""
    epoch = int(time.time()) // 300
    key = f"tenant:{tenant_id}:service:{service_name}:vol:{epoch}"
    val = await redis.get(key)
    return int(val) if val else 0


async def get_current_error_rate(tenant_id: str, service_name: str, redis: aioredis.Redis) -> float:
    """Get error rate in the last 5 minutes."""
    epoch = int(time.time()) // 300
    vol_key = f"tenant:{tenant_id}:service:{service_name}:vol:{epoch}"
    err_key = f"tenant:{tenant_id}:service:{service_name}:errors:{epoch}"
    pipe = redis.pipeline()
    pipe.get(vol_key)
    pipe.get(err_key)
    vol, err = await pipe.execute()
    total = int(vol or 0)
    errors = int(err or 0)
    if total == 0:
        return 0.0
    return errors / total


# ──────────────────────────────────────────────────────────────────────────────
# Error pattern normalization
# ──────────────────────────────────────────────────────────────────────────────

_STRIP = re.compile(
    r'\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b'  # UUID
    r'|\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b'  # IPv4
    r'|\b\d+\b'  # numbers
    r'|"[^"]{16,}"',  # long quoted strings
    re.I
)


def normalize_error_template(message: str) -> str:
    return _STRIP.sub("<VAR>", message).strip()[:256]


async def is_new_error_pattern(
    tenant_id: str, service_name: str, template: str, redis: aioredis.Redis
) -> bool:
    """Returns True if this error template has NOT been seen in the last 24 hours."""
    key = f"tenant:{tenant_id}:service:{service_name}:error_patterns"
    is_member = await redis.sismember(key, template)
    if not is_member:
        pipe = redis.pipeline()
        pipe.sadd(key, template)
        pipe.expire(key, 86400)
        await pipe.execute()
        return True
    return False


# ──────────────────────────────────────────────────────────────────────────────
# Anomaly publishing
# ──────────────────────────────────────────────────────────────────────────────

def _publish_anomaly(anomaly_record: dict) -> None:
    producer = get_producer()
    producer.send(
        TOPICS["logs.anomalies"],
        value=anomaly_record,
        key=anomaly_record["tenant_id"].encode(),
    )


# ──────────────────────────────────────────────────────────────────────────────
# Stage 1: Statistical checks
# ──────────────────────────────────────────────────────────────────────────────

async def check_volume_anomaly(
    tenant_id: str,
    service_name: str,
    redis: aioredis.Redis,
    db: AsyncSession,
) -> Optional[dict]:
    baseline = await get_volume_baseline(tenant_id, service_name, redis)
    if baseline is None or baseline == 0:
        return None  # No baseline yet — skip

    current = await get_current_volume(tenant_id, service_name, redis)

    spike_threshold = baseline * settings.ANOMALY_VOLUME_SPIKE_MULTIPLIER
    drop_threshold = baseline * settings.ANOMALY_VOLUME_DROP_MULTIPLIER

    if current > spike_threshold:
        anomaly_type = "volume_spike"
    elif current < drop_threshold and current > 0:
        anomaly_type = "volume_drop"
    else:
        return None

    deviation_pct = ((current - baseline) / baseline) * 100
    severity_score = min(1.0, abs(deviation_pct) / 500)  # 500% deviation = 1.0 severity

    now = datetime.now(timezone.utc)
    return {
        "tenant_id": tenant_id,
        "service_name": service_name,
        "anomaly_type": anomaly_type,
        "severity_score": round(severity_score, 2),
        "detected_at": now.isoformat(),
        "window_start": (now - timedelta(minutes=5)).isoformat(),
        "window_end": now.isoformat(),
        "baseline_value": baseline,
        "observed_value": current,
        "deviation_pct": round(deviation_pct, 2),
    }


async def check_error_rate_anomaly(
    tenant_id: str,
    service_name: str,
    redis: aioredis.Redis,
) -> Optional[dict]:
    baseline = await get_error_rate_baseline(tenant_id, service_name, redis)
    if baseline is None:
        return None

    current = await get_current_error_rate(tenant_id, service_name, redis)

    if baseline == 0 and current > 0:
        # Any errors when baseline is zero is anomalous
        deviation_pct = 100.0
    elif baseline > 0 and current > baseline * settings.ANOMALY_ERROR_RATE_MULTIPLIER:
        deviation_pct = ((current - baseline) / baseline) * 100
    else:
        return None

    severity_score = min(1.0, current)  # error rate itself is a good severity proxy
    now = datetime.now(timezone.utc)
    return {
        "tenant_id": tenant_id,
        "service_name": service_name,
        "anomaly_type": "error_rate_spike",
        "severity_score": round(severity_score, 2),
        "detected_at": now.isoformat(),
        "window_start": (now - timedelta(minutes=5)).isoformat(),
        "window_end": now.isoformat(),
        "baseline_value": baseline,
        "observed_value": current,
        "deviation_pct": round(deviation_pct, 2),
    }


async def check_new_error_patterns(
    tenant_id: str,
    service_name: str,
    redis: aioredis.Redis,
    db: AsyncSession,
) -> list[dict]:
    """Check Redis hot-path logs for new ERROR/CRITICAL templates."""
    hot_key = f"tenant:{tenant_id}:service:{service_name}:logs"
    raw_logs = await redis.lrange(hot_key, 0, 49)  # last 50 logs

    new_anomalies = []
    for raw in raw_logs:
        try:
            log = json.loads(raw)
        except Exception:
            continue
        if log.get("severity") not in ("ERROR", "CRITICAL"):
            continue
        template = normalize_error_template(log.get("message", ""))
        if not template:
            continue
        if await is_new_error_pattern(tenant_id, service_name, template, redis):
            now = datetime.now(timezone.utc)
            new_anomalies.append({
                "tenant_id": tenant_id,
                "service_name": service_name,
                "anomaly_type": "new_error_pattern",
                "severity_score": 0.6,  # Medium-high — new patterns are significant
                "detected_at": now.isoformat(),
                "window_start": (now - timedelta(hours=1)).isoformat(),
                "window_end": now.isoformat(),
                "baseline_value": None,
                "observed_value": None,
                "deviation_pct": None,
                "error_template": template,
            })
    return new_anomalies


async def run_detection_for_tenant_service(
    tenant_id: str,
    service_name: str,
    redis: aioredis.Redis,
    db: AsyncSession,
) -> None:
    """Run all three statistical checks for one tenant×service pair."""
    anomalies = []

    vol = await check_volume_anomaly(tenant_id, service_name, redis, db)
    if vol:
        anomalies.append(vol)

    err = await check_error_rate_anomaly(tenant_id, service_name, redis)
    if err:
        anomalies.append(err)

    patterns = await check_new_error_patterns(tenant_id, service_name, redis, db)
    anomalies.extend(patterns)

    # Save to DB and publish to Kafka
    for anomaly_data in anomalies:
        try:
            db_anomaly = Anomaly(
                tenant_id=anomaly_data["tenant_id"],
                service_name=anomaly_data["service_name"],
                anomaly_type=anomaly_data["anomaly_type"],
                severity_score=anomaly_data["severity_score"],
                window_start=datetime.fromisoformat(anomaly_data["window_start"]),
                window_end=datetime.fromisoformat(anomaly_data["window_end"]),
                baseline_value=anomaly_data.get("baseline_value"),
                observed_value=anomaly_data.get("observed_value"),
                deviation_pct=anomaly_data.get("deviation_pct"),
                status="active",
            )
            db.add(db_anomaly)
            await db.commit()
            await db.refresh(db_anomaly)

            # Enrich with DB ID before publishing
            anomaly_data["anomaly_id"] = str(db_anomaly.id)
            _publish_anomaly(anomaly_data)
            logger.info(f"Anomaly detected: {anomaly_data['anomaly_type']} for {service_name}")
        except Exception as e:
            logger.error(f"Failed to save/publish anomaly: {e}", exc_info=True)
            await db.rollback()


async def detect_anomalies_for_all_tenants() -> None:
    """Scan all tenant×service pairs in Redis and run statistical detection."""
    from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker

    engine = create_async_engine(settings.DATABASE_URL, pool_size=5, max_overflow=10)
    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    redis = aioredis.from_url(settings.REDIS_URL, decode_responses=True)

    try:
        all_keys = await redis.keys("tenant:*:services")
        tenant_ids: set[str] = set()
        for key in all_keys:
            parts = key.split(":")
            if len(parts) == 3 and parts[2] == "services":
                tenant_ids.add(parts[1])

        async with session_factory() as db:
            for tenant_id in tenant_ids:
                services_key = f"tenant:{tenant_id}:services"
                services = await redis.smembers(services_key)
                for service_name in services:
                    try:
                        await run_detection_for_tenant_service(tenant_id, service_name, redis, db)
                    except Exception as e:
                        logger.error(f"Detection failed for {tenant_id}/{service_name}: {e}")
    finally:
        await redis.aclose()
        await engine.dispose()


async def run_anomaly_detection_loop():
    """Runs Stage 1 statistical detection every 30 seconds."""
    logger.info("Anomaly detection loop starting")
    while True:
        try:
            await detect_anomalies_for_all_tenants()
            logger.info("Anomaly detection cycle complete")
        except asyncio.CancelledError:
            logger.info("Anomaly detection loop cancelled")
            break
        except Exception as e:
            logger.error(f"Anomaly detection error (continuing): {e}")
        await asyncio.sleep(30)
