"""
Alert engine: consumes from logs.alerts Kafka topic.
Deduplicates, rate-limits, scores, creates alert records, and delivers webhooks.
"""
import asyncio
import json
import logging
import signal
import time
from datetime import datetime, timezone
from typing import Optional
import uuid

import redis.asyncio as aioredis
from kafka import KafkaConsumer
from sqlalchemy import select, and_
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker

from app.core.config import settings
from app.core.kafka_client import TOPICS
from app.models.alerts import Alert
from app.models.tenants import Tenant
from app.services.webhook_deliverer import deliver_webhook

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

CONSUMER_GROUP = "alert-engines"
_shutdown = False


def handle_shutdown(sig, frame):
    global _shutdown
    _shutdown = True


signal.signal(signal.SIGTERM, handle_shutdown)
signal.signal(signal.SIGINT, handle_shutdown)


def generate_dedup_key(tenant_id: str, service: str, anomaly_type: str) -> str:
    """
    Dedup key = tenant:service:anomaly_type:current_hour.
    One alert per service+anomaly_type per hour.
    """
    current_hour = int(time.time()) // 3600
    return f"{tenant_id}:{service}:{anomaly_type}:{current_hour}"


def score_severity(
    deviation_pct: Optional[float],
    error_rate_increase: Optional[float],
    claude_analysis: Optional[dict],
) -> tuple[int, str]:
    """
    Score 0-100 and severity label.
    deviation_pct × 0.4 + error_rate_increase × 0.3 + claude_severity_score × 0.3
    """
    score = 0.0

    if deviation_pct is not None:
        score += min(100, abs(deviation_pct)) * 0.4

    if error_rate_increase is not None:
        score += min(100, abs(error_rate_increase) * 100) * 0.3  # error_rate as 0-1, scale to 0-100

    claude_score = 50.0
    if claude_analysis and isinstance(claude_analysis, dict):
        severity_map = {"LOW": 25, "MEDIUM": 50, "HIGH": 75, "CRITICAL": 100}
        claude_score = severity_map.get(claude_analysis.get("severity_assessment", ""), 50)
    score += claude_score * 0.3

    total = min(100, score)

    if total >= 75:
        return int(total), "critical"
    elif total >= 50:
        return int(total), "high"
    elif total >= 25:
        return int(total), "medium"
    else:
        return int(total), "low"


async def is_duplicate(
    tenant_id: str, dedup_key: str, redis: aioredis.Redis
) -> bool:
    """Atomically check and set dedup key. Returns True if duplicate."""
    set_key = f"alerts:dedup:{tenant_id}"
    # SADD returns 1 if added (new), 0 if already existed (duplicate)
    added = await redis.sadd(set_key, dedup_key)
    if added:
        # First time — set TTL on the set (1 hour window)
        await redis.expire(set_key, 3600)
    return added == 0  # 0 = already existed = duplicate


async def check_rate_limit(tenant_id: str, redis: aioredis.Redis) -> bool:
    """Returns True if alert is allowed (under rate limit)."""
    rate_key = f"alerts:rate:{tenant_id}"
    count = await redis.incr(rate_key)
    if count == 1:
        await redis.expire(rate_key, 3600)
    return count <= settings.ALERT_RATE_LIMIT_PER_HOUR


async def get_tenant_webhook(
    tenant_id: str, db: AsyncSession
) -> Optional[str]:
    result = await db.execute(
        select(Tenant.webhook_url).where(
            and_(
                Tenant.id == uuid.UUID(tenant_id),
                Tenant.is_active == True,
            )
        )
    )
    return result.scalar_one_or_none()


async def process_alert(
    anomaly_payload: dict,
    session_factory,
    redis: aioredis.Redis,
) -> None:
    tenant_id = anomaly_payload.get("tenant_id", "")
    service = anomaly_payload.get("service_name", "")
    anomaly_type = anomaly_payload.get("anomaly_type", "")
    anomaly_id = anomaly_payload.get("anomaly_id", "")

    # 1. Generate dedup key
    dedup_key = generate_dedup_key(tenant_id, service, anomaly_type)

    # 2. Deduplication check
    if await is_duplicate(tenant_id, dedup_key, redis):
        logger.debug(f"Skipping duplicate alert: {dedup_key}")
        return

    # 3. Rate limit check
    if not await check_rate_limit(tenant_id, redis):
        logger.warning(
            f"Rate limit exceeded for tenant {tenant_id}, skipping alert"
        )
        return

    # 4. Score and classify
    deviation_pct = anomaly_payload.get("deviation_pct")
    claude_analysis_raw = anomaly_payload.get("claude_analysis")
    claude_analysis = None
    if claude_analysis_raw:
        if isinstance(claude_analysis_raw, dict):
            claude_analysis = claude_analysis_raw
        else:
            try:
                claude_analysis = json.loads(claude_analysis_raw)
            except Exception:
                pass

    error_rate_increase = anomaly_payload.get("error_rate_increase")
    score, severity = score_severity(deviation_pct, error_rate_increase, claude_analysis)

    # 5. Get webhook URL
    async with session_factory() as db:
        webhook_url = await get_tenant_webhook(tenant_id, db)

    if not webhook_url:
        logger.info(
            f"No webhook configured for tenant {tenant_id}, skipping alert delivery"
        )
        return

    # Build webhook payload
    root_cause = ""
    recommended_actions = []
    if claude_analysis:
        root_cause = claude_analysis.get("root_cause", "")
        recommended_actions = claude_analysis.get("recommended_actions", [])

    title = f"{anomaly_type.replace('_', ' ').title()} detected in {service}"

    webhook_payload = {
        "alert_id": str(uuid.uuid4()),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "service": service,
        "severity": severity,
        "title": title,
        "description": root_cause,
        "anomaly_id": anomaly_id,
        "metrics": {
            "current_value": anomaly_payload.get("observed_value"),
            "baseline_value": anomaly_payload.get("baseline_value"),
            "deviation_pct": deviation_pct,
        },
        "recommended_actions": recommended_actions,
        "dashboard_url": f"https://app.example.com/anomalies/{anomaly_id}",
    }

    # 6. Create alert record in DB (before delivery attempt)
    alert_id = str(uuid.uuid4())
    async with session_factory() as db:
        alert = Alert(
            id=uuid.UUID(alert_id),
            tenant_id=uuid.UUID(tenant_id),
            anomaly_id=uuid.UUID(anomaly_id) if anomaly_id else uuid.uuid4(),
            alert_type=anomaly_type,
            severity=severity,
            title=title,
            description=root_cause,
            webhook_url=webhook_url,
            delivery_status="pending",
            dedup_key=dedup_key,
            retry_count=0,
        )
        db.add(alert)
        try:
            await db.commit()
        except Exception as e:
            # Likely duplicate dedup_key race — skip
            logger.warning(f"Alert insert failed (possible race): {e}")
            await db.rollback()
            return

    # 7. Deliver webhook
    webhook_payload["alert_id"] = alert_id  # Use the DB alert ID
    success, error_msg = await deliver_webhook(webhook_url, webhook_payload, alert_id)

    # 8. Update delivery status
    async with session_factory() as db:
        result = await db.execute(
            select(Alert).where(Alert.id == uuid.UUID(alert_id))
        )
        db_alert = result.scalar_one_or_none()
        if db_alert:
            if success:
                db_alert.delivery_status = "delivered"
                db_alert.delivered_at = datetime.now(timezone.utc)
            else:
                db_alert.delivery_status = "failed"
                db_alert.last_error = error_msg
                db_alert.retry_count = 4  # 1 initial + 3 retries
            await db.commit()


async def run_alert_engine():
    engine = create_async_engine(
        settings.DATABASE_URL, pool_size=5, max_overflow=10
    )
    session_factory = async_sessionmaker(
        engine, class_=AsyncSession, expire_on_commit=False
    )
    redis = aioredis.from_url(settings.REDIS_URL, decode_responses=True)

    consumer = KafkaConsumer(
        TOPICS["logs.alerts"],
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
        group_id=CONSUMER_GROUP,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=False,
    )

    logger.info(f"Alert engine started, group={CONSUMER_GROUP}")

    try:
        while not _shutdown:
            records = consumer.poll(timeout_ms=500)
            if not records:
                continue

            tasks = []
            for tp, messages in records.items():
                for message in messages:
                    tasks.append(
                        process_alert(message.value, session_factory, redis)
                    )

            if tasks:
                results = await asyncio.gather(*tasks, return_exceptions=True)
                for i, result in enumerate(results):
                    if isinstance(result, Exception):
                        logger.error(f"Alert processing task {i} failed: {result}", exc_info=result)
                consumer.commit()
    finally:
        consumer.close()
        await redis.aclose()
        await engine.dispose()
        logger.info("Alert engine shut down")


if __name__ == "__main__":
    asyncio.run(run_alert_engine())
