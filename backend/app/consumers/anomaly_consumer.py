"""
Anomaly consumer (Stage 2): reads from logs.anomalies, calls Claude Haiku for root cause analysis,
updates anomaly record, publishes to logs.alerts.

Run with: python -m app.consumers.anomaly_consumer
"""
import asyncio
import json
import logging
import signal
import time
import uuid
from datetime import datetime, timezone
from typing import Optional

import anthropic
import redis.asyncio as aioredis
from kafka import KafkaConsumer
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.core.config import settings
from app.core.kafka_client import TOPICS, get_producer
from app.models.anomalies import Anomaly

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)

CONSUMER_GROUP = "claude-analyzers"
_shutdown = False


def handle_shutdown(sig, frame) -> None:
    global _shutdown
    _shutdown = True


signal.signal(signal.SIGTERM, handle_shutdown)
signal.signal(signal.SIGINT, handle_shutdown)

CLAUDE_SYSTEM_PROMPT = """You are an expert SRE analyzing a production anomaly. Analyze the provided logs and metrics, then return ONLY a JSON object with these exact fields:
{
  "root_cause": "One sentence describing likely cause",
  "confidence": 0.0,
  "affected_components": ["list", "of", "components"],
  "recommended_actions": ["action1", "action2", "action3"],
  "severity_assessment": "LOW|MEDIUM|HIGH|CRITICAL",
  "similar_incidents": "Description of similar patterns if seen in the log history",
  "estimated_resolution_time": "X minutes/hours"
}
Respond with ONLY the JSON. No markdown. No explanation."""


async def call_claude(
    anomaly: dict,
    log_samples: list[dict],
    semaphore: asyncio.Semaphore,
    client: anthropic.AsyncAnthropic,
) -> Optional[dict]:
    """Call Claude Haiku with anomaly context. Returns parsed JSON dict or None on failure."""
    log_text = "\n".join(
        f"[{l.get('severity', '?')}] {l.get('service_name', '?')}: {l.get('message', '')[:200]}"
        for l in log_samples[:50]
    )

    user_message = f"""Anomaly type: {anomaly.get('anomaly_type')}
Service: {anomaly.get('service_name')}
Baseline: {anomaly.get('baseline_value')} events/5min
Current: {anomaly.get('observed_value')} events/5min
Deviation: {anomaly.get('deviation_pct')}%

Recent log samples (last {len(log_samples)} entries):
{log_text}

Analyze and provide root cause assessment."""

    async with semaphore:
        try:
            response = await asyncio.wait_for(
                client.messages.create(
                    model=settings.CLAUDE_MODEL,
                    max_tokens=1024,
                    system=CLAUDE_SYSTEM_PROMPT,
                    messages=[{"role": "user", "content": user_message}],
                ),
                timeout=settings.CLAUDE_TIMEOUT,
            )
            text = response.content[0].text.strip()
            # Strip markdown code fences if present (```json ... ```)
            if text.startswith("```"):
                lines = text.split("\n")
                text = "\n".join(lines[1:-1] if lines[-1] == "```" else lines[1:])
            return json.loads(text.strip())
        except asyncio.TimeoutError:
            logger.warning(f"Claude timed out for anomaly {anomaly.get('anomaly_id')}")
            return None
        except json.JSONDecodeError as e:
            logger.warning(f"Claude returned invalid JSON: {e}")
            return None
        except Exception as e:
            logger.error(f"Claude call failed: {e}", exc_info=True)
            return None


async def process_anomaly(
    anomaly: dict,
    session_factory: async_sessionmaker,
    redis: aioredis.Redis,
    semaphore: asyncio.Semaphore,
    client: anthropic.AsyncAnthropic,
) -> None:
    anomaly_id = anomaly.get("anomaly_id")
    tenant_id = anomaly.get("tenant_id")
    service_name = anomaly.get("service_name")

    # Gather context: last 50 logs for this service
    hot_key = f"tenant:{tenant_id}:service:{service_name}:logs"
    raw_logs = await redis.lrange(hot_key, 0, 49)
    log_samples: list[dict] = []
    for r in raw_logs:
        try:
            log_samples.append(json.loads(r))
        except Exception:
            pass

    # Call Claude (Stage 2 AI analysis)
    claude_result = await call_claude(anomaly, log_samples, semaphore, client)

    # Update anomaly in DB with Claude analysis
    async with session_factory() as db:
        try:
            result = await db.execute(
                select(Anomaly).where(Anomaly.id == uuid.UUID(anomaly_id))
            )
            db_anomaly = result.scalar_one_or_none()
            if db_anomaly:
                if claude_result:
                    db_anomaly.claude_analysis = json.dumps(claude_result)
                db_anomaly.status = "active"  # ensure still active
                await db.commit()
        except Exception as e:
            logger.error(f"DB update failed for anomaly {anomaly_id}: {e}")
            await db.rollback()

    # Track per-tenant daily analysis credit (FR-9 / AC-9.5)
    if tenant_id:
        epoch_day = int(time.time()) // 86400
        credit_key = f"tenant:{tenant_id}:analysis_credits:{epoch_day}"
        try:
            pipe = redis.pipeline()
            pipe.incr(credit_key)
            pipe.expire(credit_key, 86400)
            await pipe.execute()
        except Exception as e:
            logger.warning(f"Failed to update analysis credits for {tenant_id}: {e}")

    # Build alert payload and publish to logs.alerts
    # Claude failure must NOT prevent the alert from being published (AC-9.3)
    alert_payload = {
        **anomaly,
        "claude_analysis": claude_result,
        "log_sample_count": len(log_samples),
        "processed_at": time.time(),
    }

    # Publish to Redis pub/sub for WebSocket streaming clients
    try:
        ws_channel = f"tenant:{tenant_id}:anomalies:stream"
        await redis.publish(
            ws_channel,
            json.dumps({**anomaly, "claude_analysis": claude_result}, default=str),
        )
    except Exception as e:
        logger.warning(f"Failed to publish anomaly to WS channel: {e}")

    try:
        producer = get_producer()
        producer.send(
            TOPICS["logs.alerts"],
            value=alert_payload,
            key=tenant_id.encode() if tenant_id else b"unknown",
        )
    except Exception as e:
        logger.error(f"Failed to publish to logs.alerts: {e}")


async def run_anomaly_consumer() -> None:
    engine = create_async_engine(settings.DATABASE_URL, pool_size=5, max_overflow=10)
    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    redis = aioredis.from_url(settings.REDIS_URL, decode_responses=True)
    claude_client = anthropic.AsyncAnthropic(api_key=settings.ANTHROPIC_API_KEY)
    semaphore = asyncio.Semaphore(settings.CLAUDE_MAX_CONCURRENT)

    consumer = KafkaConsumer(
        TOPICS["logs.anomalies"],
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
        group_id=CONSUMER_GROUP,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=False,
    )

    logger.info(f"Anomaly consumer started, group={CONSUMER_GROUP}")

    try:
        while not _shutdown:
            records = consumer.poll(timeout_ms=500)
            if not records:
                continue

            # Collect all messages for this poll
            batch_tasks = []
            for tp, messages in records.items():
                for message in messages:
                    task = process_anomaly(
                        message.value, session_factory, redis, semaphore, claude_client
                    )
                    batch_tasks.append(task)

            if batch_tasks:
                # Process all anomalies in this batch, then commit
                await asyncio.gather(*batch_tasks, return_exceptions=True)
                consumer.commit()
                logger.info(f"Processed and committed {len(batch_tasks)} anomalies")
    finally:
        consumer.close()
        await redis.aclose()
        await engine.dispose()
        logger.info("Anomaly consumer shut down")


if __name__ == "__main__":
    asyncio.run(run_anomaly_consumer())
