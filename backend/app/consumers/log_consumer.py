"""
Log consumer: reads from logs.raw, writes to PostgreSQL + Redis, publishes to logs.processed.

Run with: python -m app.consumers.log_consumer
"""
import asyncio
import json
import logging
import signal
import time
from datetime import datetime, timezone
from typing import Optional

import redis.asyncio as aioredis
from kafka import KafkaConsumer
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker

from app.core.config import settings
from app.core.kafka_client import TOPICS, get_producer

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)

BATCH_SIZE = 100
BATCH_TIMEOUT = 1.0  # seconds — flush at most 1 s after first message in batch
CONSUMER_GROUP = "log-processors"


def publish_to_dlq(producer, messages: list[dict], error: str) -> None:
    """Send failed messages to DLQ with error metadata."""
    for msg in messages:
        dlq_envelope = {
            "original_message": msg,
            "error": str(error),
            "failed_at": time.time(),
            "consumer_group": CONSUMER_GROUP,
        }
        try:
            producer.send("logs.raw.dlq", value=dlq_envelope, key=msg.get("tenant_id", "unknown").encode() if isinstance(msg.get("tenant_id"), str) else b"unknown")
        except Exception as dlq_err:
            logger.error(f"DLQ publish also failed: {dlq_err}")

_shutdown = False


def handle_shutdown(sig, frame) -> None:
    global _shutdown
    logger.info(f"Received signal {sig}, shutting down gracefully...")
    _shutdown = True


signal.signal(signal.SIGTERM, handle_shutdown)
signal.signal(signal.SIGINT, handle_shutdown)


async def write_batch_to_postgres(batch: list[dict], session_factory: async_sessionmaker) -> None:
    """Batch insert logs into PostgreSQL using a single transaction."""
    from app.models.logs import Log

    async with session_factory() as session:
        log_objects: list[Log] = []
        for msg in batch:
            log_obj = Log(
                tenant_id=msg["tenant_id"],
                service_name=msg["service_name"],
                severity=msg["severity"],
                message=msg["message"],
                log_metadata=msg.get("metadata", {}),
                trace_id=msg.get("trace_id"),
                span_id=msg.get("span_id"),
                source_ip=msg.get("source_ip"),
                environment=msg.get("environment", "prod"),
                ingested_at=datetime.fromtimestamp(msg["ingested_at"], tz=timezone.utc),
            )
            log_objects.append(log_obj)
        session.add_all(log_objects)
        await session.commit()


async def write_batch_to_redis(batch: list[dict], redis: aioredis.Redis) -> None:
    """Write logs to Redis hot path and update volume/error counters using a single pipeline."""
    pipe = redis.pipeline()

    for msg in batch:
        tenant_id = msg["tenant_id"]
        service = msg["service_name"]
        serialized = json.dumps(msg, default=str)

        # Hot path: keep last 10 000 logs per (tenant, service) for low-latency reads
        hot_key = f"tenant:{tenant_id}:service:{service}:logs"
        pipe.lpush(hot_key, serialized)
        pipe.ltrim(hot_key, 0, 9999)
        pipe.expire(hot_key, settings.LOG_RETENTION_HOT_HOURS * 3600)

        # 5-minute volume counter (day-level TTL, read by the query API)
        epoch_5min = int(msg["ingested_at"]) // 300
        vol_key = f"tenant:{tenant_id}:service:{service}:vol:{epoch_5min}"
        pipe.incr(vol_key)
        pipe.expire(vol_key, 86400)

        # Separate error counter for error-rate computation
        if msg["severity"] in ("ERROR", "CRITICAL"):
            err_key = f"tenant:{tenant_id}:service:{service}:errors:{epoch_5min}"
            pipe.incr(err_key)
            pipe.expire(err_key, 86400)

        # Service registry — lets the metrics endpoint enumerate services without a DB query
        svc_key = f"tenant:{tenant_id}:services"
        pipe.sadd(svc_key, service)

        # Last-seen timestamp per service (used by /metrics/services)
        last_seen_key = f"tenant:{tenant_id}:service_last_seen"
        pipe.hset(last_seen_key, service, str(int(msg["ingested_at"])))

        # WebSocket pub/sub channel consumed by websocket-streamers
        channel = f"tenant:{tenant_id}:service:{service}:stream"
        pipe.publish(channel, serialized)

    await pipe.execute()


def run_consumer() -> None:
    """Entry point — creates engine, consumer, and runs the async processing loop."""

    async def main() -> None:
        engine = create_async_engine(
            settings.DATABASE_URL,
            pool_size=10,
            max_overflow=20,
        )
        session_factory = async_sessionmaker(
            engine, class_=AsyncSession, expire_on_commit=False
        )
        redis: aioredis.Redis = aioredis.from_url(
            settings.REDIS_URL, decode_responses=True
        )

        consumer = KafkaConsumer(
            TOPICS["logs.raw"],
            bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
            group_id=CONSUMER_GROUP,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            max_poll_records=500,
        )
        producer = get_producer()

        logger.info(f"Log consumer started, group={CONSUMER_GROUP}")

        batch: list[dict] = []
        last_flush = time.monotonic()

        try:
            while not _shutdown:
                records = consumer.poll(timeout_ms=500)

                for _tp, messages in records.items():
                    for message in messages:
                        batch.append(message.value)

                should_flush = len(batch) >= BATCH_SIZE or (
                    batch and time.monotonic() - last_flush >= BATCH_TIMEOUT
                )

                if should_flush and batch:
                    try:
                        await write_batch_to_postgres(batch, session_factory)
                        await write_batch_to_redis(batch, redis)

                        # Forward enriched messages to logs.processed for downstream consumers
                        for msg in batch:
                            producer.send(
                                TOPICS["logs.processed"],
                                value=msg,
                                key=msg["tenant_id"].encode(),
                            )
                        producer.flush()

                        consumer.commit()
                        logger.info(f"Flushed batch of {len(batch)} messages")
                        batch = []
                        last_flush = time.monotonic()
                    except Exception as e:
                        logger.error(f"Batch processing failed: {e}", exc_info=True)
                        # Send failed messages to DLQ so partition can advance
                        publish_to_dlq(producer, batch, str(e))
                        try:
                            consumer.commit()  # Commit so we don't reprocess indefinitely
                        except Exception as commit_err:
                            logger.error(f"Commit after DLQ failed: {commit_err}")
                        batch = []
                        raw_messages = []
                        last_flush = time.monotonic()
        finally:
            # Drain any remaining messages before exit
            if batch:
                try:
                    await write_batch_to_postgres(batch, session_factory)
                    await write_batch_to_redis(batch, redis)
                    consumer.commit()
                    logger.info(f"Final flush: {len(batch)} messages")
                except Exception as exc:
                    logger.error(f"Final flush failed: {exc}")

            consumer.close()
            await redis.aclose()
            await engine.dispose()
            logger.info("Log consumer shut down cleanly")

    asyncio.run(main())


if __name__ == "__main__":
    run_consumer()
