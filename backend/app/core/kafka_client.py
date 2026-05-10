import json
import logging
from typing import Optional, Callable
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError
from app.core.config import settings

logger = logging.getLogger(__name__)

TOPICS = {
    "logs.raw": "logs.raw",
    "logs.processed": "logs.processed",
    "logs.anomalies": "logs.anomalies",
    "logs.alerts": "logs.alerts",
}

_producer: Optional[KafkaProducer] = None


def get_producer() -> KafkaProducer:
    global _producer
    if _producer is None:
        _producer = KafkaProducer(
            bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if isinstance(k, str) else k,
            acks="all",
            retries=3,
            retry_backoff_ms=100,
            linger_ms=10,
            batch_size=16384,
            compression_type="lz4",
        )
    return _producer


def create_consumer(topics: list[str], group_id: str) -> KafkaConsumer:
    return KafkaConsumer(
        *topics,
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
        group_id=group_id,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        max_poll_records=500,
    )


def publish(topic: str, value: dict, key: Optional[str] = None) -> None:
    """Synchronous publish — blocks until broker confirms or raises KafkaError."""
    producer = get_producer()
    try:
        future = producer.send(topic, value=value, key=key)
        future.get(timeout=10)
    except KafkaError as e:
        logger.error(f"Failed to publish to {topic}: {e}")
        raise


def publish_async(
    topic: str,
    value: dict,
    key: Optional[str] = None,
    on_success: Optional[Callable] = None,
    on_error: Optional[Callable] = None,
) -> None:
    """Fire-and-forget publish with optional callbacks."""
    producer = get_producer()
    future = producer.send(topic, value=value, key=key)
    if on_success:
        future.add_callback(on_success)
    if on_error:
        future.add_errback(on_error)


def health_check() -> bool:
    """Returns True if the producer can reach at least one broker."""
    try:
        producer = get_producer()
        return producer.bootstrap_connected()
    except Exception:
        return False
