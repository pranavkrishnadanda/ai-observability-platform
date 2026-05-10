import json
import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Optional

import redis.asyncio as aioredis
from sqlalchemy import and_, func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.logs import Log

logger = logging.getLogger(__name__)


async def get_recent_logs(
    tenant_id: str,
    service_name: str,
    minutes: int,
    redis: aioredis.Redis,
    db: AsyncSession,
) -> list[dict]:
    """Return recent logs for a service, trying Redis hot path first then PostgreSQL fallback."""
    hot_key = f"tenant:{tenant_id}:service:{service_name}:logs"

    # Redis hot path — up to last 10 000 entries stored by the consumer
    try:
        raw_logs = await redis.lrange(hot_key, 0, 999)
        if raw_logs:
            cutoff = datetime.now(timezone.utc) - timedelta(minutes=minutes)
            result: list[dict] = []
            for r in raw_logs:
                log = json.loads(r)
                ts = datetime.fromtimestamp(log.get("ingested_at", 0), tz=timezone.utc)
                if ts >= cutoff:
                    result.append(log)
            if result:
                return result
    except Exception as exc:
        logger.warning(f"Redis hot path failed for {tenant_id}/{service_name}: {exc}")

    # PostgreSQL fallback — covers cold data or Redis misses
    cutoff = datetime.now(timezone.utc) - timedelta(minutes=minutes)
    stmt = (
        select(Log)
        .where(
            and_(
                Log.tenant_id == tenant_id,
                Log.service_name == service_name,
                Log.created_at >= cutoff,
            )
        )
        .order_by(Log.created_at.desc())
        .limit(1000)
    )
    result_rows = await db.execute(stmt)
    logs = result_rows.scalars().all()
    return [_log_to_dict(log) for log in logs]


async def get_historical_logs(
    tenant_id: str,
    db: AsyncSession,
    service_name: Optional[str] = None,
    severity: Optional[str] = None,
    from_time: Optional[datetime] = None,
    to_time: Optional[datetime] = None,
    search_text: Optional[str] = None,
    trace_id: Optional[str] = None,
    limit: int = 100,
    offset: int = 0,
) -> tuple[list[dict], int]:
    """Full-featured log query backed by PostgreSQL, with optional full-text search."""
    import uuid as _uuid
    tid = _uuid.UUID(tenant_id) if isinstance(tenant_id, str) else tenant_id
    conditions = [Log.tenant_id == tid]

    if service_name:
        conditions.append(Log.service_name == service_name)
    if severity:
        conditions.append(Log.severity == severity)
    if from_time:
        conditions.append(Log.created_at >= from_time)
    if to_time:
        conditions.append(Log.created_at <= to_time)
    if trace_id:
        conditions.append(Log.trace_id == trace_id)
    if search_text:
        # PostgreSQL full-text search over the message column
        conditions.append(
            text(
                "to_tsvector('english', message) @@ plainto_tsquery('english', :q)"
            ).bindparams(q=search_text)
        )

    where_clause = and_(*conditions)

    count_result = await db.execute(
        select(func.count()).select_from(Log).where(where_clause)
    )
    total: int = count_result.scalar() or 0

    rows = await db.execute(
        select(Log)
        .where(where_clause)
        .order_by(Log.created_at.desc())
        .limit(limit)
        .offset(offset)
    )
    logs = rows.scalars().all()
    return [_log_to_dict(log) for log in logs], total


async def get_log_volume(
    tenant_id: str,
    service_name: str,
    window_minutes: int,
    redis: aioredis.Redis,
) -> int:
    """Sum 5-minute Redis volume counters covering the requested window."""
    now = int(time.time())
    epochs_needed = (window_minutes + 4) // 5  # round up so the window is fully covered
    current_epoch = now // 300

    pipe = redis.pipeline()
    for i in range(epochs_needed):
        epoch = current_epoch - i
        pipe.get(f"tenant:{tenant_id}:service:{service_name}:vol:{epoch}")

    results = await pipe.execute()
    return sum(int(r) for r in results if r)


async def get_service_error_rate(
    tenant_id: str,
    service_name: str,
    window_minutes: int,
    redis: aioredis.Redis,
) -> float:
    """Return the error rate (0.0–1.0) for a service within the given window."""
    now = int(time.time())
    epochs_needed = (window_minutes + 4) // 5
    current_epoch = now // 300

    pipe = redis.pipeline()
    for i in range(epochs_needed):
        epoch = current_epoch - i
        pipe.get(f"tenant:{tenant_id}:service:{service_name}:vol:{epoch}")
        pipe.get(f"tenant:{tenant_id}:service:{service_name}:errors:{epoch}")

    results = await pipe.execute()
    total_volume = 0
    total_errors = 0
    for i in range(0, len(results), 2):
        total_volume += int(results[i] or 0)
        total_errors += int(results[i + 1] or 0)

    if total_volume == 0:
        return 0.0
    return total_errors / total_volume


def _log_to_dict(log: Log) -> dict:
    return {
        "id": str(log.id),
        "tenant_id": str(log.tenant_id),
        "service_name": log.service_name,
        "severity": log.severity,
        "message": log.message,
        "metadata": log.log_metadata,
        "trace_id": log.trace_id,
        "span_id": log.span_id,
        "source_ip": log.source_ip,
        "environment": log.environment,
        "created_at": log.created_at.isoformat() if log.created_at else None,
        "ingested_at": log.ingested_at.isoformat() if log.ingested_at else None,
    }
