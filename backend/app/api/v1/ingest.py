import uuid
import time
from fastapi import APIRouter, Depends, Request, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from app.core.auth import get_current_tenant, TenantContext
from app.core.database import get_db
from app.core.kafka_client import TOPICS, publish_async
from app.core.redis_client import rate_limit_check
from app.schemas.log import LogIngestRequest, LogIngestResponse, BatchIngestRequest, BatchIngestResponse, BatchEventResult
from app.core.config import settings

router = APIRouter()


def _build_kafka_message(event: LogIngestRequest, tenant: TenantContext, source_ip: str) -> dict:
    return {
        "event_id": str(uuid.uuid4()),
        "tenant_id": tenant.tenant_id,
        "service_name": event.service_name,
        "severity": event.severity.value,
        "message": event.message,
        "metadata": event.metadata or {},
        "trace_id": event.trace_id,
        "span_id": event.span_id,
        "source_ip": source_ip,
        "environment": event.environment.value,
        "ingested_at": time.time(),
    }


@router.post("/logs/ingest", status_code=202)
async def ingest_single(
    event: LogIngestRequest,
    request: Request,
    tenant: TenantContext = Depends(get_current_tenant),
) -> LogIngestResponse:
    # Rate limit check — fixed-window per minute using Redis INCR
    rate_key = f"rate:{tenant.tenant_id}:{int(time.time()) // 60}"
    allowed = await rate_limit_check(rate_key, tenant.rate_limit_per_minute, 120)
    if not allowed:
        raise HTTPException(status_code=429, detail="Rate limit exceeded")

    source_ip = request.client.host if request.client else "unknown"
    msg = _build_kafka_message(event, tenant, source_ip)

    # Fire-and-forget — never blocks the response path
    publish_async(TOPICS["logs.raw"], msg, key=tenant.tenant_id)

    return LogIngestResponse(event_id=msg["event_id"], status="accepted")


@router.post("/logs/ingest/batch", status_code=202)
async def ingest_batch(
    batch: BatchIngestRequest,
    request: Request,
    tenant: TenantContext = Depends(get_current_tenant),
) -> BatchIngestResponse:
    # Rate limit: count batch against per-minute quota
    rate_key = f"rate:{tenant.tenant_id}:{int(time.time()) // 60}"
    allowed = await rate_limit_check(rate_key, tenant.rate_limit_per_minute, 120)
    if not allowed:
        raise HTTPException(status_code=429, detail="Rate limit exceeded")

    source_ip = request.client.host if request.client else "unknown"
    accepted = 0
    failed = 0
    errors: list[BatchEventResult] = []

    for i, event in enumerate(batch.events):
        try:
            msg = _build_kafka_message(event, tenant, source_ip)
            publish_async(TOPICS["logs.raw"], msg, key=tenant.tenant_id)
            accepted += 1
        except Exception as e:
            failed += 1
            errors.append(BatchEventResult(index=i, error=str(e)))

    return BatchIngestResponse(accepted=accepted, failed=failed, errors=errors)
