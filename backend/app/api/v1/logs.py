import uuid
from datetime import datetime
from typing import Optional

import redis.asyncio as aioredis
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import and_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.auth import TenantContext, get_current_tenant
from app.core.database import get_db
from app.core.redis_client import get_redis
from app.models.logs import Log
from app.schemas.log import LogListResponse, LogResponse
from app.services.log_service import (
    _log_to_dict,
    get_historical_logs,
    get_log_volume,
    get_service_error_rate,
)

router = APIRouter()


@router.get("/logs", response_model=LogListResponse)
async def query_logs(
    service: Optional[str] = Query(None, description="Filter by service name"),
    severity: Optional[str] = Query(None, description="Filter by severity level"),
    from_time: Optional[datetime] = Query(None, description="Start of time range (ISO 8601)"),
    to_time: Optional[datetime] = Query(None, description="End of time range (ISO 8601)"),
    search_text: Optional[str] = Query(None, description="Full-text search across log messages"),
    trace_id: Optional[str] = Query(None, description="Filter by trace ID"),
    limit: int = Query(100, ge=1, le=1000, description="Max results to return"),
    offset: int = Query(0, ge=0, description="Pagination offset"),
    tenant: TenantContext = Depends(get_current_tenant),
    db: AsyncSession = Depends(get_db),
) -> LogListResponse:
    logs, total = await get_historical_logs(
        tenant_id=tenant.tenant_id,
        db=db,
        service_name=service,
        severity=severity,
        from_time=from_time,
        to_time=to_time,
        search_text=search_text,
        trace_id=trace_id,
        limit=limit,
        offset=offset,
    )
    return LogListResponse(data=logs, total=total, limit=limit, offset=offset)


@router.get("/logs/{log_id}")
async def get_log(
    log_id: str,
    tenant: TenantContext = Depends(get_current_tenant),
    db: AsyncSession = Depends(get_db),
) -> dict:
    try:
        log_uuid = uuid.UUID(log_id)
    except ValueError:
        raise HTTPException(status_code=422, detail="Invalid log ID format")

    result = await db.execute(
        select(Log).where(
            and_(
                Log.id == log_uuid,
                Log.tenant_id == uuid.UUID(tenant.tenant_id),
            )
        )
    )
    log = result.scalar_one_or_none()
    if not log:
        raise HTTPException(status_code=404, detail="Log not found")

    return _log_to_dict(log)


@router.get("/metrics/services")
async def get_service_metrics(
    tenant: TenantContext = Depends(get_current_tenant),
    redis: aioredis.Redis = Depends(get_redis),
):
    services_key = f"tenant:{tenant.tenant_id}:services"
    last_seen_key = f"tenant:{tenant.tenant_id}:service_last_seen"

    services = await redis.smembers(services_key)
    last_seen_data = await redis.hgetall(last_seen_key)

    result = []
    for service in services:
        volume = await get_log_volume(tenant.tenant_id, service, 60, redis)
        error_rate = await get_service_error_rate(tenant.tenant_id, service, 60, redis)
        last_seen_ts = last_seen_data.get(service)
        last_seen = None
        if last_seen_ts:
            from datetime import datetime, timezone
            last_seen = datetime.fromtimestamp(float(last_seen_ts), tz=timezone.utc).isoformat()
        result.append({
            "service_name": service,
            "volume_last_hour": volume,
            "error_rate": round(error_rate, 4),
            "health_status": "critical" if error_rate > 0.1 else "degraded" if error_rate > 0.05 else "healthy",
            "last_seen": last_seen,
        })

    return {"services": result}
