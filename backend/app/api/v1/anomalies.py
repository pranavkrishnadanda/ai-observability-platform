from datetime import datetime, timezone
from typing import Optional
import uuid

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import and_, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.auth import TenantContext, get_current_tenant
from app.core.database import get_db
from app.models.anomalies import Anomaly

router = APIRouter()


def _anomaly_to_dict(a: Anomaly) -> dict:
    return {
        "id": str(a.id),
        "tenant_id": str(a.tenant_id),
        "service_name": a.service_name,
        "anomaly_type": a.anomaly_type,
        "severity_score": float(a.severity_score),
        "detected_at": a.detected_at.isoformat() if a.detected_at else None,
        "window_start": a.window_start.isoformat() if a.window_start else None,
        "window_end": a.window_end.isoformat() if a.window_end else None,
        "baseline_value": float(a.baseline_value) if a.baseline_value is not None else None,
        "observed_value": float(a.observed_value) if a.observed_value is not None else None,
        "deviation_pct": float(a.deviation_pct) if a.deviation_pct is not None else None,
        "claude_analysis": a.claude_analysis,
        "status": a.status,
        "resolved_at": a.resolved_at.isoformat() if a.resolved_at else None,
        "created_at": a.created_at.isoformat() if a.created_at else None,
    }


@router.get("/anomalies")
async def list_anomalies(
    service: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    from_time: Optional[datetime] = Query(None),
    severity_min: Optional[float] = Query(None, ge=0.0, le=1.0),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    tenant: TenantContext = Depends(get_current_tenant),
    db: AsyncSession = Depends(get_db),
):
    conditions = [Anomaly.tenant_id == uuid.UUID(tenant.tenant_id)]
    if service:
        conditions.append(Anomaly.service_name == service)
    if status:
        conditions.append(Anomaly.status == status)
    if from_time:
        conditions.append(Anomaly.detected_at >= from_time)
    if severity_min is not None:
        conditions.append(Anomaly.severity_score >= severity_min)

    where_clause = and_(*conditions)

    count_result = await db.execute(
        select(func.count()).select_from(Anomaly).where(where_clause)
    )
    total = count_result.scalar()

    result = await db.execute(
        select(Anomaly)
        .where(where_clause)
        .order_by(Anomaly.detected_at.desc())
        .limit(limit)
        .offset(offset)
    )
    anomalies = result.scalars().all()
    return {
        "data": [_anomaly_to_dict(a) for a in anomalies],
        "total": total,
        "limit": limit,
        "offset": offset,
    }


@router.get("/anomalies/{anomaly_id}")
async def get_anomaly(
    anomaly_id: str,
    tenant: TenantContext = Depends(get_current_tenant),
    db: AsyncSession = Depends(get_db),
):
    try:
        aid = uuid.UUID(anomaly_id)
    except ValueError:
        raise HTTPException(status_code=422, detail="Invalid anomaly ID")

    result = await db.execute(
        select(Anomaly).where(
            and_(
                Anomaly.id == aid,
                Anomaly.tenant_id == uuid.UUID(tenant.tenant_id),
            )
        )
    )
    anomaly = result.scalar_one_or_none()
    if not anomaly:
        raise HTTPException(status_code=404, detail="Anomaly not found")
    return _anomaly_to_dict(anomaly)


@router.get("/anomalies/{anomaly_id}/logs")
async def get_anomaly_logs(
    anomaly_id: str,
    tenant: TenantContext = Depends(get_current_tenant),
    db: AsyncSession = Depends(get_db),
):
    try:
        aid = uuid.UUID(anomaly_id)
    except ValueError:
        raise HTTPException(status_code=422, detail="Invalid anomaly ID")

    # Fetch the anomaly first to get window bounds
    result = await db.execute(
        select(Anomaly).where(
            and_(
                Anomaly.id == aid,
                Anomaly.tenant_id == uuid.UUID(tenant.tenant_id),
            )
        )
    )
    anomaly = result.scalar_one_or_none()
    if not anomaly:
        raise HTTPException(status_code=404, detail="Anomaly not found")

    from app.models.logs import Log
    from app.services.log_service import _log_to_dict

    log_result = await db.execute(
        select(Log)
        .where(
            and_(
                Log.tenant_id == uuid.UUID(tenant.tenant_id),
                Log.service_name == anomaly.service_name,
                Log.created_at >= anomaly.window_start,
                Log.created_at <= anomaly.window_end,
            )
        )
        .order_by(Log.created_at.desc())
        .limit(200)
    )
    logs = log_result.scalars().all()
    return {"data": [_log_to_dict(l) for l in logs], "anomaly_id": anomaly_id}


@router.patch("/anomalies/{anomaly_id}/acknowledge")
async def acknowledge_anomaly(
    anomaly_id: str,
    tenant: TenantContext = Depends(get_current_tenant),
    db: AsyncSession = Depends(get_db),
):
    try:
        aid = uuid.UUID(anomaly_id)
    except ValueError:
        raise HTTPException(status_code=422, detail="Invalid anomaly ID")

    result = await db.execute(
        select(Anomaly).where(
            and_(
                Anomaly.id == aid,
                Anomaly.tenant_id == uuid.UUID(tenant.tenant_id),
            )
        )
    )
    anomaly = result.scalar_one_or_none()
    if not anomaly:
        raise HTTPException(status_code=404, detail="Anomaly not found")

    # Idempotent — already acknowledged is fine
    if anomaly.status == "resolved":
        raise HTTPException(status_code=409, detail="Cannot transition from resolved status")

    anomaly.status = "acknowledged"
    await db.commit()
    return _anomaly_to_dict(anomaly)


@router.patch("/anomalies/{anomaly_id}/resolve")
async def resolve_anomaly(
    anomaly_id: str,
    tenant: TenantContext = Depends(get_current_tenant),
    db: AsyncSession = Depends(get_db),
):
    try:
        aid = uuid.UUID(anomaly_id)
    except ValueError:
        raise HTTPException(status_code=422, detail="Invalid anomaly ID")

    result = await db.execute(
        select(Anomaly).where(
            and_(
                Anomaly.id == aid,
                Anomaly.tenant_id == uuid.UUID(tenant.tenant_id),
            )
        )
    )
    anomaly = result.scalar_one_or_none()
    if not anomaly:
        raise HTTPException(status_code=404, detail="Anomaly not found")

    anomaly.status = "resolved"
    anomaly.resolved_at = datetime.now(timezone.utc)
    await db.commit()
    return _anomaly_to_dict(anomaly)
