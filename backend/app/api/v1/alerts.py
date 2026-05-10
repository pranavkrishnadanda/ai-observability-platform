from datetime import datetime, timezone
from typing import Optional
import uuid

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import and_, desc, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.auth import get_current_tenant, TenantContext
from app.core.database import get_db
from app.models.alerts import Alert
from app.services.webhook_deliverer import deliver_webhook

router = APIRouter()


def _alert_to_dict(a: Alert) -> dict:
    return {
        "id": str(a.id),
        "tenant_id": str(a.tenant_id),
        "anomaly_id": str(a.anomaly_id),
        "alert_type": a.alert_type,
        "severity": a.severity,
        "title": a.title,
        "description": a.description,
        "webhook_url": a.webhook_url,
        "delivery_status": a.delivery_status,
        "delivered_at": a.delivered_at.isoformat() if a.delivered_at else None,
        "dedup_key": a.dedup_key,
        "retry_count": a.retry_count,
        "last_error": a.last_error,
        "created_at": a.created_at.isoformat() if a.created_at else None,
    }


@router.get("/alerts")
async def list_alerts(
    service: Optional[str] = Query(None),
    severity: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    from_time: Optional[datetime] = Query(None),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    tenant: TenantContext = Depends(get_current_tenant),
    db: AsyncSession = Depends(get_db),
):
    conditions = [Alert.tenant_id == uuid.UUID(tenant.tenant_id)]
    if service:
        # dedup_key format: "{tenant_id}:{service}:{anomaly_type}:{hour}"
        # Filter by service name appearing in the dedup_key
        conditions.append(Alert.dedup_key.contains(f":{service}:"))
    if severity:
        conditions.append(Alert.severity == severity)
    if status:
        conditions.append(Alert.delivery_status == status)
    if from_time:
        conditions.append(Alert.created_at >= from_time)

    where_clause = and_(*conditions)

    count_result = await db.execute(
        select(func.count()).select_from(Alert).where(where_clause)
    )
    total = count_result.scalar()

    result = await db.execute(
        select(Alert)
        .where(where_clause)
        .order_by(desc(Alert.created_at))
        .limit(limit)
        .offset(offset)
    )
    alerts = result.scalars().all()
    return {
        "data": [_alert_to_dict(a) for a in alerts],
        "total": total,
        "limit": limit,
        "offset": offset,
    }


@router.get("/alerts/stats")
async def get_alert_stats(
    tenant: TenantContext = Depends(get_current_tenant),
    db: AsyncSession = Depends(get_db),
):
    today_start = datetime.now(timezone.utc).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    tenant_uuid = uuid.UUID(tenant.tenant_id)

    # Total today
    total_result = await db.execute(
        select(func.count())
        .select_from(Alert)
        .where(
            and_(
                Alert.tenant_id == tenant_uuid,
                Alert.created_at >= today_start,
            )
        )
    )
    total_today = total_result.scalar()

    # By severity (today)
    severity_result = await db.execute(
        select(Alert.severity, func.count())
        .select_from(Alert)
        .where(
            and_(
                Alert.tenant_id == tenant_uuid,
                Alert.created_at >= today_start,
            )
        )
        .group_by(Alert.severity)
    )
    by_severity = {row[0]: row[1] for row in severity_result}

    # Delivery rate (all-time)
    delivered_result = await db.execute(
        select(func.count())
        .select_from(Alert)
        .where(
            and_(
                Alert.tenant_id == tenant_uuid,
                Alert.delivery_status == "delivered",
            )
        )
    )
    delivered = delivered_result.scalar()

    total_all_result = await db.execute(
        select(func.count())
        .select_from(Alert)
        .where(Alert.tenant_id == tenant_uuid)
    )
    total_all = total_all_result.scalar()
    delivery_rate = round(delivered / total_all, 4) if total_all > 0 else 0.0

    # Top alerting services — extract from dedup_key (format: tenant_id:service:anomaly_type:hour)
    top_result = await db.execute(
        select(Alert.dedup_key)
        .where(and_(Alert.tenant_id == tenant_uuid, Alert.created_at >= today_start))
    )
    service_counts: dict = {}
    for row in top_result:
        parts = row[0].split(":")
        if len(parts) >= 3:
            service = parts[1]  # second segment is service_name
            service_counts[service] = service_counts.get(service, 0) + 1
    top_services = [
        {"service": k, "count": v}
        for k, v in sorted(service_counts.items(), key=lambda x: -x[1])[:5]
    ]

    return {
        "total_today": total_today,
        "by_severity": by_severity,
        "delivery_rate": delivery_rate,
        "top_alerting_services": top_services,
    }


@router.get("/alerts/{alert_id}")
async def get_alert(
    alert_id: str,
    tenant: TenantContext = Depends(get_current_tenant),
    db: AsyncSession = Depends(get_db),
):
    try:
        aid = uuid.UUID(alert_id)
    except ValueError:
        raise HTTPException(status_code=422, detail="Invalid alert ID")

    result = await db.execute(
        select(Alert).where(
            and_(
                Alert.id == aid,
                Alert.tenant_id == uuid.UUID(tenant.tenant_id),
            )
        )
    )
    alert = result.scalar_one_or_none()
    if not alert:
        raise HTTPException(status_code=404, detail="Alert not found")
    return _alert_to_dict(alert)


@router.post("/webhooks/test")
async def test_webhook(
    tenant: TenantContext = Depends(get_current_tenant),
):
    if not tenant.webhook_url:
        raise HTTPException(status_code=400, detail="No webhook URL configured")

    test_payload = {
        "type": "webhook_test",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "message": "This is a test webhook from AI Observability Platform",
        "tenant_name": tenant.name,
    }

    success, error = await deliver_webhook(
        tenant.webhook_url,
        test_payload,
        alert_id="test",
    )

    if success:
        return {"status": "delivered", "webhook_url": tenant.webhook_url}
    else:
        raise HTTPException(
            status_code=502, detail=f"Webhook delivery failed: {error}"
        )
