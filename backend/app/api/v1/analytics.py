import logging
import uuid
from datetime import datetime, timezone, timedelta

import redis.asyncio as aioredis
from fastapi import APIRouter, Depends
from sqlalchemy import and_, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.auth import TenantContext, get_current_tenant
from app.core.database import get_db
from app.core.redis_client import cache_get, cache_set, get_redis
from app.models.alerts import Alert
from app.models.anomalies import Anomaly
from app.models.logs import Log

router = APIRouter()
logger = logging.getLogger(__name__)

CACHE_TTL = 60  # 60-second cache for all analytics endpoints


@router.get("/analytics/overview")
async def get_overview(
    tenant: TenantContext = Depends(get_current_tenant),
    db: AsyncSession = Depends(get_db),
    redis: aioredis.Redis = Depends(get_redis),
):
    cache_key = f"cache:analytics:overview:{tenant.tenant_id}"
    cached = await cache_get(cache_key)
    if cached:
        return cached

    tid = uuid.UUID(tenant.tenant_id)
    now = datetime.now(timezone.utc)
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    yesterday_start = today_start - timedelta(days=1)
    yesterday_end = today_start
    day_ago = now - timedelta(hours=24)

    # Total logs in the last 24h window
    total_24h_r = await db.execute(
        select(func.count(Log.id)).where(
            and_(Log.tenant_id == tid, Log.created_at >= day_ago)
        )
    )
    total_24h = total_24h_r.scalar() or 0

    # Error logs in the last 24h
    errors_24h_r = await db.execute(
        select(func.count(Log.id)).where(
            and_(
                Log.tenant_id == tid,
                Log.created_at >= day_ago,
                Log.severity.in_(["ERROR", "CRITICAL"]),
            )
        )
    )
    errors_24h = errors_24h_r.scalar() or 0

    # Error rate today vs yesterday (for trend information)
    total_today_r = await db.execute(
        select(func.count(Log.id)).where(
            and_(Log.tenant_id == tid, Log.created_at >= today_start)
        )
    )
    total_today = total_today_r.scalar() or 0

    errors_today_r = await db.execute(
        select(func.count(Log.id)).where(
            and_(
                Log.tenant_id == tid,
                Log.created_at >= today_start,
                Log.severity.in_(["ERROR", "CRITICAL"]),
            )
        )
    )
    errors_today = errors_today_r.scalar() or 0
    error_rate_today = errors_today / total_today if total_today else 0.0

    total_yesterday_r = await db.execute(
        select(func.count(Log.id)).where(
            and_(
                Log.tenant_id == tid,
                Log.created_at >= yesterday_start,
                Log.created_at < yesterday_end,
            )
        )
    )
    total_yesterday = total_yesterday_r.scalar() or 0

    errors_yesterday_r = await db.execute(
        select(func.count(Log.id)).where(
            and_(
                Log.tenant_id == tid,
                Log.created_at >= yesterday_start,
                Log.created_at < yesterday_end,
                Log.severity.in_(["ERROR", "CRITICAL"]),
            )
        )
    )
    errors_yesterday = errors_yesterday_r.scalar() or 0
    error_rate_yesterday = errors_yesterday / total_yesterday if total_yesterday else 0.0

    # Active anomalies
    active_anomalies_r = await db.execute(
        select(func.count(Anomaly.id)).where(
            and_(Anomaly.tenant_id == tid, Anomaly.status == "active")
        )
    )
    active_anomalies = active_anomalies_r.scalar() or 0

    # Anomalies detected in last 24h
    anomalies_24h_r = await db.execute(
        select(func.count(Anomaly.id)).where(
            and_(Anomaly.tenant_id == tid, Anomaly.detected_at >= day_ago)
        )
    )
    anomalies_detected = anomalies_24h_r.scalar() or 0

    # Alerts sent in last 24h
    alerts_24h_r = await db.execute(
        select(func.count(Alert.id)).where(
            and_(Alert.tenant_id == tid, Alert.created_at >= day_ago)
        )
    )
    alerts_sent = alerts_24h_r.scalar() or 0

    # Active services (distinct service_names with logs in last 24h)
    active_services_r = await db.execute(
        select(func.count(func.distinct(Log.service_name))).where(
            and_(Log.tenant_id == tid, Log.created_at >= day_ago)
        )
    )
    active_services = active_services_r.scalar() or 0

    # Top 5 error services today
    top_errors_r = await db.execute(
        select(Log.service_name, func.count(Log.id).label("cnt"))
        .where(
            and_(
                Log.tenant_id == tid,
                Log.created_at >= today_start,
                Log.severity.in_(["ERROR", "CRITICAL"]),
            )
        )
        .group_by(Log.service_name)
        .order_by(func.count(Log.id).desc())
        .limit(5)
    )
    top_error_services = [
        {"service": row[0], "error_count": row[1]} for row in top_errors_r
    ]

    # System health score (100 - active_anomalies * 10, capped 0–100)
    health_score = max(0, min(100, 100 - active_anomalies * 10))

    # Total logs this week (last 7 days)
    week_start = now - timedelta(days=7)
    total_week_r = await db.execute(
        select(func.count(Log.id)).where(
            and_(Log.tenant_id == tid, Log.created_at >= week_start)
        )
    )
    total_week = total_week_r.scalar() or 0

    result = {
        "window_hours": 24,
        "total_logs": total_24h,
        "total_logs_today": total_today,
        "total_logs_yesterday": total_yesterday,
        "total_logs_week": total_week,
        "error_logs": errors_24h,
        "anomalies_detected": anomalies_detected,
        "alerts_sent": alerts_sent,
        "alerts_sent_today": alerts_sent,
        "active_services": active_services,
        "error_rate_today": round(error_rate_today, 4),
        "error_rate_yesterday": round(error_rate_yesterday, 4),
        "active_anomalies": active_anomalies,
        "top_5_error_services": top_error_services,
        "system_health_score": health_score,
    }
    await cache_set(cache_key, result, CACHE_TTL)
    return result


@router.get("/analytics/services")
async def get_service_analytics(
    tenant: TenantContext = Depends(get_current_tenant),
    db: AsyncSession = Depends(get_db),
    redis: aioredis.Redis = Depends(get_redis),
):
    cache_key = f"cache:analytics:services:{tenant.tenant_id}"
    cached = await cache_get(cache_key)
    if cached:
        return cached

    tid = uuid.UUID(tenant.tenant_id)
    now = datetime.now(timezone.utc)
    hour_ago = now - timedelta(hours=1)
    day_ago = now - timedelta(hours=24)
    week_ago = now - timedelta(days=7)

    # Volume per service for each window
    def _vol_query(since):
        return (
            select(Log.service_name, func.count(Log.id).label("vol"))
            .where(and_(Log.tenant_id == tid, Log.created_at >= since))
            .group_by(Log.service_name)
            .order_by(func.count(Log.id).desc())
        )

    def _err_query(since):
        return (
            select(Log.service_name, func.count(Log.id).label("errs"))
            .where(
                and_(
                    Log.tenant_id == tid,
                    Log.created_at >= since,
                    Log.severity.in_(["ERROR", "CRITICAL"]),
                )
            )
            .group_by(Log.service_name)
        )

    vol_1h_rows = await db.execute(_vol_query(hour_ago))
    vol_1h_data = {row[0]: row[1] for row in vol_1h_rows}

    vol_24h_rows = await db.execute(_vol_query(day_ago))
    vol_24h_data = {row[0]: row[1] for row in vol_24h_rows}

    vol_7d_rows = await db.execute(_vol_query(week_ago))
    vol_7d_data = {row[0]: row[1] for row in vol_7d_rows}

    err_1h_rows = await db.execute(_err_query(hour_ago))
    err_1h_data = {row[0]: row[1] for row in err_1h_rows}

    err_24h_rows = await db.execute(_err_query(day_ago))
    err_24h_data = {row[0]: row[1] for row in err_24h_rows}

    err_7d_rows = await db.execute(_err_query(week_ago))
    err_7d_data = {row[0]: row[1] for row in err_7d_rows}

    # Anomaly counts per service in last 7 days
    anomaly_rows = await db.execute(
        select(Anomaly.service_name, func.count(Anomaly.id).label("cnt"))
        .where(and_(Anomaly.tenant_id == tid, Anomaly.detected_at >= week_ago))
        .group_by(Anomaly.service_name)
    )
    anomaly_data = {row[0]: row[1] for row in anomaly_rows}

    # Last-seen timestamp per service
    last_seen_rows = await db.execute(
        select(Log.service_name, func.max(Log.created_at).label("last_seen"))
        .where(and_(Log.tenant_id == tid, Log.created_at >= week_ago))
        .group_by(Log.service_name)
    )
    last_seen_data = {row[0]: row[1].isoformat() for row in last_seen_rows if row[1]}

    # All services seen in the last 7 days (union of all windows)
    all_services = set(vol_7d_data.keys())

    service_data = []
    for service in sorted(all_services, key=lambda s: vol_24h_data.get(s, 0), reverse=True):
        vol_1h = vol_1h_data.get(service, 0)
        vol_24h = vol_24h_data.get(service, 0)

        errs_1h = err_1h_data.get(service, 0)
        errs_24h = err_24h_data.get(service, 0)

        err_1h = round(errs_1h / vol_1h, 4) if vol_1h else 0.0
        err_24h = round(errs_24h / vol_24h, 4) if vol_24h else 0.0

        anomaly_count = anomaly_data.get(service, 0)
        health = "critical" if err_1h > 0.1 else "degraded" if err_1h > 0.05 else "healthy"

        service_data.append(
            {
                "service_name": service,
                "health_status": health,
                "log_volume_1h": vol_1h,
                "log_volume_24h": vol_24h,
                "error_rate_1h": err_1h,
                "error_rate_24h": err_24h,
                "anomaly_count_7d": anomaly_count,
                "last_seen": last_seen_data.get(service, now.isoformat()),
            }
        )

    await cache_set(cache_key, service_data, CACHE_TTL)
    return service_data


@router.get("/analytics/services/{service_name}/timeline")
async def get_service_timeline(
    service_name: str,
    tenant: TenantContext = Depends(get_current_tenant),
    db: AsyncSession = Depends(get_db),
    redis: aioredis.Redis = Depends(get_redis),
):
    cache_key = f"cache:analytics:timeline:{tenant.tenant_id}:{service_name}"
    cached = await cache_get(cache_key)
    if cached:
        return cached

    tid = uuid.UUID(tenant.tenant_id)
    now = datetime.now(timezone.utc)
    # Align to the start of the current hour
    current_hour = now.replace(minute=0, second=0, microsecond=0)

    # Fetch all logs in the 24h window once, then bucket in Python
    window_start = current_hour - timedelta(hours=24)
    rows = await db.execute(
        select(Log.created_at, Log.severity).where(
            and_(
                Log.tenant_id == tid,
                Log.service_name == service_name,
                Log.created_at >= window_start,
                Log.created_at < current_hour + timedelta(hours=1),
            )
        )
    )
    log_rows = rows.fetchall()

    # Build hourly bucket map: hour 23 (oldest) down to hour 0 (most recent complete hour)
    buckets_map: dict[datetime, dict] = {}
    for h in range(23, -1, -1):
        hour_start = current_hour - timedelta(hours=h)
        buckets_map[hour_start] = {"total": 0, "errors": 0}

    for created_at, severity in log_rows:
        if created_at.tzinfo is None:
            created_at = created_at.replace(tzinfo=timezone.utc)
        # Snap to hour boundary
        hour_bucket = created_at.replace(minute=0, second=0, microsecond=0)
        if hour_bucket in buckets_map:
            buckets_map[hour_bucket]["total"] += 1
            if severity in ("ERROR", "CRITICAL"):
                buckets_map[hour_bucket]["errors"] += 1

    timeline = [
        {
            "hour": hour_start.isoformat(),
            "total_logs": v["total"],
            "errors": v["errors"],
            "error_rate": round(v["errors"] / v["total"], 4) if v["total"] else 0.0,
        }
        for hour_start, v in sorted(buckets_map.items())
    ]

    result = {
        "service_name": service_name,
        "timeline": timeline,
    }
    await cache_set(cache_key, result, CACHE_TTL)
    return result
