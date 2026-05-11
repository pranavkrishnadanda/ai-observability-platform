"""
Baseline calculator: runs every 5 minutes, computes rolling 7-day baselines.
Stores results in Redis with 24-hour TTL.
"""
import asyncio
import logging
from datetime import datetime, timezone, timedelta

import redis.asyncio as aioredis
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy import select, func, and_

from app.core.config import settings
from app.models.logs import Log

logger = logging.getLogger(__name__)


async def calculate_volume_baseline(
    tenant_id: str, service_name: str, db: AsyncSession
) -> float:
    """Rolling average of 5-minute log counts over the last 7 days."""
    seven_days_ago = datetime.now(timezone.utc) - timedelta(days=7)

    # Count logs per 5-minute bucket over 7 days
    result = await db.execute(
        select(func.count(Log.id))
        .where(and_(
            Log.tenant_id == tenant_id,
            Log.service_name == service_name,
            Log.created_at >= seven_days_ago,
        ))
    )
    total_count = result.scalar() or 0
    # 7 days × 288 five-minute buckets/day = 2016 buckets
    return total_count / 2016


async def calculate_error_rate_baseline(
    tenant_id: str, service_name: str, db: AsyncSession
) -> float:
    """Rolling average error rate over the last 7 days."""
    seven_days_ago = datetime.now(timezone.utc) - timedelta(days=7)

    total_result = await db.execute(
        select(func.count(Log.id))
        .where(and_(
            Log.tenant_id == tenant_id,
            Log.service_name == service_name,
            Log.created_at >= seven_days_ago,
        ))
    )
    total = total_result.scalar() or 0
    if total == 0:
        return 0.0

    error_result = await db.execute(
        select(func.count(Log.id))
        .where(and_(
            Log.tenant_id == tenant_id,
            Log.service_name == service_name,
            Log.created_at >= seven_days_ago,
            Log.severity.in_(["ERROR", "CRITICAL"]),
        ))
    )
    errors = error_result.scalar() or 0
    return errors / total


async def update_baselines_for_tenant(
    tenant_id: str, redis: aioredis.Redis, db: AsyncSession
) -> None:
    """Calculate and cache baselines for all services of a tenant."""
    # Get services registered for this tenant
    services_key = f"tenant:{tenant_id}:services"
    services = await redis.smembers(services_key)

    for service in services:
        try:
            vol_baseline = await calculate_volume_baseline(tenant_id, service, db)
            err_baseline = await calculate_error_rate_baseline(tenant_id, service, db)

            pipe = redis.pipeline()
            pipe.setex(f"tenant:{tenant_id}:service:{service}:baseline:volume", 86400, str(vol_baseline))
            pipe.setex(f"tenant:{tenant_id}:service:{service}:baseline:error_rate", 86400, str(err_baseline))
            await pipe.execute()

            logger.debug(
                f"Updated baseline for {tenant_id}/{service}: "
                f"vol={vol_baseline:.1f}, err_rate={err_baseline:.4f}"
            )
        except Exception as e:
            logger.error(f"Baseline calculation failed for {tenant_id}/{service}: {e}")


async def run_baseline_updater() -> None:
    """Long-running background task — recalculates baselines every 5 minutes."""
    engine = create_async_engine(settings.DATABASE_URL, pool_size=5, max_overflow=10)
    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    redis = aioredis.from_url(settings.REDIS_URL, decode_responses=True)

    logger.info("Baseline calculator started")
    while True:
        try:
            async with session_factory() as db:
                # Get all active tenants from Redis service registries
                # Pattern: tenant:*:services
                all_keys = await redis.keys("tenant:*:services")
                tenant_ids: set[str] = set()
                for key in all_keys:
                    parts = key.split(":")
                    if len(parts) == 3 and parts[2] == "services":
                        tenant_ids.add(parts[1])

                for tenant_id in tenant_ids:
                    await update_baselines_for_tenant(tenant_id, redis, db)

            logger.info(f"Baseline update complete for {len(tenant_ids)} tenants")
        except Exception as e:
            logger.error(f"Baseline update cycle failed: {e}", exc_info=True)

        await asyncio.sleep(300)  # 5 minutes


async def calculate_baselines_for_all_tenants() -> None:
    """Single-shot baseline calculation for all tenants. Creates its own DB/Redis connections."""
    engine = create_async_engine(settings.DATABASE_URL, pool_size=5, max_overflow=10)
    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    redis = aioredis.from_url(settings.REDIS_URL, decode_responses=True)

    try:
        async with session_factory() as db:
            all_keys = await redis.keys("tenant:*:services")
            tenant_ids: set[str] = set()
            for key in all_keys:
                parts = key.split(":")
                if len(parts) == 3 and parts[2] == "services":
                    tenant_ids.add(parts[1])

            for tenant_id in tenant_ids:
                await update_baselines_for_tenant(tenant_id, redis, db)

        logger.info(f"Baseline update complete for {len(tenant_ids)} tenants")
    finally:
        await redis.aclose()
        await engine.dispose()


async def run_baseline_calculator_loop():
    """Recalculates 7-day rolling baselines every 5 minutes."""
    logger.info("Baseline calculator starting")
    while True:
        try:
            await calculate_baselines_for_all_tenants()
            logger.info("Baseline calculation complete")
        except asyncio.CancelledError:
            logger.info("Baseline calculator cancelled")
            break
        except Exception as e:
            logger.error(f"Baseline calculation error (continuing): {e}")
        await asyncio.sleep(300)


if __name__ == "__main__":
    asyncio.run(run_baseline_updater())
