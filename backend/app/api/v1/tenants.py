import uuid as uuid_mod
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.auth import (
    TenantContext,
    generate_api_key,
    get_current_tenant,
    invalidate_tenant_cache,
)
from app.core.database import get_db
from app.models.tenants import Tenant
from app.schemas.tenant import (
    TenantRegisterRequest,
    TenantRegisterResponse,
    TenantSettingsResponse,
    TenantSettingsUpdate,
)

router = APIRouter()


@router.post(
    "/auth/register",
    response_model=TenantRegisterResponse,
    status_code=status.HTTP_201_CREATED,
)
async def register_tenant(
    request: TenantRegisterRequest,
    db: AsyncSession = Depends(get_db),
) -> TenantRegisterResponse:
    """Register a new tenant. Public — no auth required."""
    # Check for duplicate name
    existing = await db.execute(
        select(Tenant).where(Tenant.name == request.name)
    )
    if existing.scalar_one_or_none():
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Tenant name already exists",
        )

    raw_key, hashed_key = generate_api_key()

    tenant = Tenant(
        name=request.name,
        api_key_hash=hashed_key,
        plan_tier=request.plan_tier.value,
        rate_limit_per_minute=1000,
        is_active=True,
    )
    db.add(tenant)
    await db.commit()
    await db.refresh(tenant)

    return TenantRegisterResponse(
        tenant_id=str(tenant.id),
        api_key=raw_key,  # Shown only once
        created_at=tenant.created_at,
    )


@router.post("/auth/rotate-key")
async def rotate_api_key(
    tenant: TenantContext = Depends(get_current_tenant),
    db: AsyncSession = Depends(get_db),
):
    """Rotate the API key for the authenticated tenant."""
    result = await db.execute(
        select(Tenant).where(Tenant.id == uuid_mod.UUID(tenant.tenant_id))
    )
    db_tenant = result.scalar_one_or_none()
    if not db_tenant:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Tenant not found"
        )

    raw_key, hashed_key = generate_api_key()
    db_tenant.api_key_hash = hashed_key
    await db.commit()

    # Invalidate auth cache so old key is rejected immediately
    await invalidate_tenant_cache()

    return {
        "api_key": raw_key,
        "rotated_at": datetime.now(timezone.utc).isoformat(),
    }


@router.get("/tenant/settings", response_model=TenantSettingsResponse)
async def get_settings(
    tenant: TenantContext = Depends(get_current_tenant),
    db: AsyncSession = Depends(get_db),
) -> TenantSettingsResponse:
    """Get settings for the authenticated tenant."""
    result = await db.execute(
        select(Tenant).where(Tenant.id == uuid_mod.UUID(tenant.tenant_id))
    )
    db_tenant = result.scalar_one_or_none()
    if not db_tenant:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Tenant not found"
        )

    return TenantSettingsResponse(
        tenant_id=str(db_tenant.id),
        name=db_tenant.name,
        plan_tier=db_tenant.plan_tier,
        rate_limit_per_minute=db_tenant.rate_limit_per_minute,
        webhook_url=db_tenant.webhook_url,
        alert_thresholds=db_tenant.alert_thresholds or {},
        retention_days=db_tenant.retention_days,
        is_active=db_tenant.is_active,
        created_at=db_tenant.created_at,
        updated_at=db_tenant.updated_at,
    )


@router.patch("/tenant/settings")
async def update_settings(
    update: TenantSettingsUpdate,
    tenant: TenantContext = Depends(get_current_tenant),
    db: AsyncSession = Depends(get_db),
):
    """Update mutable settings for the authenticated tenant."""
    result = await db.execute(
        select(Tenant).where(Tenant.id == uuid_mod.UUID(tenant.tenant_id))
    )
    db_tenant = result.scalar_one_or_none()
    if not db_tenant:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Tenant not found"
        )

    if update.webhook_url is not None:
        db_tenant.webhook_url = update.webhook_url
    if update.alert_thresholds is not None:
        db_tenant.alert_thresholds = update.alert_thresholds
    if update.retention_days is not None:
        if not 1 <= update.retention_days <= 365:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="retention_days must be between 1 and 365",
            )
        db_tenant.retention_days = update.retention_days
    if update.rate_limit_per_minute is not None:
        db_tenant.rate_limit_per_minute = update.rate_limit_per_minute

    await db.commit()
    await db.refresh(db_tenant)

    # Invalidate auth cache so updated settings are reflected immediately
    await invalidate_tenant_cache()

    return TenantSettingsResponse(
        tenant_id=str(db_tenant.id),
        name=db_tenant.name,
        plan_tier=db_tenant.plan_tier,
        rate_limit_per_minute=db_tenant.rate_limit_per_minute,
        webhook_url=db_tenant.webhook_url,
        alert_thresholds=db_tenant.alert_thresholds or {},
        retention_days=db_tenant.retention_days,
        is_active=db_tenant.is_active,
        created_at=db_tenant.created_at,
        updated_at=db_tenant.updated_at,
    )
