import dataclasses
import secrets
from typing import Optional

from fastapi import Depends, Header, HTTPException, status
from passlib.context import CryptContext
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.core.database import get_db
from app.models.tenants import Tenant

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


@dataclasses.dataclass
class TenantContext:
    tenant_id: str
    name: str
    plan_tier: str
    rate_limit_per_minute: int
    webhook_url: Optional[str]


def generate_api_key() -> tuple[str, str]:
    """Returns (raw_key, hashed_key). raw_key is shown once to the user."""
    raw = f"{settings.API_KEY_PREFIX}{secrets.token_urlsafe(32)}"
    hashed = pwd_context.hash(raw)
    return raw, hashed


def verify_api_key(raw_key: str, hashed_key: str) -> bool:
    """Constant-time bcrypt verification."""
    return pwd_context.verify(raw_key, hashed_key)


async def get_current_tenant(
    x_api_key: str = Header(..., alias="X-API-Key"),
    db: AsyncSession = Depends(get_db),
) -> TenantContext:
    if not x_api_key:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="API key required")

    # Cache tenant list to avoid per-request full table scan
    from app.core.redis_client import cache_get, cache_set
    import json

    cached = await cache_get("auth:tenant_list")
    if cached is not None:
        tenants_data = cached
    else:
        result = await db.execute(select(Tenant).where(Tenant.is_active == True))
        tenants_raw = result.scalars().all()
        tenants_data = [
            {
                "id": str(t.id),
                "name": t.name,
                "api_key_hash": t.api_key_hash,
                "plan_tier": t.plan_tier,
                "rate_limit_per_minute": t.rate_limit_per_minute,
                "webhook_url": t.webhook_url,
            }
            for t in tenants_raw
        ]
        await cache_set("auth:tenant_list", tenants_data, ttl=30)

    for tenant in tenants_data:
        if verify_api_key(x_api_key, tenant["api_key_hash"]):
            return TenantContext(
                tenant_id=tenant["id"],
                name=tenant["name"],
                plan_tier=tenant["plan_tier"],
                rate_limit_per_minute=tenant["rate_limit_per_minute"],
                webhook_url=tenant["webhook_url"],
            )

    raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid API key")


async def invalidate_tenant_cache() -> None:
    from app.core.redis_client import cache_invalidate
    await cache_invalidate("auth:tenant_list")
