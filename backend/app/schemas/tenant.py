from datetime import datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel


class PlanTier(str, Enum):
    free = "free"
    pro = "pro"
    enterprise = "enterprise"


class TenantRegisterRequest(BaseModel):
    name: str
    plan_tier: PlanTier = PlanTier.free


class TenantRegisterResponse(BaseModel):
    tenant_id: str
    api_key: str  # shown only once at registration
    created_at: datetime


class TenantSettingsResponse(BaseModel):
    tenant_id: str
    name: str
    plan_tier: str
    rate_limit_per_minute: int
    webhook_url: Optional[str]
    alert_thresholds: dict
    retention_days: int
    is_active: bool
    created_at: datetime
    updated_at: datetime


class TenantSettingsUpdate(BaseModel):
    webhook_url: Optional[str] = None
    alert_thresholds: Optional[dict] = None
    retention_days: Optional[int] = None
    rate_limit_per_minute: Optional[int] = None


class RotateKeyResponse(BaseModel):
    api_key: str
    rotated_at: datetime
