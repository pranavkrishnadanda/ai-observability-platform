from datetime import datetime
from typing import Optional

from pydantic import BaseModel


class AlertResponse(BaseModel):
    id: str
    tenant_id: str
    anomaly_id: str
    alert_type: str
    severity: str
    title: str
    description: Optional[str]
    webhook_url: str
    delivery_status: str
    delivered_at: Optional[datetime]
    dedup_key: str
    retry_count: int
    last_error: Optional[str]
    created_at: datetime


class AlertListResponse(BaseModel):
    data: list[AlertResponse]
    total: int
    limit: int
    offset: int


class AlertStatsResponse(BaseModel):
    total_today: int
    by_severity: dict[str, int]
    delivery_rate: float
    top_alerting_services: list[dict]


class WebhookTestResponse(BaseModel):
    delivered: bool
    status_code: int
    duration_ms: float
    signature: str
