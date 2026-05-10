from datetime import datetime
from typing import Optional

from pydantic import BaseModel


class AnomalyResponse(BaseModel):
    id: str
    tenant_id: str
    service_name: str
    anomaly_type: str
    severity_score: float
    detected_at: datetime
    window_start: datetime
    window_end: datetime
    baseline_value: Optional[float]
    observed_value: Optional[float]
    deviation_pct: Optional[float]
    claude_analysis: Optional[str]
    status: str
    resolved_at: Optional[datetime]
    created_at: datetime


class AnomalyListResponse(BaseModel):
    data: list[AnomalyResponse]
    total: int
    limit: int
    offset: int
