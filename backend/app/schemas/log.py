from datetime import datetime
from enum import Enum
from typing import Any, Optional

from pydantic import BaseModel, field_validator, model_validator


class Severity(str, Enum):
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


class Environment(str, Enum):
    prod = "prod"
    staging = "staging"
    dev = "dev"


class LogIngestRequest(BaseModel):
    service_name: str
    severity: Severity
    message: str
    metadata: Optional[dict] = {}
    trace_id: Optional[str] = None
    span_id: Optional[str] = None
    environment: Environment = Environment.prod


class LogIngestResponse(BaseModel):
    event_id: str
    status: str = "accepted"


class BatchIngestRequest(BaseModel):
    events: list[LogIngestRequest]

    @field_validator("events")
    @classmethod
    def validate_batch_size(cls, v: list) -> list:
        if len(v) > 1000:
            raise ValueError("Batch size cannot exceed 1000 events")
        if len(v) == 0:
            raise ValueError("Batch must contain at least one event")
        return v


class BatchEventResult(BaseModel):
    index: int
    event_id: Optional[str] = None
    error: Optional[str] = None


class BatchIngestResponse(BaseModel):
    accepted: int
    failed: int
    errors: list[BatchEventResult]


class LogResponse(BaseModel):
    id: str
    tenant_id: str
    service_name: str
    severity: str
    message: str
    metadata: dict
    trace_id: Optional[str]
    span_id: Optional[str]
    source_ip: Optional[str]
    environment: str
    created_at: datetime
    ingested_at: datetime

    model_config = {"from_attributes": True}

    @field_validator("source_ip", mode="before")
    @classmethod
    def coerce_source_ip(cls, v: Any) -> Optional[str]:
        """Convert IPv4Address/IPv6Address objects returned by asyncpg to str."""
        if v is None:
            return None
        return str(v)


class LogListResponse(BaseModel):
    data: list[LogResponse]
    total: int
    limit: int
    offset: int
