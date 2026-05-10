import uuid
from datetime import datetime
from typing import Optional

from sqlalchemy import CheckConstraint, DateTime, Index, String, Text, func
from sqlalchemy.dialects.postgresql import JSON, UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.models.base import Base


class Log(Base):
    __tablename__ = "logs"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    tenant_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), nullable=False, index=True
    )
    service_name: Mapped[str] = mapped_column(String(255), nullable=False)
    severity: Mapped[str] = mapped_column(String(20), nullable=False)
    message: Mapped[str] = mapped_column(Text, nullable=False)
    # "metadata" is a reserved attribute on SQLAlchemy Base; map to the column via alias.
    log_metadata: Mapped[dict] = mapped_column("metadata", JSON, nullable=False, default=dict)
    trace_id: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    span_id: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    source_ip: Mapped[Optional[str]] = mapped_column(String(45), nullable=True)
    environment: Mapped[str] = mapped_column(String(20), nullable=False, default="prod")
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    ingested_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    __table_args__ = (
        CheckConstraint(
            "severity IN ('DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL')",
            name="ck_logs_severity",
        ),
        CheckConstraint(
            "environment IN ('prod', 'staging', 'dev')",
            name="ck_logs_environment",
        ),
        Index("idx_logs_tenant_created", "tenant_id", "created_at"),
        Index("idx_logs_service_severity", "service_name", "severity", "created_at"),
    )
