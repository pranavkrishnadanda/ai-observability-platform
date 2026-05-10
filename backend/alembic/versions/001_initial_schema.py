"""Initial schema: tenants, logs, anomalies, alerts with indexes and extensions.

Revision ID: 001_initial
Revises:
Create Date: 2026-05-10 00:00:00.000000

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "001_initial"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Enable required extensions
    op.execute('CREATE EXTENSION IF NOT EXISTS "uuid-ossp"')
    op.execute('CREATE EXTENSION IF NOT EXISTS pg_trgm')
    op.execute('CREATE EXTENSION IF NOT EXISTS pgcrypto')

    # Create tenants table
    op.create_table(
        "tenants",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
            nullable=False,
        ),
        sa.Column("name", sa.VARCHAR(255), nullable=False),
        sa.Column("api_key_hash", sa.VARCHAR(255), nullable=False),
        sa.Column("plan_tier", sa.VARCHAR(50), nullable=False, server_default="free"),
        sa.Column(
            "rate_limit_per_minute", sa.Integer(), nullable=False, server_default="1000"
        ),
        sa.Column("webhook_url", sa.VARCHAR(500), nullable=True),
        sa.Column(
            "alert_thresholds",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default="{}",
        ),
        sa.Column("retention_days", sa.Integer(), nullable=False, server_default="90"),
        sa.Column(
            "created_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.text("NOW()"),
        ),
        sa.Column(
            "updated_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.text("NOW()"),
        ),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default="true"),
        sa.UniqueConstraint("name", name="uq_tenants_name"),
        sa.CheckConstraint(
            "plan_tier IN ('free','pro','enterprise')", name="ck_tenants_plan_tier"
        ),
        sa.CheckConstraint(
            "retention_days BETWEEN 1 AND 365", name="ck_tenants_retention_days"
        ),
    )

    # Partial index for active tenants
    op.create_index(
        "idx_tenants_active",
        "tenants",
        ["is_active"],
        postgresql_where=sa.text("is_active = TRUE"),
    )

    # Create logs table
    op.create_table(
        "logs",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
            nullable=False,
        ),
        sa.Column(
            "tenant_id", postgresql.UUID(as_uuid=True), nullable=False
        ),
        sa.Column("service_name", sa.VARCHAR(255), nullable=False),
        sa.Column("severity", sa.VARCHAR(20), nullable=False),
        sa.Column("message", sa.Text(), nullable=False),
        sa.Column(
            "metadata",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default="{}",
        ),
        sa.Column("trace_id", sa.VARCHAR(128), nullable=True),
        sa.Column("span_id", sa.VARCHAR(64), nullable=True),
        sa.Column("source_ip", postgresql.INET(), nullable=True),
        sa.Column(
            "environment", sa.VARCHAR(20), nullable=False, server_default="prod"
        ),
        sa.Column(
            "created_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.text("NOW()"),
        ),
        sa.Column(
            "ingested_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.text("NOW()"),
        ),
        sa.ForeignKeyConstraint(
            ["tenant_id"],
            ["tenants.id"],
            name="fk_logs_tenant_id",
            ondelete="CASCADE",
        ),
        sa.CheckConstraint(
            "severity IN ('DEBUG','INFO','WARNING','ERROR','CRITICAL')",
            name="ck_logs_severity",
        ),
        sa.CheckConstraint(
            "environment IN ('prod','staging','dev')", name="ck_logs_environment"
        ),
    )

    op.create_index(
        "idx_logs_tenant_created",
        "logs",
        ["tenant_id", sa.text("created_at DESC")],
    )
    op.create_index(
        "idx_logs_service_severity_created",
        "logs",
        ["service_name", "severity", sa.text("created_at DESC")],
    )
    op.create_index(
        "idx_logs_message_trgm",
        "logs",
        ["message"],
        postgresql_using="gin",
        postgresql_ops={"message": "gin_trgm_ops"},
    )
    op.execute("CREATE INDEX idx_logs_message_fts ON logs USING gin (to_tsvector('english', message))")
    op.create_index(
        "idx_logs_trace",
        "logs",
        ["trace_id"],
        postgresql_where=sa.text("trace_id IS NOT NULL"),
    )

    # Create anomalies table
    op.create_table(
        "anomalies",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
            nullable=False,
        ),
        sa.Column("tenant_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("service_name", sa.VARCHAR(255), nullable=False),
        sa.Column("anomaly_type", sa.VARCHAR(50), nullable=False),
        sa.Column("severity_score", sa.Numeric(3, 2), nullable=False),
        sa.Column(
            "detected_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.text("NOW()"),
        ),
        sa.Column("window_start", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("window_end", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("baseline_value", sa.Numeric(12, 2), nullable=True),
        sa.Column("observed_value", sa.Numeric(12, 2), nullable=True),
        sa.Column("deviation_pct", sa.Numeric(8, 2), nullable=True),
        sa.Column("claude_analysis", sa.Text(), nullable=True),
        sa.Column(
            "status", sa.VARCHAR(20), nullable=False, server_default="active"
        ),
        sa.Column("resolved_at", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column(
            "created_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.text("NOW()"),
        ),
        sa.ForeignKeyConstraint(
            ["tenant_id"],
            ["tenants.id"],
            name="fk_anomalies_tenant_id",
        ),
        sa.CheckConstraint(
            "anomaly_type IN ('volume_spike','volume_drop','new_error_pattern','error_rate_spike')",
            name="ck_anomalies_type",
        ),
        sa.CheckConstraint(
            "severity_score BETWEEN 0.00 AND 1.00",
            name="ck_anomalies_severity_score",
        ),
        sa.CheckConstraint(
            "status IN ('active','acknowledged','resolved')",
            name="ck_anomalies_status",
        ),
    )

    op.create_index(
        "idx_anomalies_tenant_detected",
        "anomalies",
        ["tenant_id", sa.text("detected_at DESC")],
    )
    op.create_index(
        "idx_anomalies_service_status",
        "anomalies",
        ["service_name", "status"],
    )
    op.create_index(
        "idx_anomalies_tenant_status_detected",
        "anomalies",
        ["tenant_id", "status", sa.text("detected_at DESC")],
    )

    # Create alerts table
    op.create_table(
        "alerts",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
            nullable=False,
        ),
        sa.Column("tenant_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("anomaly_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("alert_type", sa.VARCHAR(50), nullable=False),
        sa.Column("severity", sa.VARCHAR(20), nullable=False),
        sa.Column("title", sa.VARCHAR(255), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("webhook_url", sa.VARCHAR(500), nullable=False),
        sa.Column(
            "delivery_status",
            sa.VARCHAR(20),
            nullable=False,
            server_default="pending",
        ),
        sa.Column("delivered_at", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("dedup_key", sa.VARCHAR(255), nullable=False),
        sa.Column("retry_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column(
            "created_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.text("NOW()"),
        ),
        sa.ForeignKeyConstraint(
            ["tenant_id"],
            ["tenants.id"],
            name="fk_alerts_tenant_id",
        ),
        sa.ForeignKeyConstraint(
            ["anomaly_id"],
            ["anomalies.id"],
            name="fk_alerts_anomaly_id",
        ),
        sa.CheckConstraint(
            "severity IN ('low','medium','high','critical')",
            name="ck_alerts_severity",
        ),
        sa.CheckConstraint(
            "delivery_status IN ('pending','delivered','failed')",
            name="ck_alerts_delivery_status",
        ),
    )

    op.create_index(
        "uq_alerts_dedup_key",
        "alerts",
        ["dedup_key"],
        unique=True,
    )
    op.create_index(
        "idx_alerts_tenant_created",
        "alerts",
        ["tenant_id", sa.text("created_at DESC")],
    )
    op.create_index(
        "idx_alerts_status_created",
        "alerts",
        ["delivery_status", sa.text("created_at DESC")],
    )

    # Create updated_at trigger for tenants
    op.execute(
        """
        CREATE OR REPLACE FUNCTION set_updated_at() RETURNS trigger AS $$
        BEGIN NEW.updated_at = NOW(); RETURN NEW; END;
        $$ LANGUAGE plpgsql
        """
    )
    op.execute(
        """
        CREATE TRIGGER trg_tenants_updated
            BEFORE UPDATE ON tenants
            FOR EACH ROW EXECUTE FUNCTION set_updated_at()
        """
    )


def downgrade() -> None:
    op.execute("DROP TRIGGER IF EXISTS trg_tenants_updated ON tenants")
    op.execute("DROP FUNCTION IF EXISTS set_updated_at()")

    op.drop_index("idx_alerts_status_created", table_name="alerts")
    op.drop_index("idx_alerts_tenant_created", table_name="alerts")
    op.drop_index("uq_alerts_dedup_key", table_name="alerts")
    op.drop_table("alerts")

    op.drop_index("idx_anomalies_tenant_status_detected", table_name="anomalies")
    op.drop_index("idx_anomalies_service_status", table_name="anomalies")
    op.drop_index("idx_anomalies_tenant_detected", table_name="anomalies")
    op.drop_table("anomalies")

    op.drop_index("idx_logs_trace", table_name="logs")
    op.execute("DROP INDEX IF EXISTS idx_logs_message_fts")
    op.drop_index("idx_logs_message_trgm", table_name="logs")
    op.drop_index("idx_logs_service_severity_created", table_name="logs")
    op.drop_index("idx_logs_tenant_created", table_name="logs")
    op.drop_table("logs")

    op.drop_index("idx_tenants_active", table_name="tenants")
    op.drop_table("tenants")
