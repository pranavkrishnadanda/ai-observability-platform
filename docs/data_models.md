# Data Models

This document is the authoritative schema reference for PostgreSQL tables and Redis key patterns used by the AI Observability Platform.

---

## 1. PostgreSQL Tables

PostgreSQL extension `pg_trgm` must be enabled for trigram GIN indexes:
```sql
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE EXTENSION IF NOT EXISTS pgcrypto;
```

### 1.1 tenants

```sql
CREATE TABLE tenants (
    id                     UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name                   VARCHAR(255) NOT NULL UNIQUE,
    api_key_hash           VARCHAR(255) NOT NULL,
    plan_tier              VARCHAR(50)  NOT NULL DEFAULT 'free'
                           CHECK (plan_tier IN ('free','pro','enterprise')),
    rate_limit_per_minute  INTEGER      NOT NULL DEFAULT 1000,
    webhook_url            VARCHAR(500),
    alert_thresholds       JSONB        NOT NULL DEFAULT '{}'::jsonb,
    retention_days         INTEGER      NOT NULL DEFAULT 90
                           CHECK (retention_days BETWEEN 1 AND 365),
    created_at             TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at             TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    is_active              BOOLEAN      NOT NULL DEFAULT TRUE
);

CREATE INDEX idx_tenants_active ON tenants(is_active) WHERE is_active = TRUE;
```

| Column | Type | Notes |
|--------|------|-------|
| id | UUID | PK; `gen_random_uuid()`. |
| name | VARCHAR(255) | Unique. |
| api_key_hash | VARCHAR(255) | bcrypt hash, cost ≥ 12. |
| plan_tier | VARCHAR(50) | free/pro/enterprise. |
| rate_limit_per_minute | INTEGER | Default 1000. |
| webhook_url | VARCHAR(500) | Nullable; HTTPS URL. |
| alert_thresholds | JSONB | Default `{}`. |
| retention_days | INTEGER | 1–365, default 90. |
| created_at / updated_at | TIMESTAMPTZ | Maintained by trigger. |
| is_active | BOOLEAN | Soft-disable flag. |

### 1.2 logs

```sql
CREATE TABLE logs (
    id            UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id     UUID         NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    service_name  VARCHAR(255) NOT NULL,
    severity      VARCHAR(20)  NOT NULL
                  CHECK (severity IN ('DEBUG','INFO','WARNING','ERROR','CRITICAL')),
    message       TEXT         NOT NULL,
    metadata      JSONB        NOT NULL DEFAULT '{}'::jsonb,
    trace_id      VARCHAR(128),
    span_id       VARCHAR(64),
    source_ip     INET,
    environment   VARCHAR(20)  NOT NULL DEFAULT 'prod'
                  CHECK (environment IN ('prod','staging','dev')),
    created_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    ingested_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_logs_tenant_created
    ON logs (tenant_id, created_at DESC);
CREATE INDEX idx_logs_service_severity_created
    ON logs (service_name, severity, created_at DESC);
CREATE INDEX idx_logs_message_trgm
    ON logs USING GIN (message gin_trgm_ops);
CREATE INDEX idx_logs_trace
    ON logs (trace_id) WHERE trace_id IS NOT NULL;
```

| Column | Type | Notes |
|--------|------|-------|
| id | UUID | PK. |
| tenant_id | UUID | FK → tenants. ON DELETE CASCADE. |
| service_name | VARCHAR(255) | Logical service identifier. |
| severity | VARCHAR(20) | DEBUG/INFO/WARNING/ERROR/CRITICAL. |
| message | TEXT | Free-form log message. |
| metadata | JSONB | Default `{}`. |
| trace_id | VARCHAR(128) | Optional. |
| span_id | VARCHAR(64) | Optional. |
| source_ip | INET | Originating client IP. |
| environment | VARCHAR(20) | prod/staging/dev. |
| created_at | TIMESTAMPTZ | Event time. |
| ingested_at | TIMESTAMPTZ | Server-side ingestion time. |

**Partitioning note:** for production scale, `logs` may be range-partitioned by `created_at` daily. Schema above is unpartitioned for the initial implementation.

### 1.3 anomalies

```sql
CREATE TABLE anomalies (
    id              UUID          PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id       UUID          NOT NULL REFERENCES tenants(id),
    service_name    VARCHAR(255)  NOT NULL,
    anomaly_type    VARCHAR(50)   NOT NULL
                    CHECK (anomaly_type IN
                          ('volume_spike','volume_drop','new_error_pattern','error_rate_spike')),
    severity_score  NUMERIC(3,2)  NOT NULL
                    CHECK (severity_score BETWEEN 0.00 AND 1.00),
    detected_at     TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    window_start    TIMESTAMPTZ   NOT NULL,
    window_end      TIMESTAMPTZ   NOT NULL,
    baseline_value  NUMERIC(12,2),
    observed_value  NUMERIC(12,2),
    deviation_pct   NUMERIC(8,2),
    claude_analysis TEXT,
    status          VARCHAR(20)   NOT NULL DEFAULT 'active'
                    CHECK (status IN ('active','acknowledged','resolved')),
    resolved_at     TIMESTAMPTZ,
    created_at      TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_anomalies_tenant_detected
    ON anomalies (tenant_id, detected_at DESC);
CREATE INDEX idx_anomalies_service_status
    ON anomalies (service_name, status);
CREATE INDEX idx_anomalies_tenant_status_detected
    ON anomalies (tenant_id, status, detected_at DESC);
```

| Column | Type | Notes |
|--------|------|-------|
| id | UUID | PK. |
| tenant_id | UUID | FK → tenants. |
| service_name | VARCHAR(255) | Service producing the anomaly. |
| anomaly_type | VARCHAR(50) | One of 4 types. |
| severity_score | NUMERIC(3,2) | 0.00–1.00. |
| detected_at | TIMESTAMPTZ | When detector fired. |
| window_start / window_end | TIMESTAMPTZ | Analysis window bounds. |
| baseline_value | NUMERIC(12,2) | Historical baseline metric. |
| observed_value | NUMERIC(12,2) | Current observed metric. |
| deviation_pct | NUMERIC(8,2) | Percent deviation. |
| claude_analysis | TEXT | Set by claude-analyzer. |
| status | VARCHAR(20) | active/acknowledged/resolved. |
| resolved_at | TIMESTAMPTZ | Set on resolve. |
| created_at | TIMESTAMPTZ | Row insertion time. |

### 1.4 alerts

```sql
CREATE TABLE alerts (
    id              UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id       UUID         NOT NULL REFERENCES tenants(id),
    anomaly_id      UUID         NOT NULL REFERENCES anomalies(id),
    alert_type      VARCHAR(50)  NOT NULL,
    severity        VARCHAR(20)  NOT NULL
                    CHECK (severity IN ('low','medium','high','critical')),
    title           VARCHAR(255) NOT NULL,
    description     TEXT,
    webhook_url     VARCHAR(500) NOT NULL,
    delivery_status VARCHAR(20)  NOT NULL DEFAULT 'pending'
                    CHECK (delivery_status IN ('pending','delivered','failed')),
    delivered_at    TIMESTAMPTZ,
    dedup_key       VARCHAR(255) NOT NULL,
    retry_count     INTEGER      NOT NULL DEFAULT 0,
    last_error      TEXT,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX uq_alerts_dedup_key       ON alerts (dedup_key);
CREATE INDEX        idx_alerts_tenant_created ON alerts (tenant_id, created_at DESC);
CREATE INDEX        idx_alerts_status_created ON alerts (delivery_status, created_at DESC);
```

| Column | Type | Notes |
|--------|------|-------|
| id | UUID | PK. |
| tenant_id | UUID | FK → tenants. |
| anomaly_id | UUID | FK → anomalies. |
| alert_type | VARCHAR(50) | e.g. `anomaly`, `manual`. |
| severity | VARCHAR(20) | low/medium/high/critical. |
| title | VARCHAR(255) | Short summary. |
| description | TEXT | Full description (may include Claude analysis). |
| webhook_url | VARCHAR(500) | Snapshotted at creation. |
| delivery_status | VARCHAR(20) | pending/delivered/failed. |
| delivered_at | TIMESTAMPTZ | When webhook 2xx received. |
| dedup_key | VARCHAR(255) | UNIQUE; `sha256(tenant_id:service:type:epoch_hour)`. |
| retry_count | INTEGER | 0–3. |
| last_error | TEXT | Last delivery failure detail. |
| created_at | TIMESTAMPTZ | Row insertion time. |

### 1.5 Triggers

```sql
CREATE OR REPLACE FUNCTION set_updated_at() RETURNS trigger AS $$
BEGIN NEW.updated_at = NOW(); RETURN NEW; END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_tenants_updated
    BEFORE UPDATE ON tenants
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();
```

---

## 2. Redis Key Patterns

All Redis keys are namespaced by `tenant_id` to enforce isolation (NFR-6). Redis configuration:
```
maxmemory 512mb
maxmemory-policy allkeys-lru
```

### 2.1 Hot log storage
| Property | Value |
|----------|-------|
| Key | `tenant:{tenant_id}:service:{service_name}:logs` |
| Type | List |
| Operations | `LPUSH` then `LTRIM 0 9999` |
| TTL | 3600 s |
| Value | JSON-encoded log object (matches API log shape) |

### 2.2 Volume counter (5-minute window)
| Property | Value |
|----------|-------|
| Key | `tenant:{tenant_id}:service:{service_name}:vol:{epoch_5min}` |
| Type | String (counter) |
| Operations | `INCR`, `EXPIRE` |
| TTL | 86400 s (24 h) |
| Value | Integer count of all logs in the bucket |

`{epoch_5min}` = `floor(unix_seconds / 300)`.

### 2.3 Error counter (5-minute window)
| Property | Value |
|----------|-------|
| Key | `tenant:{tenant_id}:service:{service_name}:errors:{epoch_5min}` |
| Type | String (counter) |
| Operations | `INCR`, `EXPIRE` |
| TTL | 86400 s |
| Value | Integer count of `ERROR`+`CRITICAL` logs in bucket |

### 2.4 Baseline metrics — volume
| Property | Value |
|----------|-------|
| Key | `tenant:{tenant_id}:service:{service_name}:baseline:volume` |
| Type | String |
| TTL | 86400 s |
| Value | JSON `{ "mean": float, "stddev": float, "n": int, "updated_at": iso8601 }` |

### 2.5 Baseline metrics — error rate
| Property | Value |
|----------|-------|
| Key | `tenant:{tenant_id}:service:{service_name}:baseline:error_rate` |
| Type | String |
| TTL | 86400 s |
| Value | JSON `{ "mean": float, "stddev": float, "n": int, "updated_at": iso8601 }` |

### 2.6 Known error patterns
| Property | Value |
|----------|-------|
| Key | `tenant:{tenant_id}:service:{service_name}:error_patterns` |
| Type | Set |
| Operations | `SADD`, `SISMEMBER`, `EXPIRE` |
| TTL | 86400 s |
| Value | Normalized error template strings (e.g. `TimeoutError: <REDACTED> at <REDACTED>`) |

Templates are produced by stripping numeric IDs, UUIDs, and IPs.

### 2.7 Alert deduplication
| Property | Value |
|----------|-------|
| Key | `alerts:dedup:{tenant_id}` |
| Type | Set |
| Operations | `SADD`, `SISMEMBER`, `EXPIRE` |
| TTL | 3600 s |
| Value | `dedup_key` strings (`sha256(...)` hex) |

### 2.8 Alert rate limit (per tenant)
| Property | Value |
|----------|-------|
| Key | `alerts:rate:{tenant_id}` |
| Type | String (counter) |
| Operations | `INCR`, `EXPIRE` |
| TTL | 3600 s |
| Cap | 10 per hour (NFR-8) |

### 2.9 API rate limit (per tenant per minute)
| Property | Value |
|----------|-------|
| Key | `rate:{tenant_id}:{window_minute}` |
| Type | String (counter) |
| Operations | `INCR`, `EXPIRE` |
| TTL | 120 s |
| Cap | `tenants.rate_limit_per_minute` |

`{window_minute}` = `floor(unix_seconds / 60)`.

### 2.10 WebSocket — log streaming (Pub/Sub)
| Property | Value |
|----------|-------|
| Channel | `tenant:{tenant_id}:service:{service_name}:stream` |
| Type | Pub/Sub |
| TTL | none (ephemeral) |
| Payload | JSON-encoded log object |

### 2.11 WebSocket — anomaly streaming (Pub/Sub)
| Property | Value |
|----------|-------|
| Channel | `tenant:{tenant_id}:anomalies:stream` |
| Type | Pub/Sub |
| TTL | none |
| Payload | JSON-encoded anomaly object |

### 2.12 Analysis credit tracking
| Property | Value |
|----------|-------|
| Key | `tenant:{tenant_id}:analysis_credits:{epoch_day}` |
| Type | String (counter) |
| Operations | `INCR`, `EXPIRE` |
| TTL | 172800 s (2 days) |
| Value | Integer count of analyses used today |

`{epoch_day}` = `floor(unix_seconds / 86400)`.

### 2.13 Service registry
| Property | Value |
|----------|-------|
| Key | `tenant:{tenant_id}:services` |
| Type | Set |
| Operations | `SADD` (on each ingest), `SMEMBERS` |
| TTL | none (persistent) |
| Value | `service_name` strings |

---

## 3. Foreign Key & Referential Integrity Summary

```
tenants 1 ──< logs       (ON DELETE CASCADE)
tenants 1 ──< anomalies  (ON DELETE RESTRICT — preserve audit trail)
tenants 1 ──< alerts     (ON DELETE RESTRICT)
anomalies 1 ──< alerts   (ON DELETE RESTRICT)
```

`anomalies` and `alerts` use `RESTRICT` rather than `CASCADE` to preserve audit history. Tenant deletion in production should be a soft delete (`is_active = FALSE`) followed by an out-of-band purge.

---

## 4. Storage Budget & Retention

| Layer | Retention | Mechanism |
|-------|-----------|-----------|
| Redis hot logs | 1 hour | TTL + `LTRIM` |
| Redis counters | 24 hours | TTL |
| Redis baselines | 24 hours | TTL, refreshed |
| Redis dedup | 1 hour | TTL |
| PostgreSQL `logs` | tenant `retention_days` (default 90) | daily reaper job |
| PostgreSQL `anomalies` / `alerts` | indefinite (audit) | manual purge |
