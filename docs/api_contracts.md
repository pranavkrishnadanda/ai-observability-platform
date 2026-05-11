# API Contracts

All endpoints are versioned under `/api/v1`. Authenticated endpoints require header `X-API-Key: <plaintext-api-key>`. All request and response bodies are JSON unless otherwise noted.

## Common Error Responses

| HTTP | Body | Used When |
|------|------|-----------|
| 401 | `{"error": "unauthorized", "message": "..."}` | Missing/invalid API key, inactive tenant. |
| 422 | `{"error": "validation_error", "details": [{"field": "...", "message": "..."}]}` | Schema/constraint violation. |
| 429 | `{"error": "rate_limited", "retry_after_seconds": N}` | Tenant rate limit exceeded. |
| 503 | `{"error": "dependency_unavailable", "component": "redis|postgres|kafka"}` | Backend dependency not reachable. |

---

## 1. Authentication & Tenant

### POST /api/v1/auth/register
Public — no auth required.

**Request body:**
```json
{
  "name": "Acme Corp",
  "plan_tier": "pro"
}
```
| Field | Type | Required | Constraints |
|-------|------|----------|-------------|
| name | string | yes | 1–255 chars, unique |
| plan_tier | string | yes | one of `free`, `pro`, `enterprise` |

**Response 201:**
```json
{
  "tenant_id": "8b1e0c3a-5b41-4a4f-9b0c-1f2a3b4c5d6e",
  "api_key": "aobs_live_a1B2c3D4e5F6g7H8i9J0kLmNoPqRsTuV",
  "created_at": "2026-05-10T12:00:00Z"
}
```
**Errors:** 409 (duplicate `name`), 422 (invalid `plan_tier`).

---

### POST /api/v1/auth/rotate-key
Auth: `X-API-Key`.

**Request body:** none.

**Response 200:**
```json
{
  "api_key": "aobs_live_newKey...",
  "rotated_at": "2026-05-10T12:05:00Z"
}
```

---

### GET /api/v1/tenant/settings
Auth: `X-API-Key`.

**Response 200:**
```json
{
  "tenant_id": "8b1e0c3a-...",
  "name": "Acme Corp",
  "plan_tier": "pro",
  "rate_limit_per_minute": 1000,
  "webhook_url": "https://hooks.acme.com/aobs",
  "alert_thresholds": {"volume_spike_stddev": 3.0, "error_rate_spike_stddev": 3.0},
  "retention_days": 90,
  "is_active": true,
  "created_at": "2026-05-10T12:00:00Z",
  "updated_at": "2026-05-10T12:00:00Z"
}
```

---

### PATCH /api/v1/tenant/settings
Auth: `X-API-Key`.

**Request body:** any subset of:
```json
{
  "webhook_url": "https://hooks.acme.com/aobs",
  "alert_thresholds": {"volume_spike_stddev": 3.0},
  "retention_days": 30
}
```
| Field | Type | Constraints |
|-------|------|-------------|
| webhook_url | string | HTTPS URL, ≤ 500 chars |
| alert_thresholds | object | matches threshold schema |
| retention_days | integer | 1–365 |

**Response 200:** updated settings (same shape as GET).

---

## 2. Log Ingestion

### POST /api/v1/logs/ingest
Auth: `X-API-Key`. Target p99 latency < 10 ms.

**Request body:**
```json
{
  "service_name": "checkout-api",
  "severity": "ERROR",
  "message": "Failed to charge card: gateway timeout",
  "metadata": {"user_id": "u_123", "order_id": "ord_456"},
  "trace_id": "9b2f3c4d5e6a7b8c9d0e1f2a3b4c5d6e",
  "span_id": "1a2b3c4d5e6f7a8b",
  "environment": "prod"
}
```
| Field | Type | Required | Constraints |
|-------|------|----------|-------------|
| service_name | string | yes | 1–255 chars |
| severity | string | yes | DEBUG/INFO/WARNING/ERROR/CRITICAL |
| message | text | yes | 1–8192 chars |
| metadata | object | no | JSON object, ≤ 8 KB serialized |
| trace_id | string | no | hex, ≤ 128 chars |
| span_id | string | no | hex, ≤ 64 chars |
| environment | string | no | prod/staging/dev, default `prod` |

**Response 202:**
```json
{ "event_id": "f7a1c2b3-d4e5-6789-0123-456789abcdef", "status": "accepted" }
```
**Errors:** 401, 422, 429, 503.

---

### POST /api/v1/logs/ingest/batch
Auth: `X-API-Key`. Up to 1000 events per call.

**Request body:**
```json
{
  "events": [
    { "service_name": "api", "severity": "INFO", "message": "request handled" },
    { "service_name": "api", "severity": "ERROR", "message": "db error" }
  ]
}
```
| Field | Type | Required | Constraints |
|-------|------|----------|-------------|
| events | array | yes | 1 ≤ length ≤ 1000 |

Each event uses the schema from `/logs/ingest`.

**Response 202:**
```json
{
  "accepted": 998,
  "failed": 2,
  "errors": [
    {"index": 17, "message": "severity must be one of DEBUG/INFO/WARNING/ERROR/CRITICAL"},
    {"index": 942, "message": "service_name is required"}
  ]
}
```
**Errors:** 401, 422 (entire batch rejected when `events` array invalid), 429, 503.

---

## 3. Log Query

### GET /api/v1/logs
Auth: `X-API-Key`. Target p99 latency < 500 ms.

**Query params:**
| Param | Type | Default | Constraints |
|-------|------|---------|-------------|
| service | string | — | optional filter |
| severity | string | — | DEBUG/INFO/WARNING/ERROR/CRITICAL |
| from_time | ISO 8601 | now − 1h | inclusive |
| to_time | ISO 8601 | now | inclusive, ≥ from_time |
| search_text | string | — | free-text search on message (pg_trgm) |
| trace_id | string | — | exact match |
| limit | integer | 100 | 1–1000 |
| offset | integer | 0 | ≥ 0 |

**Response 200:**
```json
{
  "data": [
    {
      "id": "f7a1c2b3-...",
      "service_name": "checkout-api",
      "severity": "ERROR",
      "message": "Failed to charge card",
      "metadata": {"user_id": "u_123"},
      "trace_id": "9b2f3c4d...",
      "span_id": "1a2b3c4d...",
      "environment": "prod",
      "created_at": "2026-05-10T11:59:50Z",
      "ingested_at": "2026-05-10T11:59:50Z"
    }
  ],
  "total": 1247,
  "page_info": { "limit": 100, "offset": 0, "has_next": true }
}
```

---

### GET /api/v1/logs/{log_id}
Auth: `X-API-Key`.

**Response 200:** single log object (same shape as `data[]` element above).
**Errors:** 401, 404.

---

### GET /api/v1/metrics/services
Auth: `X-API-Key`.

**Response 200:**
```json
{
  "services": [
    {
      "service_name": "checkout-api",
      "volume_5m": 4321,
      "error_rate_5m": 0.012,
      "last_seen_at": "2026-05-10T12:04:30Z"
    }
  ]
}
```

---

## 4. Anomalies

### GET /api/v1/anomalies
Auth: `X-API-Key`.

**Query params:**
| Param | Type | Default | Constraints |
|-------|------|---------|-------------|
| service | string | — | filter |
| status | string | — | active/acknowledged/resolved |
| from_time | ISO 8601 | now − 24h | |
| severity_min | float | 0.0 | 0.0–1.0 |
| limit | integer | 100 | 1–1000 |
| offset | integer | 0 | ≥ 0 |

**Response 200:** flat array (not paginated wrapper)
```json
[
  {
    "id": "...",
    "tenant_id": "8b1e0c3a-...",
    "service_name": "checkout-api",
    "anomaly_type": "error_rate_spike",
    "severity_score": 0.82,
    "detected_at": "2026-05-10T12:00:00Z",
    "window_start": "2026-05-10T11:55:00Z",
    "window_end": "2026-05-10T12:00:00Z",
    "baseline_value": 0.01,
    "observed_value": 0.18,
    "deviation_pct": 1700.00,
    "claude_analysis": "Spike correlated with deploy v1.42.0 ...",
    "status": "active",
    "resolved_at": null,
    "created_at": "2026-05-10T12:00:01Z"
  }
]
```

---

### GET /api/v1/anomalies/{id}
Auth: `X-API-Key`. Returns single anomaly object.

---

### GET /api/v1/anomalies/{id}/logs
Auth: `X-API-Key`. Returns logs whose `created_at ∈ [window_start, window_end]` for the same service.

**Response 200:**
```json
{
  "data": [ /* log objects */ ],
  "total": 215,
  "page_info": {"limit": 100, "offset": 0, "has_next": true}
}
```

---

### PATCH /api/v1/anomalies/{id}/acknowledge
Auth: `X-API-Key`. Body: empty.

**Response 200:** updated anomaly.
**Errors:** 401, 404, 409 (already resolved).

---

### PATCH /api/v1/anomalies/{id}/resolve
Auth: `X-API-Key`. Body: empty.

**Response 200:** updated anomaly with `status="resolved"`, `resolved_at` set.

---

## 5. Alerts

### GET /api/v1/alerts
Auth: `X-API-Key`.

**Query params:**
| Param | Type | Constraints |
|-------|------|-------------|
| service | string | optional |
| severity | string | low/medium/high/critical |
| status | string | pending/delivered/failed |
| from_time | ISO 8601 | default now − 24h |
| limit | integer | 1–1000, default 100 |
| offset | integer | ≥ 0 |

**Response 200:**
```json
{
  "data": [
    {
      "id": "...",
      "anomaly_id": "...",
      "alert_type": "anomaly",
      "severity": "high",
      "title": "Error rate spike on checkout-api",
      "description": "Error rate jumped from 1% to 18% over the last 5 minutes.",
      "webhook_url": "https://hooks.acme.com/aobs",
      "delivery_status": "delivered",
      "delivered_at": "2026-05-10T12:00:05Z",
      "dedup_key": "sha256:...",
      "retry_count": 0,
      "last_error": null,
      "created_at": "2026-05-10T12:00:02Z"
    }
  ],
  "total": 3,
  "page_info": {"limit": 100, "offset": 0, "has_next": false}
}
```

---

### GET /api/v1/alerts/{id}
Auth: `X-API-Key`. Returns single alert.

---

### GET /api/v1/alerts/stats
Auth: `X-API-Key`.

**Response 200:**
```json
{
  "total_today": 42,
  "by_severity": { "low": 10, "medium": 18, "high": 12, "critical": 2 },
  "delivery_rate": 0.976,
  "top_alerting_services": [
    { "service_name": "checkout-api", "count": 15 },
    { "service_name": "auth-svc", "count": 9 }
  ]
}
```

---

### POST /api/v1/webhooks/test
Auth: `X-API-Key`. Body: empty. Sends a synthetic alert payload to the tenant's configured webhook.

**Response 200:**
```json
{
  "delivered": true,
  "status_code": 200,
  "duration_ms": 134,
  "signature": "sha256=abcdef..."
}
```

---

## 6. Analytics

### GET /api/v1/analytics/overview
Auth: `X-API-Key`.

**Response 200:**
```json
{
  "window_hours": 24,
  "total_logs": 5237412,
  "total_logs_today": 1842310,
  "total_logs_yesterday": 1654200,
  "total_logs_week": 9123400,
  "error_logs": 18923,
  "anomalies_detected": 12,
  "alerts_sent": 9,
  "alerts_sent_today": 9,
  "active_services": 14,
  "error_rate_today": 0.0102,
  "error_rate_yesterday": 0.0093,
  "active_anomalies": 3,
  "top_5_error_services": [
    { "service": "checkout-api", "error_count": 4321 }
  ],
  "system_health_score": 70
}
```

---

### GET /api/v1/analytics/services
Auth: `X-API-Key`. Returns per-service rollups sorted by 24h volume desc.

**Response 200:** flat array
```json
[
  {
    "service_name": "checkout-api",
    "health_status": "degraded",
    "log_volume_1h": 84321,
    "log_volume_24h": 1284321,
    "error_rate_1h": 0.012,
    "error_rate_24h": 0.0034,
    "anomaly_count_7d": 2,
    "last_seen": "2026-05-10T12:04:30Z"
  }
]

---

### GET /api/v1/analytics/services/{name}/timeline
Auth: `X-API-Key`. Returns hourly bucketed time series for the last 24 hours.

**Response 200:**
```json
{
  "service_name": "checkout-api",
  "timeline": [
    { "hour": "2026-05-09T12:00:00Z", "total_logs": 4123, "errors": 12, "error_rate": 0.0029 },
    { "hour": "2026-05-09T13:00:00Z", "total_logs": 4002, "errors": 11, "error_rate": 0.0027 }
  ]
}
```

---

## 7. Analysis

### POST /api/v1/analysis/root-cause
Auth: `X-API-Key`. Calls Claude Haiku.

**Request body:**
```json
{
  "service": "checkout-api",
  "from_time": "2026-05-10T11:00:00Z",
  "to_time":   "2026-05-10T12:00:00Z"
}
```
| Field | Constraints |
|-------|-------------|
| service | required |
| from_time, to_time | window ≤ 24h, to_time ≥ from_time |

**Response 200:**
```json
{
  "summary": "Errors began at 11:42 UTC after deploy v1.42.0 ...",
  "suspected_causes": [
    "Deploy v1.42.0 introduced a regression in the payment client.",
    "Upstream payment gateway latency increased 3x at 11:40 UTC."
  ],
  "evidence_log_ids": ["f7a1c2b3-...", "abcd1234-..."],
  "tokens_used": 1842
}
```

---

### POST /api/v1/analysis/compare
Auth: `X-API-Key`.

**Request body:**
```json
{
  "service": "checkout-api",
  "period1_start": "2026-05-09T11:00:00Z",
  "period2_start": "2026-05-10T11:00:00Z",
  "hours": 1
}
```
| Field | Constraints |
|-------|-------------|
| service | required |
| period1_start, period2_start | ISO 8601 |
| hours | 1–24 |

**Response 200:**
```json
{
  "service": "checkout-api",
  "hours": 1,
  "period1": { "volume": 240000, "errors": 240, "error_rate": 0.001 },
  "period2": { "volume": 245000, "errors": 4321, "error_rate": 0.0176 },
  "delta":   { "volume_pct": 2.08, "error_rate_pct": 1660.0 },
  "top_new_errors": [ "TimeoutError: payments-gateway" ],
  "claude_commentary": "Period 2 shows a 17x error-rate increase ..."
}
```

---

## 8. WebSocket

### WS /ws/logs/{service_name}?token=<api_key>
Server-pushed JSON messages, one log per frame:
```json
{
  "type": "log",
  "data": { /* log object */ }
}
```
- Heartbeat: `{"type": "ping"}` every 30 s; client must reply with `{"type": "pong"}`.
- Disconnect codes: `4401` (unauthorized), `1013` (backpressure overflow), `1011` (server error).

### WS /ws/anomalies?token=<api_key>
Server-pushed JSON messages:
```json
{
  "type": "anomaly",
  "data": { /* anomaly object */ }
}
```

---

## 9. Health

### GET /health
Public — no auth.

**Response 200 (healthy):**
```json
{
  "status": "healthy",
  "components": { "postgres": "up", "redis": "up", "kafka": "up" },
  "timestamp": "2026-05-10T12:00:00Z"
}
```
**Response 503 (unhealthy):** same shape with `status` set to `degraded` or `unhealthy` and component values `up | down`.
