# AI Observability Platform — Requirements

## 1. Overview

The AI Observability Platform is a multi-tenant SaaS API that ingests application logs from customer applications, persists them across hot (Redis) and cold (PostgreSQL) tiers, detects anomalies via statistical baselines and Claude Haiku AI, and delivers alerts via webhooks. A React dashboard provides real-time log streaming and analytics.

This document defines the functional and non-functional requirements that govern the system. All subsequent design and implementation work must satisfy these requirements.

---

## 2. Functional Requirements

### FR-1: Tenant Registration
**Title:** Tenant self-service registration
**Description:** A new tenant (company) can register via `POST /api/v1/auth/register`, providing a tenant `name` and `plan_tier`. The system creates a tenant record, generates a unique API key, hashes the key with bcrypt, and returns the plaintext API key exactly once.
**Acceptance Criteria:**
- AC-1.1: Endpoint returns HTTP 201 with `{tenant_id, api_key, created_at}` on success.
- AC-1.2: API key is at least 32 characters of cryptographically random data.
- AC-1.3: Plaintext key is never persisted; only bcrypt hash is stored in `tenants.api_key_hash`.
- AC-1.4: `name` must be unique across all tenants; duplicate returns HTTP 409.
- AC-1.5: `plan_tier` is one of `free`, `pro`, `enterprise`; invalid value returns HTTP 422.
- AC-1.6: Default `rate_limit_per_minute = 1000`, `retention_days = 90`, `is_active = TRUE`.

### FR-2: API Key Management
**Title:** API key rotation and authentication
**Description:** Tenants can rotate their API key. All authenticated endpoints validate `X-API-Key` header against the bcrypt hash.
**Acceptance Criteria:**
- AC-2.1: `POST /api/v1/auth/rotate-key` issues a new key, invalidates the old key, and returns the new plaintext key once.
- AC-2.2: Missing or invalid `X-API-Key` returns HTTP 401 with `{error: "unauthorized"}`.
- AC-2.3: Authentication latency is bounded by a single bcrypt verify per request (cached for the request lifetime).
- AC-2.4: Inactive tenants (`is_active = FALSE`) are rejected with HTTP 401 regardless of key validity.

### FR-3: Tenant Settings Management
**Title:** Read and update tenant configuration
**Description:** Tenants can read and patch their settings, including webhook URL, alert thresholds, and retention.
**Acceptance Criteria:**
- AC-3.1: `GET /api/v1/tenant/settings` returns full settings except `api_key_hash`.
- AC-3.2: `PATCH /api/v1/tenant/settings` accepts partial updates of `webhook_url`, `alert_thresholds`, `retention_days`.
- AC-3.3: `webhook_url` must be a valid HTTPS URL of length ≤ 500.
- AC-3.4: `retention_days` must be in `[1, 365]`; otherwise HTTP 422.
- AC-3.5: `alert_thresholds` is a JSON object validated against a published schema.

### FR-4: Single Log Ingestion
**Title:** Ingest a single log event
**Description:** Tenants send one log event via `POST /api/v1/logs/ingest`. The event is validated, published to Kafka topic `logs.raw`, and HTTP 202 returned.
**Acceptance Criteria:**
- AC-4.1: Endpoint returns HTTP 202 `{event_id, status: "accepted"}` after Kafka produce ack.
- AC-4.2: Required fields: `service_name`, `severity`, `message`. Optional: `metadata`, `trace_id`, `span_id`, `environment`.
- AC-4.3: `severity` must be one of `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL`.
- AC-4.4: `environment` defaults to `prod` and must be `prod`, `staging`, or `dev`.
- AC-4.5: Validation failure returns HTTP 422 with field-level error detail.
- AC-4.6: Endpoint returns within p99 < 10 ms (NFR-2).

### FR-5: Batch Log Ingestion
**Title:** Ingest up to 1000 log events in a single request
**Description:** `POST /api/v1/logs/ingest/batch` accepts an array of up to 1000 log events. Each event is validated independently; valid events are produced to Kafka.
**Acceptance Criteria:**
- AC-5.1: Endpoint returns HTTP 202 `{accepted: N, failed: M, errors: [{index, message}]}`.
- AC-5.2: Batch size > 1000 returns HTTP 422 immediately without processing.
- AC-5.3: A partial batch with some invalid events still accepts the valid subset.
- AC-5.4: All accepted events share a server-generated `ingested_at` timestamp set at request receipt.

### FR-6: Hot Path Log Storage
**Title:** Store last 1 hour of logs in Redis
**Description:** Each log event is appended to a per-tenant per-service Redis list capped at 10,000 entries with a 1-hour TTL.
**Acceptance Criteria:**
- AC-6.1: Logs are written to `tenant:{tenant_id}:service:{service_name}:logs` via `LPUSH`.
- AC-6.2: List is trimmed by `LTRIM 0 9999` after each push.
- AC-6.3: Key TTL is set/refreshed to 3600 seconds on each write.
- AC-6.4: Redis is configured with `maxmemory 512mb` and `maxmemory-policy allkeys-lru`.

### FR-7: Cold Path Log Storage
**Title:** Persist logs to PostgreSQL for the retention window
**Description:** All ingested logs are persisted to the `logs` table in PostgreSQL by the log-processor consumer using batched inserts.
**Acceptance Criteria:**
- AC-7.1: Logs older than `tenants.retention_days` are deleted by a daily reaper job.
- AC-7.2: Batched insert size is at least 500 logs or 1 second since first batched event, whichever first.
- AC-7.3: Insert failures are retried up to 3 times before being routed to `logs.raw.dlq`.
- AC-7.4: Logs default retention is 90 days unless overridden by tenant.

### FR-8: Statistical Anomaly Detection
**Title:** Detect anomalies via baseline statistical analysis
**Description:** Every 30 seconds per `tenant × service`, the anomaly-detector consumer compares current 5-minute volume and error counts to baseline metrics. Anomalies are emitted with a `severity_score` in `[0.00, 1.00]`.
**Acceptance Criteria:**
- AC-8.1: Detected anomaly types: `volume_spike`, `volume_drop`, `new_error_pattern`, `error_rate_spike`.
- AC-8.2: An anomaly is emitted when the current value deviates by ≥ 3 standard deviations from baseline OR when a new normalized error template appears that is absent from `error_patterns` set.
- AC-8.3: `severity_score` is computed from deviation magnitude, normalized to `[0.00, 1.00]`.
- AC-8.4: Anomalies are written to PostgreSQL `anomalies` table and published to Kafka `logs.anomalies`.
- AC-8.5: Baseline values are stored in Redis with 24-hour TTL and refreshed on a rolling window.

### FR-9: AI-Powered Anomaly Analysis
**Title:** Enrich anomalies with Claude Haiku root-cause analysis
**Description:** The claude-analyzer consumer reads each anomaly from `logs.anomalies`, calls Claude Haiku within 30 seconds, attaches `claude_analysis` text, and publishes to `logs.alerts`.
**Acceptance Criteria:**
- AC-9.1: Claude Haiku is called within 30 seconds of anomaly detection (NFR-10).
- AC-9.2: A semaphore caps concurrent Claude calls at 10.
- AC-9.3: Claude failures fall back to a templated description and the anomaly is still alerted.
- AC-9.4: `anomalies.claude_analysis` is updated with the analysis text.
- AC-9.5: Each tenant's daily Claude analysis count is tracked in `tenant:{tenant_id}:analysis_credits:{epoch_day}`.

### FR-10: Alert Generation
**Title:** Convert analyzed anomalies into alerts
**Description:** The alert-engine consumer creates an `alerts` row per analyzed anomaly, applies deduplication and rate limiting, and queues webhook delivery.
**Acceptance Criteria:**
- AC-10.1: Each alert has `dedup_key = sha256("{tenant_id}:{service_name}:{anomaly_type}:{epoch_hour}")`.
- AC-10.2: Alerts mapped to existing `dedup_key` (within 1 hour) are dropped — no duplicate row inserted.
- AC-10.3: Alert severity (low/medium/high/critical) is derived from `severity_score` thresholds: `< 0.30 low`, `< 0.60 medium`, `< 0.85 high`, otherwise critical.
- AC-10.4: An alert is created only if the tenant's hourly alert quota is not exhausted (NFR-8).

### FR-11: Alert Deduplication
**Title:** Suppress repeat alerts within a 1-hour window
**Description:** A given `service + anomaly_type` combination produces at most 1 alert per tenant per hour.
**Acceptance Criteria:**
- AC-11.1: Dedup state is held in Redis set `alerts:dedup:{tenant_id}` with 3600 s TTL.
- AC-11.2: PostgreSQL has a `UNIQUE` constraint on `alerts.dedup_key` enforcing the same invariant at write time.
- AC-11.3: Dropped alerts are counted in metrics for observability.

### FR-12: Webhook Delivery
**Title:** Deliver alerts via HTTPS webhook
**Description:** Each alert is delivered via HTTP POST to `tenants.webhook_url`. Body is signed via HMAC-SHA256.
**Acceptance Criteria:**
- AC-12.1: Body is JSON: `{alert_id, tenant_id, service_name, anomaly_type, severity, title, description, claude_analysis, detected_at}`.
- AC-12.2: Header `X-Signature: sha256=<hex>` carries HMAC-SHA256 of the raw body using the tenant's API key as the secret.
- AC-12.3: Successful delivery is HTTP 2xx within 10 s; otherwise retried up to 3 times with exponential backoff (1s, 5s, 25s).
- AC-12.4: After 3 failed retries, `delivery_status = "failed"` and `last_error` is recorded.
- AC-12.5: Slack-compatible payload format is supported: same body schema works as a Slack incoming webhook payload after a transformation hook.

### FR-13: Alert Rate Limiting
**Title:** Cap alerts per tenant
**Description:** Each tenant is limited to 10 alerts per hour (NFR-8).
**Acceptance Criteria:**
- AC-13.1: Counter held in Redis `alerts:rate:{tenant_id}` with 3600 s TTL.
- AC-13.2: Once cap reached, additional alerts are recorded in PostgreSQL with `delivery_status = "pending"` but webhook is suppressed and a flag indicates rate-limit suppression.
- AC-13.3: Rate-limit counter resets on TTL expiry.

### FR-14: Log Query API
**Title:** Filter, search, and paginate logs
**Description:** `GET /api/v1/logs` returns logs filtered by service, severity, time range, free-text search, trace_id, with pagination.
**Acceptance Criteria:**
- AC-14.1: `limit` defaults to 100 and is capped at 1000.
- AC-14.2: `from_time` and `to_time` are ISO 8601 timestamps; missing values default to last 1 hour.
- AC-14.3: Free-text `search_text` uses PostgreSQL `pg_trgm` GIN index on `message`.
- AC-14.4: Response includes `{data, total, page_info: {limit, offset, has_next}}`.
- AC-14.5: Single-log fetch via `GET /api/v1/logs/{log_id}` returns 404 if absent.
- AC-14.6: p99 latency < 500 ms (NFR-3).

### FR-15: Service Metrics Endpoint
**Title:** Per-service rollup metrics
**Description:** `GET /api/v1/metrics/services` returns the list of all services for a tenant with rollup error rate, volume, and last-seen timestamp.
**Acceptance Criteria:**
- AC-15.1: Computes from the last 1-hour window backed by Redis counters.
- AC-15.2: Service list comes from `tenant:{tenant_id}:services` Redis set.
- AC-15.3: Each entry: `{service_name, volume_5m, error_rate_5m, last_seen_at}`.

### FR-16: Anomaly Query API
**Title:** Filter and inspect anomalies
**Description:** Endpoints `GET /api/v1/anomalies`, `GET /api/v1/anomalies/{id}`, and `GET /api/v1/anomalies/{id}/logs` allow listing, fetching, and retrieving the source logs that triggered the anomaly.
**Acceptance Criteria:**
- AC-16.1: List endpoint supports filters: `service`, `status`, `from_time`, `severity_min`, `limit`, `offset`.
- AC-16.2: `severity_min` is `[0.0, 1.0]`; rejects out-of-range with HTTP 422.
- AC-16.3: `/logs` endpoint returns logs whose `created_at ∈ [window_start, window_end]` for the anomaly's service.
- AC-16.4: Anomalies are scoped strictly to the requesting tenant (NFR-6).

### FR-17: Anomaly Lifecycle Management
**Title:** Acknowledge and resolve anomalies
**Description:** `PATCH /api/v1/anomalies/{id}/acknowledge` and `PATCH /api/v1/anomalies/{id}/resolve` transition anomaly status.
**Acceptance Criteria:**
- AC-17.1: Acknowledge moves status `active → acknowledged`; idempotent if already acknowledged.
- AC-17.2: Resolve moves status to `resolved` and sets `resolved_at = NOW()`.
- AC-17.3: Cannot transition from `resolved` back to `active`; returns HTTP 409.

### FR-18: Alert Query API
**Title:** Filter and inspect alerts
**Description:** Endpoints to list, fetch, and aggregate alerts.
**Acceptance Criteria:**
- AC-18.1: `GET /api/v1/alerts` filters by `service`, `severity`, `status`, `from_time`.
- AC-18.2: `GET /api/v1/alerts/{id}` returns full alert detail including delivery history.
- AC-18.3: `GET /api/v1/alerts/stats` returns `{total_today, by_severity, delivery_rate, top_alerting_services}`.
- AC-18.4: `POST /api/v1/webhooks/test` sends a synthetic alert payload to the configured webhook and returns the delivery result.

### FR-19: Analytics Endpoints
**Title:** Tenant-level analytics
**Description:** Endpoints provide tenant-wide and service-specific aggregations.
**Acceptance Criteria:**
- AC-19.1: `GET /api/v1/analytics/overview` returns 24-hour rollup: total logs, error count, anomaly count, alert count.
- AC-19.2: `GET /api/v1/analytics/services` returns per-service 24-hour rollups sorted by volume desc.
- AC-19.3: `GET /api/v1/analytics/services/{name}/timeline` returns 5-minute bucketed time series for the last 24 hours.

### FR-20: Root Cause Analysis
**Title:** Claude-driven RCA on demand
**Description:** `POST /api/v1/analysis/root-cause` returns a Claude Haiku-generated root-cause explanation for a service over a time window.
**Acceptance Criteria:**
- AC-20.1: Body: `{service, from_time, to_time}`.
- AC-20.2: Window must be ≤ 24 hours; otherwise HTTP 422.
- AC-20.3: Returns `{summary, suspected_causes: [...], evidence_log_ids: [...]}`.
- AC-20.4: Each invocation increments `tenant:{tenant_id}:analysis_credits:{epoch_day}` and respects per-plan daily caps.

### FR-21: Period Comparison Analysis
**Title:** Compare two time periods for a service
**Description:** `POST /api/v1/analysis/compare` compares two equal-length windows of logs/metrics.
**Acceptance Criteria:**
- AC-21.1: Body: `{service, period1_start, period2_start, hours}` where `hours ∈ [1, 24]`.
- AC-21.2: Returns delta of volume, error rate, top error patterns, with Claude commentary.

### FR-22: WebSocket Log Streaming
**Title:** Real-time log stream
**Description:** Clients connect to `WS /ws/logs/{service_name}?token=<api_key>` to receive live log events for a service.
**Acceptance Criteria:**
- AC-22.1: Authentication uses query-string token validated identically to `X-API-Key`.
- AC-22.2: On connect, the server subscribes to Redis pub/sub channel `tenant:{tenant_id}:service:{service_name}:stream`.
- AC-22.3: Heartbeat ping every 30 s; disconnect after 90 s of silence.
- AC-22.4: Backpressure: if client buffer exceeds 1000 messages, server disconnects with code `1013`.

### FR-23: WebSocket Anomaly Streaming
**Title:** Real-time anomaly stream
**Description:** Clients connect to `WS /ws/anomalies?token=<api_key>` for live anomaly notifications.
**Acceptance Criteria:**
- AC-23.1: Subscribes to Redis pub/sub `tenant:{tenant_id}:anomalies:stream`.
- AC-23.2: Each message carries the full anomaly object including `claude_analysis` if available.

### FR-24: Health Check
**Title:** Operational health endpoint
**Description:** `GET /health` reports service health and dependency status.
**Acceptance Criteria:**
- AC-24.1: Returns `{status: "healthy"|"degraded"|"unhealthy", components: {postgres, redis, kafka}, timestamp}`.
- AC-24.2: `healthy` requires all components reachable; `degraded` if Kafka or Redis unreachable but PostgreSQL OK; otherwise `unhealthy`.
- AC-24.3: No authentication required.

---

## 3. Non-Functional Requirements

### NFR-1: Ingestion Throughput
The system must sustain **100,000 log events per minute** end-to-end ingestion for at least 60 minutes without dropped events or growing backlog. Verified by load test.

### NFR-2: Ingestion Latency
`POST /api/v1/logs/ingest` and `POST /api/v1/logs/ingest/batch` must return in **< 10 ms p99** under the NFR-1 load. Latency excludes network RTT measured from server entry to server exit.

### NFR-3: Query Latency
`GET /api/v1/logs` and `GET /api/v1/anomalies` must return in **< 500 ms p99** for queries returning ≤ 1000 rows over a 24-hour window.

### NFR-4: Storage Tiering
- Redis: holds last **1 hour** of logs; configured with `maxmemory 512mb`, `maxmemory-policy allkeys-lru`.
- PostgreSQL: holds **90 days** by default (per-tenant override permitted). Retention enforced by daily reaper job.

### NFR-5: Availability
The system must achieve **99.9% uptime** measured monthly on the public ingestion and query endpoints.

### NFR-6: Multi-Tenancy Isolation
All data access must be scoped by `tenant_id`. Cross-tenant reads or writes are not permitted at any layer (PostgreSQL queries, Redis keys, Kafka message routing, WebSocket channels). Every authenticated handler must derive `tenant_id` from the API key, never from request input.

### NFR-7: Security
- API keys are hashed with **bcrypt** (cost ≥ 12) and never logged in plaintext.
- Webhook payloads are signed via **HMAC-SHA256** in header `X-Signature`.
- All external endpoints require HTTPS in production.
- API key rotation invalidates the old key immediately.
- Input validation rejects malformed payloads before they reach Kafka.

### NFR-8: Alert Rate Limiting
Maximum **10 alerts per tenant per hour**. Suppressed alerts are recorded but the webhook is not sent.

### NFR-9: Alert Deduplication Window
Within a rolling **1-hour window**, at most **1 alert per (service, anomaly_type) per tenant** is delivered. Dedup is enforced both in Redis and via PostgreSQL `UNIQUE(dedup_key)`.

### NFR-10: AI Analysis Latency
Claude Haiku analysis must be invoked within **30 seconds** of statistical anomaly detection. Concurrent calls capped at 10 via semaphore.

### NFR-11: Observability
The platform itself must expose Prometheus metrics for: ingestion rate, query latency, Kafka consumer lag, Claude API call latency, webhook delivery success rate, Redis memory usage, PostgreSQL connection pool utilization.

### NFR-12: Backpressure & Graceful Degradation
- If Redis is unreachable, ingestion continues to Kafka and PostgreSQL; hot path read endpoints return HTTP 503.
- If Kafka is unreachable, ingestion endpoints return HTTP 503; no data is silently dropped.
- If PostgreSQL is unreachable, ingestion continues to Kafka with deferred writes; query endpoints return HTTP 503.

### NFR-13: Configuration & Secrets
All credentials, API keys, and connection strings are read from environment variables. No secrets are committed to source control.
