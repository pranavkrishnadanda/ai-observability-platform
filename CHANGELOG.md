# Changelog

## [1.1.0] — 2026-05-11

### Fixed

- **Kafka local dev connectivity** — Added `PLAINTEXT_HOST` dual-listener to all three Kafka brokers in `docker-compose.yml` so a locally-running FastAPI process can connect via `localhost:9092/9093/9094`. Docker containers continue using internal `kafka-1:9092` addresses via the `environment:` override block. (`docker-compose.yml`)

- **Auth Redis cache invalidation on register** — `register_tenant` now calls `invalidate_tenant_cache()` after committing a new tenant. Previously, if the `auth:tenant_list` cache was warm, the freshly-created tenant would not be found for up to 30 s, causing every subsequent REST call to return 401 and redirect the user back to login. (`backend/app/api/v1/tenants.py`)

- **Login validation endpoint** — Sign-in now validates the API key against `GET /api/v1/tenant/settings` (returns 401/422 on bad keys) instead of `GET /health` (always 200). (`frontend/src/pages/Login.tsx`)

- **Kafka DLQ topic name** — `publish_to_dlq()` was sending to the non-existent topic `logs.raw.dlq`, causing a 60 s `KafkaTimeoutError` blocking `producer.flush()` on every batch containing malformed messages. The correct topic is `logs.dlq`, now referenced via `TOPICS["logs.dlq"]`. (`backend/app/consumers/log_consumer.py`, `backend/app/core/kafka_client.py`)

- **log-consumer resilience against malformed messages** — Batches mixing valid and invalid messages (missing `tenant_id`, `service_name`, `severity`, `message`, or `ingested_at`) would `KeyError`-crash and DLQ the entire batch including valid messages. Batches are now split into `valid_msgs` / `invalid_msgs` before processing: invalid messages are DLQ'd individually, valid messages proceed normally. (`backend/app/consumers/log_consumer.py`)

- **API response shape mismatches causing blank dashboard** — Three endpoints returned shapes inconsistent with the frontend TypeScript types, causing `TypeError: …map is not a function` which rendered the entire dashboard blank:
  - `GET /api/v1/anomalies` — was `{data: [...], total, limit, offset}`, now returns a flat `Anomaly[]` array.
  - `GET /api/v1/analytics/services` — was `{services: [{log_volume: {last_1h, …}, error_rate: {last_1h, …}}]}`, now returns a flat `ServiceMetrics[]` array with `log_volume_1h`, `log_volume_24h`, `error_rate_1h`, `error_rate_24h`, `last_seen` fields. Also adds a per-service `last_seen` query (max `created_at` within the 7-day window).
  - `GET /api/v1/analytics/overview` — was missing `total_logs_yesterday` and `alerts_sent_today`; both are now included.
  (`backend/app/api/v1/anomalies.py`, `backend/app/api/v1/analytics.py`)

---

## [1.0.0] — 2026-05-10

### Added
- **Phase 1 — Requirements**: Multi-tenant SaaS requirements, 100k events/min target, dual-storage architecture
- **Phase 2 — Architecture**: FastAPI async design, Kafka pipeline, Redis hot path, PostgreSQL cold path
- **Phase 3 — Infrastructure**: Docker Compose, PostgreSQL 16, Redis 7, single-broker Kafka, Alembic migrations
- **Phase 4 — Core Backend**: Tenant auth with bcrypt + Redis cache, rate limiting, base models
- **Phase 5 — Ingestion Pipeline**: Kafka consumer with DLQ, dual-path storage writer, batch ingest endpoint
- **Phase 6 — AI Detection**: Two-stage anomaly detection — statistical baselines + Claude Haiku analysis
- **Phase 7 — Alert Engine**: Redis SADD dedup, token bucket, HMAC-signed webhook delivery with retry backoff
- **Phase 8 — Query API**: Log search, analytics overview, per-service metrics, 24h timeline
- **Phase 9 — Tests**: 40 pytest tests covering auth, ingest, anomaly detection, alerting, analytics
- **Phase 10 — Load Tests**: Locust scenarios — baseline 50 users, stress 200 users, mixed workload 100 users
- **Phase 11 — Documentation**: Architecture docs, API contracts, performance baseline, interview prep
- **Phase 12 — uv Migration**: Replaced pip with uv, generated lockfile, updated Dockerfile to use official uv image
- **Phase 13 — Scheduler**: Wired anomaly detection (30s) and baseline recalculation (5min) to FastAPI lifespan
- **Phase 14 — Kafka Cluster**: Upgraded to 3-broker cluster with replication-factor 3, MIN_INSYNC_REPLICAS 2
- **Phase 15 — Frontend Scaffold**: React 19 + Vite + TypeScript + Tailwind CSS v4 + bun
- **Phase 16 — API Layer**: Axios client with X-API-Key interceptor, typed API functions, WebSocket hook with exponential backoff, Zustand store
- **Phase 17 — Components**: SeverityBadge, StatusDot, MetricsBar, LogStream (virtualized), AnomalyCard, ServiceCard, ServiceHealthGrid, HealthIndicator, Layout
- **Phase 18 — Pages**: Login, Dashboard, ServiceDetail, Anomalies, Services, App router with protected routes
- **Phase 19 — Integration**: Frontend Dockerfile, nginx config, docker-compose frontend service, integration notes
- **Phase 20 — Documentation**: README rewrite, interview prep additions, performance notes, CHANGELOG
