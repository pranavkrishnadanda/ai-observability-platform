# SDLC Checklist — AI Observability Platform

A comprehensive checklist of every deliverable across all 11 phases. When the project is complete, every box below should be checked and every file should exist on disk.

Conventions:
- Paths are relative to the repository root unless otherwise noted.
- Code paths assume a Python `backend/` and a `frontend/` React app.

---

## Phase 1 — Requirements & Design

- [ ] `docs/requirements.md` — functional + non-functional requirements
- [ ] `docs/api_contracts.md` — OpenAPI-style endpoint specs
- [ ] `docs/data_models.md` — PostgreSQL + Redis schema
- [ ] `docs/kafka_design.md` — topics, consumers, DLQs
- [ ] `docs/sdlc_checklist.md` — this file
- [ ] `docs/status.txt` — `PHASE_1_COMPLETE`

## Phase 2 — Project Scaffolding & Infrastructure

- [ ] `README.md` — project overview, quickstart
- [ ] `.gitignore`
- [ ] `.env.example` — all required environment variables
- [ ] `docker-compose.yml` — postgres, redis, kafka, zookeeper
- [ ] `Dockerfile` (backend)
- [ ] `frontend/Dockerfile`
- [ ] `Makefile` — `make up`, `make down`, `make test`, `make lint`, `make migrate`, `make seed`
- [ ] `pyproject.toml` or `requirements.txt`
- [ ] `frontend/package.json`
- [ ] `scripts/kafka_bootstrap.py` — creates all topics + DLQs
- [ ] `scripts/wait_for_deps.sh` — readiness gate for compose
- [ ] `.github/workflows/ci.yml` — lint, type-check, test on PR
- [ ] `.pre-commit-config.yaml`

## Phase 3 — Database Layer

- [ ] `backend/migrations/0001_initial.sql` — tenants, logs, anomalies, alerts + indexes
- [ ] `backend/migrations/0002_extensions.sql` — pg_trgm, pgcrypto
- [ ] `backend/migrations/0003_triggers.sql` — `set_updated_at`
- [ ] `backend/app/db/connection.py` — async pool (asyncpg) + retry logic
- [ ] `backend/app/db/models.py` — SQLAlchemy / Pydantic ORM models
- [ ] `backend/app/db/repositories/tenants.py`
- [ ] `backend/app/db/repositories/logs.py`
- [ ] `backend/app/db/repositories/anomalies.py`
- [ ] `backend/app/db/repositories/alerts.py`
- [ ] `backend/app/db/reaper.py` — daily retention enforcement
- [ ] `backend/tests/db/test_repositories.py`

## Phase 4 — Redis Layer

- [ ] `backend/app/cache/redis_client.py` — connection pool + helpers
- [ ] `backend/app/cache/hot_logs.py` — LPUSH/LTRIM helpers
- [ ] `backend/app/cache/counters.py` — volume + error counters
- [ ] `backend/app/cache/baselines.py` — baseline read/write
- [ ] `backend/app/cache/error_patterns.py` — SADD/SISMEMBER helpers
- [ ] `backend/app/cache/dedup.py` — alert dedup helpers
- [ ] `backend/app/cache/rate_limiter.py` — API + alert rate limiting
- [ ] `backend/app/cache/services_registry.py`
- [ ] `backend/app/cache/pubsub.py` — publish + subscribe wrappers
- [ ] `backend/tests/cache/test_redis_helpers.py`

## Phase 5 — Kafka Producers & Consumers

- [ ] `backend/app/kafka/producer.py` — shared idempotent producer
- [ ] `backend/app/kafka/consumer_base.py` — manual-commit base class with DLQ routing
- [ ] `backend/app/kafka/consumers/log_processor.py`
- [ ] `backend/app/kafka/consumers/anomaly_detector.py`
- [ ] `backend/app/kafka/consumers/claude_analyzer.py`
- [ ] `backend/app/kafka/consumers/alert_engine.py`
- [ ] `backend/app/kafka/consumers/websocket_streamer.py`
- [ ] `backend/app/kafka/schemas.py` — pydantic models for each topic message
- [ ] `backend/tests/kafka/test_log_processor.py`
- [ ] `backend/tests/kafka/test_anomaly_detector.py`
- [ ] `backend/tests/kafka/test_alert_engine.py`

## Phase 6 — Core API (Auth, Tenants, Ingestion, Query)

- [ ] `backend/app/main.py` — FastAPI app factory + middleware wiring
- [ ] `backend/app/api/middleware/auth.py` — `X-API-Key` validator
- [ ] `backend/app/api/middleware/rate_limit.py`
- [ ] `backend/app/api/middleware/request_id.py`
- [ ] `backend/app/api/v1/auth.py` — register, rotate-key
- [ ] `backend/app/api/v1/tenant.py` — settings GET/PATCH
- [ ] `backend/app/api/v1/logs_ingest.py` — single + batch ingest
- [ ] `backend/app/api/v1/logs_query.py` — list, fetch by id
- [ ] `backend/app/api/v1/metrics.py` — services metrics
- [ ] `backend/app/api/v1/anomalies.py` — list, fetch, ack, resolve, anomaly logs
- [ ] `backend/app/api/v1/alerts.py` — list, fetch, stats, webhook test
- [ ] `backend/app/api/v1/analytics.py` — overview, services, timeline
- [ ] `backend/app/api/v1/analysis.py` — root-cause, compare
- [ ] `backend/app/api/v1/health.py`
- [ ] `backend/app/security/api_key.py` — bcrypt hash + verify
- [ ] `backend/app/security/hmac.py` — webhook signing
- [ ] `backend/tests/api/test_auth.py`
- [ ] `backend/tests/api/test_logs_ingest.py`
- [ ] `backend/tests/api/test_logs_query.py`
- [ ] `backend/tests/api/test_anomalies.py`
- [ ] `backend/tests/api/test_alerts.py`
- [ ] `backend/tests/api/test_analytics.py`
- [ ] `backend/tests/api/test_health.py`

## Phase 7 — WebSocket Layer

- [ ] `backend/app/websocket/auth.py` — token validation from query string
- [ ] `backend/app/websocket/log_stream.py` — `/ws/logs/{service}`
- [ ] `backend/app/websocket/anomaly_stream.py` — `/ws/anomalies`
- [ ] `backend/app/websocket/heartbeat.py` — ping/pong + idle disconnect
- [ ] `backend/app/websocket/backpressure.py` — buffer + 1013 disconnect
- [ ] `backend/tests/websocket/test_log_stream.py`
- [ ] `backend/tests/websocket/test_anomaly_stream.py`

## Phase 8 — AI Integration & Analysis

- [ ] `backend/app/ai/claude_client.py` — Anthropic SDK wrapper, semaphore, timeout
- [ ] `backend/app/ai/prompts/anomaly_analysis.txt`
- [ ] `backend/app/ai/prompts/root_cause.txt`
- [ ] `backend/app/ai/prompts/compare_periods.txt`
- [ ] `backend/app/ai/anomaly_analyzer.py`
- [ ] `backend/app/ai/root_cause.py`
- [ ] `backend/app/ai/compare.py`
- [ ] `backend/app/ai/credit_tracker.py`
- [ ] `backend/tests/ai/test_claude_client.py`
- [ ] `backend/tests/ai/test_root_cause.py`

## Phase 9 — Frontend Dashboard (React)

- [ ] `frontend/src/main.tsx` / `frontend/src/App.tsx`
- [ ] `frontend/src/api/client.ts` — typed REST client
- [ ] `frontend/src/api/websocket.ts` — reconnecting WS client
- [ ] `frontend/src/pages/Login.tsx` — API key entry
- [ ] `frontend/src/pages/Dashboard.tsx` — overview KPIs
- [ ] `frontend/src/pages/Logs.tsx` — real-time stream + filters
- [ ] `frontend/src/pages/Anomalies.tsx` — list + detail drawer
- [ ] `frontend/src/pages/Alerts.tsx` — list + delivery status
- [ ] `frontend/src/pages/Services.tsx` — per-service metrics + timeline charts
- [ ] `frontend/src/pages/Analysis.tsx` — root-cause + compare UIs
- [ ] `frontend/src/pages/Settings.tsx` — webhook URL, retention, key rotation
- [ ] `frontend/src/components/LogStream.tsx` — WebSocket-driven log viewer
- [ ] `frontend/src/components/SeverityChart.tsx`
- [ ] `frontend/src/components/AnomalyCard.tsx`
- [ ] `frontend/src/components/AlertBadge.tsx`
- [ ] `frontend/src/state/store.ts` — global state
- [ ] `frontend/tests/components.test.tsx`
- [ ] `frontend/tests/api_client.test.ts`

## Phase 10 — Observability & Operations

- [ ] `backend/app/observability/metrics.py` — Prometheus metric definitions
- [ ] `backend/app/observability/logging.py` — structured JSON logging
- [ ] `backend/app/observability/tracing.py` — OpenTelemetry setup
- [ ] `backend/app/api/v1/metrics_export.py` — `/metrics` Prometheus endpoint
- [ ] `ops/grafana/dashboards/ingestion.json`
- [ ] `ops/grafana/dashboards/anomalies.json`
- [ ] `ops/grafana/dashboards/alerts.json`
- [ ] `ops/prometheus/prometheus.yml`
- [ ] `ops/alerts/rules.yml`
- [ ] `docs/runbook.md` — incident response procedures
- [ ] `docs/operations.md` — deployment, scaling, backup

## Phase 11 — Load Testing, Hardening & Release

- [ ] `loadtest/locustfile.py` — 100k events/min sustained scenario
- [ ] `loadtest/scenarios/ingest_single.py`
- [ ] `loadtest/scenarios/ingest_batch.py`
- [ ] `loadtest/scenarios/query_logs.py`
- [ ] `loadtest/scenarios/mixed_workload.py`
- [ ] `loadtest/README.md` — how to run, target SLOs
- [ ] `docs/load_test_results.md` — captured results meeting NFR-1 / NFR-2 / NFR-3
- [ ] `docs/security_review.md` — bcrypt cost, HMAC validation, dependency scan
- [ ] `docs/threat_model.md`
- [ ] `backend/tests/integration/test_end_to_end.py` — log → anomaly → alert flow
- [ ] `backend/tests/integration/test_multi_tenant_isolation.py`
- [ ] `backend/tests/integration/test_failure_modes.py` — Redis/Kafka/PG down
- [ ] `CHANGELOG.md`
- [ ] `RELEASE_NOTES_v1.0.md`
- [ ] `docs/status.txt` — `RELEASE_READY`

---

## Cross-Phase Quality Gates

- [ ] All linters pass (`ruff`, `mypy`, `eslint`, `tsc`)
- [ ] Unit test coverage ≥ 80% on backend
- [ ] All API endpoints have at least one happy-path + one error-path test
- [ ] Multi-tenant isolation test passes
- [ ] Load test demonstrates 100,000 events/min sustained for 60 minutes
- [ ] p99 ingest latency < 10 ms under sustained load
- [ ] p99 log query latency < 500 ms
- [ ] Webhook delivery success rate ≥ 99% in staging
- [ ] All secrets sourced from environment variables; `.env` not committed
- [ ] Health check returns `healthy` only when postgres + redis + kafka reachable
- [ ] Documentation reviewed for accuracy at end of each phase
