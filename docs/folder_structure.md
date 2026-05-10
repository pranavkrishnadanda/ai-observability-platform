# Folder Structure (FINAL)

This is the authoritative file tree for the AI Observability Platform. **Subsequent
engineers must not deviate from this structure.** Every file listed here will exist;
no additional top-level directories or modules will be created.

Each entry has a one-line purpose comment. Where a comment ends with `[NEW]`, the
file does not yet exist and will be created in implementation.

```
ai-observability-platform/
├── backend/
│   ├── app/
│   │   ├── __init__.py                            # Marks app as a Python package.
│   │   ├── api/
│   │   │   ├── __init__.py                        # Marks api as a Python package.
│   │   │   └── v1/
│   │   │       ├── __init__.py                    # Aggregates v1 routers and exports a single APIRouter.
│   │   │       ├── ingest.py                      # POST /logs/ingest and /logs/ingest/batch — Kafka-only fast path.
│   │   │       ├── logs.py                        # GET /logs query endpoints — hot/cold tier read.
│   │   │       ├── anomalies.py                   # Anomaly CRUD and status updates (acknowledge, resolve).
│   │   │       ├── alerts.py                      # Alert listing, stats, and POST /webhooks/test.
│   │   │       ├── analytics.py                   # Overview, services, services/{name}/timeline endpoints.
│   │   │       ├── analysis.py                    # POST /analysis/root-cause and /analysis/compare (Claude).
│   │   │       ├── websocket.py                   # WS /ws/logs/{service} and /ws/anomalies streaming.
│   │   │       └── tenants.py                     # Auth register, rotate-key, GET/PATCH /tenant/settings.
│   │   ├── core/
│   │   │   ├── __init__.py                        # Marks core as a Python package.
│   │   │   ├── config.py                          # Pydantic Settings: all env vars (DB_URL, REDIS_URL, KAFKA_BROKERS, ANTHROPIC_API_KEY, etc).
│   │   │   ├── database.py                        # Async SQLAlchemy engine, AsyncSession factory, get_db dependency.
│   │   │   ├── redis_client.py                    # Async Redis pool, helpers (incr_with_ttl, lpush_ltrim, pubsub_publish).
│   │   │   ├── kafka_client.py                    # Singleton aiokafka Producer; consumer factory with idempotent config.
│   │   │   └── auth.py                            # API key generation, bcrypt hash/verify, FastAPI tenant dependency.
│   │   ├── models/
│   │   │   ├── __init__.py                        # Re-exports SQLAlchemy Base and all ORM models.
│   │   │   ├── tenants.py                         # SQLAlchemy Tenant model (id, name, api_key_hash, plan_tier, ...).
│   │   │   ├── logs.py                            # SQLAlchemy Log model (id, tenant_id, service_name, severity, message, ...).
│   │   │   ├── anomalies.py                       # SQLAlchemy Anomaly model (id, anomaly_type, severity_score, status, ...).
│   │   │   └── alerts.py                          # SQLAlchemy Alert model (id, anomaly_id, dedup_key, delivery_status, ...).
│   │   ├── schemas/
│   │   │   ├── __init__.py                        # Re-exports all Pydantic schemas.
│   │   │   ├── tenant.py                          # Pydantic schemas: TenantRegisterIn, TenantOut, TenantSettingsPatch.
│   │   │   ├── log.py                             # Pydantic schemas: LogIngestIn, LogBatchIn, LogOut, LogQueryParams.
│   │   │   ├── anomaly.py                         # Pydantic schemas: AnomalyOut, AnomalyQueryParams.
│   │   │   └── alert.py                           # Pydantic schemas: AlertOut, AlertStatsOut, WebhookTestOut.
│   │   ├── services/
│   │   │   ├── __init__.py                        # Marks services as a Python package.
│   │   │   ├── log_service.py                     # Hot/cold-path log queries, volume/error metric rollups, retention reaper.
│   │   │   ├── anomaly_detector.py                # Stage-1 statistical detection (3-sigma, new pattern) per tenant×service tick.
│   │   │   ├── baseline_calculator.py             # Rolling 7-day EWMA baseline computation written back to Redis.
│   │   │   ├── alert_engine.py                    # Dedup check, rate-limit check, severity bucketing, alert row creation.
│   │   │   └── webhook_deliverer.py               # Async httpx POST with HMAC-SHA256 signing and 1s/5s/25s retry.
│   │   ├── consumers/
│   │   │   ├── __init__.py                        # Marks consumers as a Python package; exposes run() entrypoints.
│   │   │   ├── log_consumer.py                    # Consumes logs.raw → PG batch insert + Redis writes + produce logs.processed; also hosts websocket-streamer mode.
│   │   │   └── anomaly_consumer.py                # Hosts anomaly-detector, claude-analyzer, and alert-engine modes selected by --mode flag.
│   │   └── utils/
│   │       ├── __init__.py                        # Marks utils as a Python package.
│   │       └── log_template.py                    # Normalizes error messages (strips UUIDs/IPs/numbers) for new_error_pattern detection.
│   ├── main.py                                    # FastAPI app factory: middleware stack, lifespan (Kafka producer, Redis pool, PG engine), router mount.
│   ├── alembic/
│   │   ├── alembic.ini                            # Alembic config: script_location, sqlalchemy.url placeholder.
│   │   ├── env.py                                 # Alembic env loading async engine and target metadata from app.models.
│   │   └── versions/
│   │       └── 001_initial_schema.py              # Initial migration creating tenants, logs, anomalies, alerts + indexes + extensions.
│   ├── tests/
│   │   ├── __init__.py                            # Marks tests as a Python package.
│   │   ├── conftest.py                            # Pytest fixtures: test DB, fakeredis, mock Kafka producer, AsyncClient, tenant factory.
│   │   ├── test_ingestion.py                      # Tests for POST /logs/ingest and /logs/ingest/batch happy + error paths.
│   │   ├── test_anomaly_detection.py              # Unit tests for detector logic, baseline calc, template normalization.
│   │   ├── test_alert_engine.py                   # Tests dedup, rate limit, HMAC signing, retry policy.
│   │   ├── test_query_api.py                      # Tests GET /logs hot/cold paths, filters, pagination.
│   │   └── test_integration.py                    # End-to-end ingest → consume → query happy path with real Postgres + fakeredis.
│   ├── Dockerfile                                 # Multi-stage build: python:3.12-slim, installs pyproject deps, runs uvicorn or consumer.
│   └── pyproject.toml                             # Build system + dependency manifest (fastapi, uvicorn, aiokafka, sqlalchemy[asyncio], asyncpg, redis, anthropic, structlog, prometheus-client, httpx, bcrypt, pytest, alembic).
├── frontend/
│   ├── src/
│   │   ├── components/
│   │   │   ├── LogStream.tsx                      # Real-time log stream UI, consumes useWebSocket('/ws/logs/{service}').
│   │   │   ├── AnomalyList.tsx                    # Anomaly cards with severity, Claude analysis, status actions.
│   │   │   ├── AlertList.tsx                      # Alert history table with delivery status filtering.
│   │   │   ├── ServiceOverview.tsx                # Per-service health card: volume_5m, error_rate_5m, last_seen.
│   │   │   └── AnalyticsDashboard.tsx             # Overview metrics charts (24h volume, errors, anomalies, alerts).
│   │   ├── pages/
│   │   │   ├── Dashboard.tsx                      # Top-level dashboard combining AnalyticsDashboard + ServiceOverview.
│   │   │   ├── Logs.tsx                           # Logs page: filter form + LogStream + paginated table.
│   │   │   ├── Anomalies.tsx                      # Anomalies page: filters + AnomalyList + detail drawer.
│   │   │   └── Settings.tsx                       # Settings page: API key rotate, webhook URL, alert thresholds, retention.
│   │   ├── hooks/
│   │   │   └── useWebSocket.ts                    # Reusable WebSocket hook with auto-reconnect, heartbeat pong, exponential backoff.
│   │   ├── api/
│   │   │   └── client.ts                          # Axios instance, X-API-Key header injection, typed REST methods.
│   │   └── App.tsx                                # Top-level App component: router, layout, page registry.
│   ├── package.json                               # React/Vite/TypeScript dependencies + dev/build scripts.
│   └── Dockerfile                                 # Multi-stage: vite build then nginx:alpine serving /usr/share/nginx/html.
├── load_tests/
│   ├── locustfile.py                              # Locust scenarios: 100k logs/min ingestion + concurrent query mix.
│   ├── run_tests.sh                               # Convenience shell script to run Locust headless against docker-compose stack.
│   └── README.md                                  # Instructions for running load tests and interpreting NFR-1/NFR-2 results.
├── docs/
│   ├── requirements.md                            # Functional & non-functional requirements (Phase 1).
│   ├── api_contracts.md                           # REST + WebSocket API contracts (Phase 1).
│   ├── data_models.md                             # Postgres schema + Redis key reference (Phase 1).
│   ├── kafka_design.md                            # Topics, consumer groups, DLQ pattern (Phase 1).
│   ├── sdlc_checklist.md                          # SDLC phase tracking (Phase 1).
│   ├── architecture.md                            # System diagram + data flows (this phase).
│   ├── folder_structure.md                        # This file — final tree (this phase).
│   ├── tech_decisions.md                          # Tech choice rationale (this phase).
│   ├── interview_prep.md                          # Interview-ready talking points (later phase).
│   └── performance.md                             # Load test results and NFR validation (later phase).
├── infra/
│   └── init.sql                                   # CREATE EXTENSION pg_trgm/pgcrypto + initial DDL (run by postgres image on first boot).
├── docker-compose.yml                             # Postgres, Redis, Zookeeper, Kafka, kafka-bootstrap, backend-api, all consumer services, frontend.
├── .env.example                                   # Template env file documenting every required variable.
└── README.md                                      # Project overview, quickstart, architecture summary, links to /docs.
```

## Total File Count
- Backend Python files: 38 (including `__init__.py` files, alembic, tests)
- Backend config: 2 (`Dockerfile`, `pyproject.toml`)
- Alembic: 3 (`alembic.ini`, `env.py`, `001_initial_schema.py`)
- Frontend TS/TSX: 12 source files + `package.json` + `Dockerfile`
- Load tests: 3 files
- Docs: 10 files
- Infra: 1 file
- Root: 3 files (`docker-compose.yml`, `.env.example`, `README.md`)

## Module Boundary Rules (binding)
1. `app/api/v1/*` modules contain **only HTTP/WS handlers**. They depend on `core`,
   `schemas`, and `services` — never on `consumers` or `models` directly (use services).
2. `app/services/*` is the only layer allowed to compose Postgres + Redis + Kafka calls.
3. `app/consumers/*` modules are independently runnable Python entrypoints
   (`python -m app.consumers.log_consumer`). They reuse `services` for business logic.
4. `app/models/*` exposes ORM classes only. No business logic.
5. `app/schemas/*` exposes Pydantic models only. No I/O.
6. `app/core/*` is pure infrastructure (engines, pools, factories) and has zero
   dependencies on `services`/`api`/`consumers`.
7. The `frontend/` directory is fully self-contained. It talks to the backend only
   over HTTPS/WSS using the URL configured at build time via `VITE_API_BASE_URL`.
