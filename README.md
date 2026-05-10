# AI Observability Platform

> Multi-tenant log ingestion and AI-driven anomaly detection that survives 100,000 events/min on a single host without losing a message, with sub-10 ms p99 ingest latency and Claude Haiku root-cause analysis attached to every alert.

---

## Architecture

```
                                                                                                
                         +---------------------------------------------------+                  
                         |                  CUSTOMER APPLICATIONS             |                  
                         |       (SDKs / curl / log shippers, X-API-Key)      |                  
                         +-------------------------+--------------------------+                  
                                                   | HTTPS / WSS                                  
                                                   v                                              
+------------------------+               +-----------------------------------+                    
|   React + Vite SPA     |  HTTPS/WSS    |  FastAPI APP (uvicorn, asyncio)   |                    
|   Dashboard            |<------------> |-----------------------------------|                    
|   LogStream (WS)       |               |  Auth (bcrypt + 30s Redis cache)  |                    
|   Anomalies, Alerts    |               |  Rate-limit (Redis token bucket)  |                    
+------------------------+               |  Routers: ingest|logs|anomalies   |                    
                                         |   |alerts|analytics|analysis|ws   |                    
                                         +-----+--------+--------+----------++                    
                                  produce       |        | read   | pub/sub                       
                                  acks=all,lz4  |        |        |                               
                                                v        v        v                               
                       +------------------------+--+  +--+---------+---+   +-------------------+  
                       |          KAFKA CLUSTER     |  |   REDIS 7      |   |   POSTGRES 15     |  
                       |----------------------------|  |----------------|   |-------------------|  
                       |  logs.raw       (12 part)  |  | hot logs (1h)  |   | tenants            |  
                       |  logs.processed (12 part)  |  | 5-min counters |   | logs (pg_trgm GIN) |  
                       |  logs.anomalies (4 part)   |  | baselines      |   | anomalies          |  
                       |  logs.alerts    (4 part)   |  | error patterns |   | alerts (UNIQUE)    |  
                       |  *.dlq          (7d)       |  | dedup + rate   |   |                    |  
                       +---+----+----+--------+-----+  | Pub/Sub fanout |   +-------------------+  
                           |    |    |        |        | analysis_credits         ^                
                           |    |    |        |        +----+----+----+           |                
                           v    v    v        v             ^    ^                |                
                    +------+--+ +-+----+ +----+-----+  +----+    |                |                
                    | log-    | |anom. | |claude-   |  | alert-  |                |                
                    | proc(12)| |det(4)| |analyzers |  | engines |                |                
                    +----+----+ +--+---+ |sem(10), 2|  | (2)     |                |                
                         |         |     +----+-----+  +----+----+                |                
                         |         |          |             |                     |                
                         | persist | produce  | HTTPS       | dedup/rate          |                
                         | + Redis | logs.    | Anthropic   | Redis SADD          |                
                         | hot path| anomalies| Claude      | INSERT alerts       |                
                         +---------+----------+-Haiku-------+                     |                
                                                            |                     |                
                                                            v                     |                
                                                   +-----------------------+      |                
                                                   |  TENANT WEBHOOK URL   |      |                
                                                   |  (Slack / PagerDuty / |      |                
                                                   |   custom HTTPS)       |      |                
                                                   +-----------------------+      |                
                                                                                  |                
                                                  PUBLISH tenant:{id}:...:stream  |                
                                                          v                       |                
                                                +-------------------+             |                
                                                | websocket-streamers <-----------+                
                                                | (4 consumers)     |                              
                                                +-------------------+                              
                                                                                                
        Health: GET /health probes Postgres + Redis + Kafka.                                      
        Metrics: Prometheus /metrics on the API and every consumer.                               
```

The ingest path **never** touches Postgres or Redis directly — `POST /api/v1/logs/ingest` returns HTTP 202 after a single Kafka enqueue. All durable side effects happen in consumer processes. Every Kafka message is keyed by `tenant_id` so a tenant's events stay on a single partition (per-tenant ordering, blast-radius isolation).

---

## Why This Is Hard

**1. High-throughput ingestion without data loss.** A naive design writes to Postgres on the request path; under 100k events/min that turns into 1,667 round-trips/sec on a single connection pool — and one slow query stalls every client. The fix is to push everything off the request path: `backend/app/api/v1/ingest.py` does fire-and-forget `publish_async(TOPICS["logs.raw"], ...)` and returns 202, while a 12-partition Kafka topic with `acks=all`, `enable.idempotence=true`, and `compression.type=lz4` (`docs/kafka_design.md` §4) absorbs bursts. The `log-processors` consumer group (12 consumers, one per partition) batches 500 rows or 1 second — whichever comes first — into a single `executemany` insert, then issues a Redis pipeline (`LPUSH`+`LTRIM 0 9999`+`EXPIRE`+`INCR`) in a single round-trip. Manual offset commit only after both side effects succeed gives at-least-once delivery; a poison pill is routed to `logs.raw.dlq` after 3 retries (`docs/kafka_design.md` §6).

**2. Two-stage anomaly detection.** Claude is too slow and too expensive to run on every log. Stage 1 (`backend/app/services/anomaly_detector.py`) is pure statistics: 5-minute Redis counters compared to a rolling 7-day baseline using configurable multipliers (`ANOMALY_VOLUME_SPIKE_MULTIPLIER`, `ANOMALY_VOLUME_DROP_MULTIPLIER`, `ANOMALY_ERROR_RATE_MULTIPLIER`). New error patterns are detected by `SISMEMBER` against a 24h-TTL Redis set of normalized templates (UUIDs, IPs, and numeric IDs stripped — see `_STRIP` in `anomaly_detector.py:70`). This runs in <5ms per check. Only confirmed statistical anomalies are forwarded to `logs.anomalies`, where Stage 2 (`backend/app/consumers/anomaly_consumer.py`) calls Claude Haiku under an `asyncio.Semaphore(10)` with a 25-second timeout, asks for structured JSON (`root_cause`, `affected_components`, `recommended_actions`, `severity_assessment`), and *attaches* it to the alert — but never blocks the alert pipeline. A Claude failure produces a templated fallback so webhooks still fire.

**3. Alert deduplication under load.** Burst anomalies (cascading service failures) fire dozens of duplicate alerts in seconds. We need exactly-one delivery per `(tenant, service, anomaly_type, hour)` even with 4 alert-engine consumers racing on the same topic. The mechanism is `redis.sadd(set_key, dedup_key)` in `backend/app/services/alert_engine.py:91` — Redis's `SADD` is atomic and returns 1 if the key was added or 0 if it already existed, so the racing consumers cannot both win. A `UNIQUE(dedup_key)` index on the `alerts` table is the durable backstop in case Redis is evicted under memory pressure. Rate limiting is a separate Redis counter capped at 10/hour/tenant; webhook delivery uses HMAC-SHA256 signatures and exponential retry backoff (1s, 5s, 25s).

---

## Tech Stack Decisions

| Component | Choice | Why Not |
|-----------|--------|---------|
| Web framework | FastAPI + uvicorn (asyncio) | Flask is sync — would need 10× the workers. Django Channels adds Daphne for WebSocket. |
| Message queue | Apache Kafka, 12 partitions on `logs.raw` | Redis Streams keeps backlog in the same Redis we use for hot reads. RabbitMQ has no native log replay. SQS has 256 KB / no ordering. |
| Hot storage | Redis 7, `maxmemory 512mb`, `allkeys-lru` | ClickHouse insert path batches; query overhead dominates on tiny result sets. TimescaleDB is just Postgres+ext — doesn't replace pub/sub. |
| Cold storage | PostgreSQL 15 + `pg_trgm` GIN | MongoDB needs Atlas Search for free-text. DynamoDB scans across services are expensive. |
| AI analysis | Claude Haiku (Anthropic) | GPT-4o-mini is comparable; Haiku has a stable structured-output story and fits the `asyncio.Semaphore(10)` budget. Local vLLM = GPU + cold-start. |
| Auth | bcrypt (cost ≥ 12) + 30s Redis cache | SHA-256 is fast but allows offline brute-force. Per-request bcrypt would cost 100–300 ms per call. |
| Rate limiting | Redis fixed-window counter | Sliding-window log is O(n) memory. Sliding-window counter doubles read cost per request. |
| ORM | SQLAlchemy 2.x async + asyncpg | Tortoise's migrations are younger. Raw asyncpg loses migrations and query DSL. |
| Realtime | WebSocket via FastAPI | SSE is one-way; bidirectional pong/heartbeat is mandated by AC-22.3. |
| Dev orchestration | Docker Compose | k8s adds PVC / ingress overhead with no observable benefit at demo scale. |

Full rationale: `docs/tech_decisions.md`.

---

## Performance Targets

| Metric | Target | Design Decision | Source |
|--------|--------|-----------------|--------|
| Ingestion latency | <10 ms p99 | Fire-and-forget Kafka publish, return 202 immediately | `app/api/v1/ingest.py:31-49` |
| Ingestion throughput | 100,000 events/min | 12 Kafka partitions × `lz4` compression × batch ingest | `docs/kafka_design.md` §7 |
| Query latency (hot) | <500 ms p99 | Redis `LRANGE` for last hour when params allow | `docs/architecture.md` §3.4 |
| Query latency (cold) | <500 ms p99 | Composite `(tenant_id, created_at DESC)` B-tree + GIN on `message` | `docs/data_models.md` §1.2 |
| Anomaly detection | <30 s end-to-end | Stage 1 statistical (<1 s) + Stage 2 Claude under `Semaphore(10)` | `app/services/anomaly_detector.py`, `app/consumers/anomaly_consumer.py` |
| Alert dedup window | 1 alert / service / anomaly_type / hour | Redis `SADD` atomic + Postgres `UNIQUE(dedup_key)` backstop | `app/services/alert_engine.py:41-95` |
| Alert rate limit | 10 alerts / tenant / hour | Redis `INCR alerts:rate:{tenant_id}` with 3600s TTL | `app/services/alert_engine.py:98-104` |
| AI analysis concurrency | 10 in-flight Claude calls | `asyncio.Semaphore(settings.CLAUDE_MAX_CONCURRENT)` | `app/consumers/anomaly_consumer.py:195` |

Full analysis (theoretical math, bottleneck breakdown, scale-up plan): `docs/performance.md`.

---

## Quick Start (5 steps)

```bash
# 1. Clone and configure
git clone <repo>
cd ai-observability-platform
cp .env.example .env
# Add ANTHROPIC_API_KEY to .env

# 2. Start infrastructure
docker compose up -d postgres redis kafka zookeeper

# 3. Run database migrations
cd backend && alembic upgrade head

# 4. Start the API
docker compose up -d fastapi log-processor anomaly-detector claude-analyzer alert-engine

# 5. Register a tenant and send your first log
curl -s -X POST http://localhost:8000/api/v1/auth/register \
  -H "Content-Type: application/json" \
  -d '{"name": "my-company", "plan_tier": "pro"}' | jq .
# Returns: {"tenant_id": "...", "api_key": "aobs_live_...", "created_at": "..."}

# Use the returned api_key:
curl -s -X POST http://localhost:8000/api/v1/logs/ingest \
  -H "X-API-Key: aobs_live_<your_key>" \
  -H "Content-Type: application/json" \
  -d '{
    "service_name": "api-gateway",
    "severity": "ERROR",
    "message": "Connection refused to database",
    "environment": "prod"
  }'
# 202 Accepted — {"event_id": "...", "status": "accepted"}
```

---

## API Reference

All authenticated endpoints require the `X-API-Key` header. Replace `$KEY` with your `aobs_live_...` key.

### Auth (public — no API key required for register)

```bash
# Register a tenant (returns api_key once, never again)
curl -X POST http://localhost:8000/api/v1/auth/register \
  -H "Content-Type: application/json" \
  -d '{"name": "acme-corp", "plan_tier": "pro"}'

# Rotate the API key (old key is rejected immediately via cache invalidation)
curl -X POST http://localhost:8000/api/v1/auth/rotate-key -H "X-API-Key: $KEY"

# Get / update tenant settings (webhook_url, retention_days, rate_limit_per_minute)
curl http://localhost:8000/api/v1/tenant/settings -H "X-API-Key: $KEY"
curl -X PATCH http://localhost:8000/api/v1/tenant/settings \
  -H "X-API-Key: $KEY" -H "Content-Type: application/json" \
  -d '{"webhook_url": "https://hooks.slack.com/services/...", "retention_days": 30}'
```

### Ingestion

```bash
# Single log (HTTP 202, p99 < 10 ms)
curl -X POST http://localhost:8000/api/v1/logs/ingest \
  -H "X-API-Key: $KEY" -H "Content-Type: application/json" \
  -d '{"service_name": "checkout", "severity": "ERROR", "message": "stripe timeout"}'

# Batch (up to 100 events per call)
curl -X POST http://localhost:8000/api/v1/logs/ingest/batch \
  -H "X-API-Key: $KEY" -H "Content-Type: application/json" \
  -d '{"events": [
    {"service_name": "checkout", "severity": "INFO",  "message": "order created"},
    {"service_name": "checkout", "severity": "ERROR", "message": "card declined"}
  ]}'
```

### Logs

```bash
# Query (Redis hot path when from_time >= now-1h, single service, no search/trace/severity filter)
curl "http://localhost:8000/api/v1/logs?service=checkout&limit=100" -H "X-API-Key: $KEY"

# Cold path (full-text search via pg_trgm GIN)
curl "http://localhost:8000/api/v1/logs?search_text=timeout&from_time=2026-05-09T00:00:00Z" \
  -H "X-API-Key: $KEY"

# Single log by id
curl http://localhost:8000/api/v1/logs/<log_id> -H "X-API-Key: $KEY"

# Live service health (Redis-backed, sub-5ms)
curl http://localhost:8000/api/v1/metrics/services -H "X-API-Key: $KEY"
```

### Anomalies

```bash
curl "http://localhost:8000/api/v1/anomalies?status=active&severity_min=0.5" -H "X-API-Key: $KEY"
curl http://localhost:8000/api/v1/anomalies/<id>      -H "X-API-Key: $KEY"
curl http://localhost:8000/api/v1/anomalies/<id>/logs -H "X-API-Key: $KEY"
curl -X PATCH http://localhost:8000/api/v1/anomalies/<id>/acknowledge -H "X-API-Key: $KEY"
curl -X PATCH http://localhost:8000/api/v1/anomalies/<id>/resolve     -H "X-API-Key: $KEY"
```

### Alerts

```bash
curl "http://localhost:8000/api/v1/alerts?severity=critical&status=delivered" -H "X-API-Key: $KEY"
curl http://localhost:8000/api/v1/alerts/<id>    -H "X-API-Key: $KEY"
curl http://localhost:8000/api/v1/alerts/stats   -H "X-API-Key: $KEY"
curl -X POST http://localhost:8000/api/v1/webhooks/test -H "X-API-Key: $KEY"
```

### Analytics (60s Redis cache)

```bash
curl http://localhost:8000/api/v1/analytics/overview                              -H "X-API-Key: $KEY"
curl http://localhost:8000/api/v1/analytics/services                              -H "X-API-Key: $KEY"
curl http://localhost:8000/api/v1/analytics/services/checkout/timeline            -H "X-API-Key: $KEY"
```

### AI Analysis (on-demand Claude Haiku)

```bash
# Root-cause analysis over a time window (max 24h, ≤50 logs sampled)
curl -X POST http://localhost:8000/api/v1/analysis/root-cause \
  -H "X-API-Key: $KEY" -H "Content-Type: application/json" \
  -d '{
    "service": "checkout",
    "from_time": "2026-05-10T10:00:00Z",
    "to_time":   "2026-05-10T11:00:00Z"
  }'

# Compare two periods (volume / error-rate delta + Claude commentary)
curl -X POST http://localhost:8000/api/v1/analysis/compare \
  -H "X-API-Key: $KEY" -H "Content-Type: application/json" \
  -d '{
    "service": "checkout",
    "period1_start": "2026-05-09T10:00:00Z",
    "period2_start": "2026-05-10T10:00:00Z",
    "hours": 1
  }'
```

### WebSocket (token via query string)

```bash
# Live log stream for one service
wscat -c "ws://localhost:8000/ws/logs/checkout?token=$KEY"

# Live anomaly stream (already enriched with claude_analysis)
wscat -c "ws://localhost:8000/ws/anomalies?token=$KEY"
# Server sends {"type":"log","data":{...}}, plus {"type":"ping","ts":...} every 30s.
```

### Health & Metrics

```bash
curl http://localhost:8000/health    # 200 healthy / 503 degraded — probes Postgres, Redis, Kafka
curl http://localhost:8000/metrics   # Prometheus exposition
```

---

## Running Tests

```bash
cd backend
python3.12 -m pytest tests/ -v
# 5 test modules: test_ingestion, test_anomaly_detection, test_alert_engine,
# test_query_api, test_integration — covering ingest fire-and-forget, dedup
# atomicity, baseline math, hot/cold query routing, end-to-end pipeline.
```

---

## Load Testing

```bash
cd load_tests
./run_tests.sh baseline  # 50 users, 60s,  target 5,000+ req/min
./run_tests.sh stress    # 200 users, 120s, target 100,000+ events/min (batch tag only)
./run_tests.sh mixed     # 100 users, 180s, target p95 <100ms on queries
./run_tests.sh all       # all three sequentially; reports under load_tests/results/<ts>/
```

The stress scenario uses the `--tags ingestion` Locust tag so only batch ingestion runs; mixed exercises queries, analytics, anomalies, and ingest in realistic ratios. Theoretical math and bottleneck analysis: `docs/performance.md`.

---

## Project Structure

```
ai-observability-platform/
├── backend/                       FastAPI app + Kafka consumers
│   ├── main.py                    ASGI entrypoint, lifespan, middleware, /health
│   ├── app/
│   │   ├── api/v1/                Routers: ingest, logs, anomalies, alerts, analytics, analysis, websocket, tenants
│   │   ├── consumers/             log_consumer.py, anomaly_consumer.py (Kafka consumer groups)
│   │   ├── core/                  config (Pydantic BaseSettings), auth, database, redis_client, kafka_client
│   │   ├── models/                SQLAlchemy 2.x ORM: tenants, logs, anomalies, alerts
│   │   ├── schemas/               Pydantic request/response models
│   │   └── services/              anomaly_detector, alert_engine, log_service, webhook_deliverer
│   ├── alembic/                   migrations
│   └── tests/                     pytest: 5 modules, fixtures in conftest.py
├── load_tests/                    locustfile.py + run_tests.sh (baseline / stress / mixed)
├── docs/                          architecture, tech_decisions, performance, kafka_design,
│                                  data_models, requirements, interview_prep
├── infra/                         Kafka bootstrap, Postgres init scripts
├── docker-compose.yml             postgres, redis, zookeeper, kafka, fastapi, all consumer roles
└── .env.example                   ANTHROPIC_API_KEY, DATABASE_URL, REDIS_URL, KAFKA_BOOTSTRAP_SERVERS, …
```

---

## Architecture Deep Dive

- `docs/architecture.md` — full system design, all 5 data flows, component-by-component breakdown, failure handling, deployment topology.
- `docs/kafka_design.md` — every topic, partition count, retention, consumer group, producer/consumer config, DLQ pattern.
- `docs/data_models.md` — Postgres DDL with every index, all 13 Redis key patterns with TTLs.
- `docs/tech_decisions.md` — every major choice with the alternative considered and the tradeoff at scale.
- `docs/performance.md` — theoretical throughput math, 5 named bottlenecks with 10× scale-up plans.
- `docs/interview_prep.md` — 7 deep-dive topics for systems-design interview prep.
