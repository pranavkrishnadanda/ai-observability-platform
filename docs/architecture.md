# System Architecture

This document is the authoritative architecture reference for the AI Observability Platform.
All component boundaries, message flows, and storage responsibilities described here are
binding on the implementation.

---

## 1. System Diagram

```
                                                                                                        
                          +----------------------------------------------------+                        
                          |                  CUSTOMER APPLICATIONS              |                        
                          |   (SDKs / curl / log shippers, send X-API-Key)      |                        
                          +-------------------------+--------------------------+                        
                                                    | HTTPS                                              
                                                    | POST /logs/ingest, /logs/ingest/batch              
                                                    | GET  /logs, /anomalies, /alerts, /analytics        
                                                    | WS   /ws/logs, /ws/anomalies                       
                                                    v                                                    
+--------------------------+              +--------------------------------------+                       
|       BROWSER            |  HTTPS/WSS   |   FastAPI APP (uvicorn, asyncio)     |                       
|   React + Vite SPA       |<------------>|--------------------------------------|                       
|   - Dashboard            |              |  Middleware: auth (X-API-Key /       |                       
|   - LogStream (WS)       |              |    bcrypt verify, tenant_id ctx),    |                       
|   - AnomalyList          |              |    rate limit (Redis token bucket),  |                       
|   - AlertList            |              |    request id, prometheus            |                       
|   - AnalyticsDashboard   |              |--------------------------------------|                       
+--------------------------+              |  Routers (api/v1):                   |                       
                                          |   ingest | logs | anomalies |        |                       
                                          |   alerts | analytics | analysis |    |                       
                                          |   websocket | tenants            |   |                       
                                          +-----+--------+--------+----------+--+                       
                                                |        |        |          |                          
                          produce (lz4, acks=all)|        | read   | read     | pub/sub                  
                                                |        |        |          |                          
                                                v        v        v          v                          
                  +-----------------------------+--+  +--+--------+-+   +----+------------+              
                  |          KAFKA CLUSTER          |  |   REDIS    |   |   POSTGRES 15   |              
                  |---------------------------------|  | (asyncio)  |   |---------------- |              
                  | logs.raw          (12 part, 1h) |  |------------|   | tenants         |              
                  | logs.processed    (12 part, 30m)|  | hot logs   |   | logs (pg_trgm)  |              
                  | logs.anomalies    ( 4 part, 4h) |  | counters   |   | anomalies       |              
                  | logs.alerts       ( 4 part, 4h) |  | baselines  |   | alerts (UNIQUE) |              
                  | *.dlq             (7d)          |  | error pats |   |                 |              
                  +---+---+----+-------+-------+----+  | dedup/rate |   +-----------------+              
                      |   |    |       |       |      | pub/sub    |          ^   ^                     
                      |   |    |       |       |      | services   |          |   |                     
                      |   |    |       |       |      | credits    |          |   |                     
                      |   |    |       |       |      +---+----+---+          |   |                     
                      |   |    |       |       |          ^    ^              |   |                     
                      |   |    |       |       |          |    |              |   |                     
                      v   v    v       v       v          |    |              |   |                     
              +-------+---+ +--+----+ +-+--------+ +-------+    |              |   |                     
              | log-      | |anomaly| |claude-   | |alert-      |              |   |                     
              | processors| |detect.| |analyzers | |engines     |              |   |                     
              | (12 cons) | |(4)    | |(2,sem=10)| |(2)         |              |   |                     
              +-----+-----+ +---+---+ +----+-----+ +-+----+-----+              |   |                     
                    |           |          |         |    |                    |   |                     
                    | persist   |producesi |HTTPS    | dedup/rate (Redis)      |   |                     
                    | + Redis   |logs.anom |Anthropic|                         |   |                     
                    | + produce |          |Claude   | INSERT alert            |   |                     
                    | logs.proc |          |Haiku    |                         |   |                     
                    |           |          |         |                         |   |                     
                    +-----------+----------+---------+-------------------------+   |                     
                                |                    |                              |                     
                                |                    | HTTP POST (HMAC-SHA256)      |                     
                                |                    v                              |                     
                                |          +-----------------------+                |                     
                                |          | TENANT WEBHOOK URL    |                |                     
                                |          | (Slack / PagerDuty /  |                |                     
                                |          |  custom HTTPS)        |                |                     
                                |          +-----------------------+                |                     
                                |                                                   |                     
                                v                                                   |                     
                       +-------------------+                                        |                     
                       | websocket-        |  PUBLISH tenant:{id}:...:stream        |                     
                       | streamers (4)     |---------------------------------------->                     
                       +-------------------+                                                              
                                                                                                        
        Reapers/Cron:                                                                                    
          - logs reaper (daily, deletes rows older than tenants.retention_days)                           
          - baseline refresher (rolling 7-day EWMA writeback to Redis)                                    
          - DLQ inspector CLI (manual)                                                                    
                                                                                                        
        Observability:                                                                                   
          - Prometheus /metrics exporter on FastAPI + each consumer                                      
          - Health: GET /health probes Postgres + Redis + Kafka                                          
```

---

## 2. Component Descriptions

### 2.1 FastAPI Application (`backend/main.py` + `backend/app/api/v1/*`)
- ASGI app served by `uvicorn` workers behind a reverse proxy in production.
- Single process hosts:
  - Public REST API (`/api/v1/...`)
  - WebSocket endpoints (`/ws/logs/{service}`, `/ws/anomalies`)
  - Health endpoint (`/health`)
  - Prometheus metrics endpoint (`/metrics`)
- Middleware stack (outermost → innermost):
  1. `RequestIdMiddleware` — assigns `X-Request-ID`.
  2. `PrometheusMiddleware` — counts requests, observes latencies.
  3. `AuthMiddleware` (`app/core/auth.py`) — extracts `X-API-Key`, verifies via bcrypt, caches tenant for the request lifetime in `request.state.tenant`. Skipped for `/health`, `/api/v1/auth/register`, and `/metrics`.
  4. `RateLimitMiddleware` — Redis-backed token bucket per tenant per minute.
- Critical guarantees:
  - Ingest path NEVER writes to Postgres or Redis directly; it only produces to Kafka (preserves p99 < 10 ms NFR-2).
  - Query path NEVER reads Kafka; only Redis (hot) and Postgres (cold).
  - All handlers derive `tenant_id` from `request.state.tenant.id`, never from the request body (NFR-6).

### 2.2 Kafka (3-broker cluster in prod, single-broker dev)
- Durable buffer between ingest API and downstream processing.
- 5 source topics + 4 DLQs (see `docs/kafka_design.md`).
- Producer config: `acks=all`, `enable.idempotence=true`, `compression.type=lz4`, `linger.ms=10`.
- Consumer config: manual commit, `max.poll.records=500`, `auto.offset.reset=latest`.
- Message key on every topic is `tenant_id` so all events for a tenant land on the same partition (preserves per-tenant ordering and isolates noisy neighbors to specific partitions).

### 2.3 Redis 7 (single primary, optional replica)
- Configured `maxmemory 512mb`, `maxmemory-policy allkeys-lru`.
- Six categories of keys (see `docs/data_models.md` §2):
  1. Hot log lists (1-hour ring buffer of last 10k logs/service).
  2. 5-minute volume + error counters.
  3. Rolling baselines for anomaly detection.
  4. Known error pattern set.
  5. Alert dedup set + alert rate counter + per-tenant API rate counter.
  6. Pub/Sub channels for WebSocket fan-out.
- Async client: `redis-py` 5.x with connection pool sized to `2 × cpu_count`.

### 2.4 PostgreSQL 15
- Cold-tier durable store. Four tables: `tenants`, `logs`, `anomalies`, `alerts`.
- Extensions: `pg_trgm` (free-text log search via GIN), `pgcrypto` (`gen_random_uuid`).
- Connection pool: `asyncpg` driver via SQLAlchemy 2.x async, `pool_size=20`, `max_overflow=10`.
- Reaper job (`backend/app/services/log_service.py::reap_old_logs`) runs daily and deletes rows older than `tenants.retention_days`.

### 2.5 Claude Haiku (Anthropic API)
- Called from `claude-analyzers` consumer (`app/consumers/anomaly_consumer.py`) and from on-demand `/analysis/root-cause` and `/analysis/compare` endpoints.
- Concurrency capped at 10 with an `asyncio.Semaphore` (NFR-10).
- Per-call timeout 25 s; on failure a templated fallback `claude_analysis` is used and the alert still flows.
- Per-tenant per-day usage tracked in `tenant:{tenant_id}:analysis_credits:{epoch_day}`.

### 2.6 React Frontend (`frontend/`)
- Vite + React 18 + TypeScript SPA. No SSR.
- `frontend/src/api/client.ts` — Axios client that injects `X-API-Key` from local storage.
- `frontend/src/hooks/useWebSocket.ts` — reconnecting WebSocket hook with exponential backoff.
- 5 page components (Dashboard, Logs, Anomalies, Alerts, Settings) compose 5 reusable component widgets.
- Dev served by `vite`; prod served by Nginx in the `frontend` Docker image.

### 2.7 WebSocket Subsystem
- FastAPI `WebSocketRoute` per endpoint.
- On connect: validate `?token=...` against bcrypt hash → resolve tenant → subscribe to Redis pub/sub channel scoped by `tenant_id`.
- Server pushes one frame per Redis message; sends `{"type": "ping"}` every 30 s.
- Backpressure: per-connection queue (`asyncio.Queue(maxsize=1000)`); overflow disconnects with code `1013`.
- WebSocket connections never read Kafka directly — they read Redis Pub/Sub (so a dropped client cannot stall log persistence).

---

## 3. Data Flows

### 3.1 Use Case 1 — Log Ingestion (single + batch)

```
client                ingest API              Kafka                log-processors           PG / Redis            websocket-streamers      WS clients
  │  POST /logs/ingest    │                       │                       │                       │                       │                       │
  │  X-API-Key: ...       │                       │                       │                       │                       │                       │
  │──────────────────────>│                       │                       │                       │                       │                       │
  │                       │ AuthMiddleware: bcrypt verify(X-API-Key, tenants.api_key_hash)        │                       │                       │
  │                       │ RateLimitMiddleware: INCR rate:{tid}:{minute} (Redis)                 │                       │                       │
  │                       │ Pydantic validate body (severity enum, env enum, lengths)             │                       │                       │
  │                       │ envelope = {tenant_id, service_name, severity, message, metadata,     │                       │                       │
  │                       │             trace_id, span_id, source_ip, environment, ingested_at}   │                       │                       │
  │                       │ producer.send("logs.raw", key=tenant_id, value=envelope)              │                       │                       │
  │                       │──────── await ack (acks=all, idempotent) ────>│                       │                       │                       │
  │  HTTP 202             │<───────────────────── ack ────────────────────│                       │                       │                       │
  │  {event_id, accepted} │                       │                       │                       │                       │                       │
  │<──────────────────────│                       │                       │                       │                       │                       │
  │                       │                       │ poll batch (≤500 or 1s)                       │                       │                       │
  │                       │                       │──────────────────────>│                       │                       │                       │
  │                       │                       │                       │ assign log_id, persisted_at                   │                       │
  │                       │                       │                       │ INSERT logs ... (executemany, 500 rows)       │                       │
  │                       │                       │                       │ ─────────────────────>│ PostgreSQL            │                       │
  │                       │                       │                       │ pipeline: LPUSH+LTRIM+EXPIRE+INCR             │                       │
  │                       │                       │                       │   tenant:{tid}:service:{svc}:logs            │                       │
  │                       │                       │                       │   tenant:{tid}:service:{svc}:vol:{5min}      │                       │
  │                       │                       │                       │   tenant:{tid}:service:{svc}:errors:{5min}   │                       │
  │                       │                       │                       │   SADD tenant:{tid}:services                 │                       │
  │                       │                       │                       │ ─────────────────────>│ Redis                 │                       │
  │                       │                       │                       │ produce("logs.processed", same+log_id)        │                       │
  │                       │                       │                       │──────────────────────>│ Kafka                 │                       │
  │                       │                       │                       │ commit() Kafka offsets (manual, at-least-once)                        │
  │                       │                       │                       │                       │                       │ poll logs.processed   │
  │                       │                       │                       │                       │                       │<──────────────────────│
  │                       │                       │                       │                       │                       │ PUBLISH tenant:{tid}:service:{svc}:stream
  │                       │                       │                       │                       │                       │ ─────────> Redis Pub/Sub
  │                       │                       │                       │                       │                       │           │           │
  │                       │                       │                       │                       │                       │           │ message   │
  │                       │                       │                       │                       │                       │           ▼           │
  │                       │                       │                       │                       │                       │  subscribed WS pushes ─> {"type":"log","data":...}
  │                       │                       │                       │                       │                       │                       │
```

Failure handling:
- Kafka unreachable → ingest returns HTTP 503 (NFR-12). No silent drop.
- Schema-invalid message inside log-processors → routed to `logs.raw.dlq`, offset committed.
- Postgres failure → batch retried up to 3× (exponential 100ms, 500ms, 2.5s) → on final failure, DLQ.
- Redis failure inside log-processors → logged, hot-path writes skipped, message still produced to `logs.processed` (NFR-12 graceful degradation).

### 3.2 Use Case 2 — Statistical Anomaly Detection + Claude Analysis

```
anomaly-detectors         Redis                 PG anomalies          Kafka                 claude-analyzers        Anthropic
  │ poll logs.processed       │                      │                     │                       │                       │
  │ (consumer just keeps      │                      │                     │                       │                       │
  │ counters fresh; eval is   │                      │                     │                       │                       │
  │ tick-based per tenant×svc)│                      │                     │                       │                       │
  │                           │                      │                     │                       │                       │
  │ ── every 30s tick ──>     │                      │                     │                       │                       │
  │   for each (tenant,svc) seen in last 30s:        │                     │                       │                       │
  │     vol_now    = sum INCR vol:{tid}:{svc}:{last 5m buckets}            │                       │                       │
  │     err_now    = sum INCR errors:{tid}:{svc}:{last 5m buckets}         │                       │                       │
  │     baseline   = GET tenant:{tid}:service:{svc}:baseline:volume        │                       │                       │
  │                  GET tenant:{tid}:service:{svc}:baseline:error_rate    │                       │                       │
  │     z_vol      = (vol_now - baseline.mean) / baseline.stddev           │                       │                       │
  │     z_err      = ...                                                   │                       │                       │
  │     templates  = normalized(error messages observed in window)         │                       │                       │
  │     new_pat?   = SISMEMBER error_patterns each → false                 │                       │                       │
  │   if |z_vol| ≥ 3 or |z_err| ≥ 3 or new_pat:                            │                       │                       │
  │     compute severity_score                                             │                       │                       │
  │     INSERT anomalies(...)                          │                   │                       │                       │
  │ ─────────────────────────>│ ──────────────────>   │                     │                       │                       │
  │     SADD error_patterns (new templates)            │                     │                       │                       │
  │ ─────────────────────────>│                      │                     │                       │                       │
  │     produce logs.anomalies(anomaly_id, sample_log_ids, ...)             │                       │                       │
  │ ─────────────────────────────────────────────────────────────────────> │                       │                       │
  │     commit Kafka offsets for logs.processed                             │                       │                       │
  │                           │                      │                     │  poll logs.anomalies  │                       │
  │                           │                      │                     │ ─────────────────────>│                       │
  │                           │                      │                     │                       │ async with sem(10):   │
  │                           │                      │ SELECT logs WHERE id = ANY(sample_log_ids)   │                       │
  │                           │                      │<────────────────────────────────────────────│                       │
  │                           │                      │                     │                       │ POST /v1/messages     │
  │                           │                      │                     │                       │ ─────────────────────>│
  │                           │                      │                     │                       │ (timeout 25 s)        │
  │                           │                      │                     │                       │<──── analysis text ───│
  │                           │                      │ UPDATE anomalies SET claude_analysis=...     │                       │
  │                           │                      │<────────────────────────────────────────────│                       │
  │                           │ INCR analysis_credits:{tid}:{epoch_day}                            │                       │
  │                           │<─────────────────────────────────────────────────────────────────  │                       │
  │                           │ PUBLISH tenant:{tid}:anomalies:stream (anomaly object incl. analysis)                       │
  │                           │<─────────────────────────────────────────────────────────────────  │                       │
  │                           │                      │                     │ produce logs.alerts   │                       │
  │                           │                      │                     │<──────────────────────│                       │
  │                           │                      │                     │ commit logs.anomalies offsets                  │
```

Failure handling:
- Claude timeout / 5xx → fallback `claude_analysis = "Statistical anomaly detected: <type> on <service>. Observed=<val>, baseline=<val>, deviation=<pct>%."` is used, message is still published to `logs.alerts` (NFR-12, AC-9.3).
- Claude consistent 4xx → DLQ `logs.anomalies.dlq` after 3 retries.
- INSERT anomalies UNIQUE collision (same window) → skip without error.

### 3.3 Use Case 3 — Alert Delivery

```
alert-engines             Redis                  PG alerts              tenant webhook
  │ poll logs.alerts            │                      │                       │
  │ msg = anomaly + claude_analysis                    │                       │
  │                             │                      │                       │
  │ dedup_key = sha256(tid:svc:type:floor(now/3600))   │                       │
  │ SISMEMBER alerts:dedup:{tid} dedup_key             │                       │
  │ ───────────────────────────>│                      │                       │
  │ <── 1 (hit)                 │                      │                       │
  │     metrics.alerts_dropped_dedup += 1              │                       │
  │     commit offset, return                          │                       │
  │ <── 0 (miss)                │                      │                       │
  │ INCR alerts:rate:{tid} (TTL 3600 if new)           │                       │
  │ ───────────────────────────>│                      │                       │
  │ <── n                       │                      │                       │
  │ if n > 10:                                         │                       │
  │   INSERT alerts(... delivery_status='pending', last_error='rate_limited')  │
  │   ────────────────────────────────────────────────>│                       │
  │   commit offset, return                            │                       │
  │ else:                                              │                       │
  │   SADD alerts:dedup:{tid} dedup_key; EXPIRE 3600   │                       │
  │   ──────────────────────────>│                     │                       │
  │   severity = bucketize(severity_score)             │                       │
  │   INSERT alerts(..., delivery_status='pending', dedup_key=...)             │
  │   UNIQUE(dedup_key) defends against race; on conflict → drop, commit       │
  │   ────────────────────────────────────────────────>│                       │
  │   body  = {alert_id, tenant_id, service_name, anomaly_type, severity,      │
  │            title, description, claude_analysis, detected_at}                │
  │   sig   = HMAC-SHA256(api_key, body) → "X-Signature: sha256=<hex>"          │
  │   POST tenants.webhook_url (timeout 10s)                                   │
  │   ─────────────────────────────────────────────────────────────────────────>
  │   <── 2xx                                                                  │
  │     UPDATE alerts SET delivery_status='delivered', delivered_at=NOW()      │
  │     ──────────────────────────────────────────────>│                       │
  │   <── non-2xx / timeout                                                    │
  │     retry with exponential backoff (1s, 5s, 25s)                           │
  │     after 3 failures: UPDATE alerts SET delivery_status='failed',          │
  │                       retry_count=3, last_error='...'                      │
  │   commit Kafka offset                                                      │
```

Notes:
- Redis dedup is the fast path; the Postgres `UNIQUE(dedup_key)` is the source of truth (handles Redis eviction).
- Slack-compatible payload (AC-12.5) is produced by an optional transform when `webhook_url` matches `*.slack.com/*` — same body fields are wrapped into Slack block format.

### 3.4 Use Case 4 — Log Query (`GET /api/v1/logs`)

```
client                  FastAPI              Redis                  PostgreSQL
  │ GET /api/v1/logs?service=...&from_time=...&limit=100              │
  │ X-API-Key: ...                                                    │
  │─────────────────>│                                                │
  │                  │ Auth middleware → tenant_id                    │
  │                  │ Pydantic parse query                           │
  │                  │ if (from_time ≥ now-1h) AND service set        │
  │                  │    AND severity not set AND search_text empty  │
  │                  │    AND trace_id not set AND offset==0:         │
  │                  │   key = tenant:{tid}:service:{svc}:logs        │
  │                  │   LRANGE key 0 limit-1                         │
  │                  │ ────────────────>│                             │
  │                  │ <─── JSON list ──│                             │
  │                  │   filter by from_time/to_time in process,      │
  │                  │   if returned ≥ limit → respond                │
  │                  │ else (cold path):                              │
  │                  │   SELECT id, service_name, severity, message,  │
  │                  │          metadata, trace_id, span_id,          │
  │                  │          environment, created_at, ingested_at  │
  │                  │   FROM logs                                    │
  │                  │   WHERE tenant_id=$1                           │
  │                  │     AND (service_name=$2 OR $2 IS NULL)        │
  │                  │     AND (severity=$3 OR $3 IS NULL)            │
  │                  │     AND created_at BETWEEN $4 AND $5           │
  │                  │     AND (message %% $6 OR $6 IS NULL)          │
  │                  │     AND (trace_id=$7 OR $7 IS NULL)            │
  │                  │   ORDER BY created_at DESC                     │
  │                  │   LIMIT $8 OFFSET $9                           │
  │                  │ ──────────────────────────────────>│           │
  │                  │ <───── rows + count(*) over() ─────│           │
  │                  │ build {data, total, page_info}                 │
  │ HTTP 200 ────────│                                                │
```

Notes:
- Hot path used only when query params fit the cached shape (recent + per-service + minimal filters); otherwise straight to Postgres.
- `total` is computed via `count(*) over()` window function in same query to keep p99 < 500 ms (NFR-3).
- `pg_trgm` GIN index handles `message %% search_text`.

### 3.5 Use Case 5 — WebSocket Streaming

```
browser                FastAPI WS              Redis Pub/Sub
  │ WS /ws/logs/checkout-api?token=aobs_live_...
  │────────────────────>│
  │                     │ extract token, bcrypt verify against tenants.api_key_hash
  │                     │ if invalid OR is_active=false → close 4401
  │                     │ tenant_id resolved
  │                     │ channel = f"tenant:{tid}:service:checkout-api:stream"
  │                     │ pubsub = redis.pubsub(); pubsub.subscribe(channel)
  │                     │─────────────────────────────────>│
  │                     │ accept(); spawn 2 tasks: send_loop, recv_loop
  │                     │ also spawn heartbeat_task: every 30s send {"type":"ping"}
  │                     │
  │                     │  send_loop:
  │                     │    async for msg in pubsub.listen():
  │                     │      queue.put_nowait({"type":"log","data":msg})
  │                     │      if queue.qsize() > 1000: close(1013); break
  │                     │
  │                     │  drain queue → ws.send_json
  │                     │
  │ <── {"type":"log","data":{...}}
  │ <── {"type":"log","data":{...}}
  │ <── {"type":"ping"}
  │ ── {"type":"pong"} ──>
  │ ...
  │
  │ if no client message in 90s → close 1011
  │ on disconnect: pubsub.unsubscribe(); close redis pubsub conn
```

`/ws/anomalies` is identical except it subscribes to `tenant:{tid}:anomalies:stream`, which is published to from the `claude-analyzers` consumer (after analysis is attached) so the browser sees the enriched anomaly object.

---

## 4. Cross-Cutting Concerns

### 4.1 Authentication
- API key format: `aobs_live_<32 base62 chars>` generated via `secrets.token_urlsafe(32)`.
- bcrypt cost ≥ 12; verification cached per-request via `request.state.tenant`.
- WebSocket auth uses query-string `?token=` validated identically.

### 4.2 Multi-Tenancy Isolation (NFR-6)
- Every Postgres query carries `WHERE tenant_id = $1`.
- Every Redis key is prefixed `tenant:{tenant_id}:...`.
- Every Kafka message keyed by `tenant_id` (so partition assignment is tenant-stable).
- WebSocket channels are tenant-scoped Redis Pub/Sub names.
- `tenant_id` is derived from API key only — never read from request body.

### 4.3 Rate Limiting
- API rate limit: Redis token-bucket-equivalent counter per minute (`rate:{tid}:{minute}`), capped at `tenants.rate_limit_per_minute`.
- Alert rate limit: 10 alerts/hour/tenant via `alerts:rate:{tid}` (NFR-8).
- Both use Redis `INCR` + conditional `EXPIRE` (atomic via Lua script).

### 4.4 Backpressure & Degradation (NFR-12)
| Failed dependency | Ingest API | Query API | WebSocket | Consumers |
|-------------------|------------|-----------|-----------|-----------|
| Redis down | 200 (skip hot path), `/health=degraded` | 503 for hot-path-only routes | close 1011 | log warn, skip Redis ops |
| Kafka down | 503 | 200 | unaffected | back off and retry |
| Postgres down | 503 | 503 | unaffected | retry batch, then DLQ |

### 4.5 Observability
- Prometheus metrics exposed by the FastAPI app and by every consumer process.
- Standard metrics: `ingest_requests_total`, `ingest_duration_seconds`, `query_duration_seconds`, `kafka_consumer_lag`, `claude_call_duration_seconds`, `webhook_delivery_total{status}`, `redis_op_duration_seconds`, `pg_pool_in_use`.
- Structured JSON logging via `structlog`; every log carries `tenant_id` and `request_id` when in request context.

### 4.6 Configuration
- Pydantic `BaseSettings` reads from `.env` and environment.
- All credentials, broker URLs, and Anthropic API keys come from env (NFR-13).

---

## 5. Deployment Topology (Docker Compose for dev/demo)

```
docker-compose.yml services:
  postgres        : postgres:15-alpine (volume: pgdata)
  redis           : redis:7-alpine (cmd flags: --maxmemory 512mb --maxmemory-policy allkeys-lru)
  zookeeper       : confluentinc/cp-zookeeper:7.6.0
  kafka           : confluentinc/cp-kafka:7.6.0
  kafka-bootstrap : one-shot job that creates topics + DLQs
  backend-api     : uvicorn FastAPI on :8000
  log-processor   : python -m app.consumers.log_consumer (replicas: 4 for demo)
  anomaly-detector: python -m app.consumers.anomaly_consumer --mode=detector
  claude-analyzer : python -m app.consumers.anomaly_consumer --mode=claude
  alert-engine    : python -m app.consumers.anomaly_consumer --mode=alert
  websocket-streamer: python -m app.consumers.log_consumer --mode=stream
  frontend        : nginx serving the built React bundle on :3000
```

Production uses the same images on Kubernetes (one Deployment per consumer role, HPA on Kafka lag); see `docs/tech_decisions.md` §9 for the rationale on Compose vs K8s.

---

## 6. Sequence Summary (one paragraph per flow)

1. **Ingest:** client POST → FastAPI validates and produces to `logs.raw` → 202 returned within 10 ms p99 → log-processors batch-INSERT to Postgres, write Redis hot path, register service, produce to `logs.processed` → websocket-streamers fan out via Redis Pub/Sub → connected browsers receive frames.
2. **Anomaly:** anomaly-detectors tick every 30 s per (tenant, service), comparing current Redis counters to baselines, INSERT anomalies, produce to `logs.anomalies` → claude-analyzers (semaphore=10) call Claude Haiku, UPDATE `claude_analysis`, produce to `logs.alerts`, also publish on `tenant:{tid}:anomalies:stream` for the UI.
3. **Alert:** alert-engines consume `logs.alerts`, dedup against Redis set + Postgres UNIQUE, rate-limit at 10/hour/tenant, INSERT `alerts`, POST signed webhook with retries (1s/5s/25s).
4. **Query:** client GET hits FastAPI → tries Redis hot path when params allow → falls back to Postgres `pg_trgm` query → returns `{data,total,page_info}` < 500 ms p99.
5. **WebSocket:** client connects, server validates token, subscribes to tenant-scoped Redis Pub/Sub channel, pushes frames with 30 s heartbeat and 1000-message backpressure cap.
