# AI Observability Platform

> Distributed log ingestion, real-time AI anomaly detection, and intelligent alerting — built to handle 100,000+ events/minute.

---

## Table of Contents

- [How It Works](#how-it-works)
- [Running Everything (Docker — Recommended)](#running-everything-docker--recommended)
- [Running for Development (Local)](#running-for-development-local)
- [First Login & Getting an API Key](#first-login--getting-an-api-key)
- [Watching the UI ↔ Backend Connection](#watching-the-ui--backend-connection)
- [Sending Test Data](#sending-test-data)
- [Architecture](#architecture)
- [API Reference](#api-reference)
- [Running Tests](#running-tests)
- [Stack](#stack)

---

## How It Works

The platform has four moving parts that work together as a pipeline:

```
Your App / curl
      │
      │  POST /api/v1/logs/ingest  ← returns 202 immediately
      ▼
┌─────────────┐     produces      ┌──────────────────────┐
│  FastAPI    │ ─────────────────▶│  Kafka (3 brokers)   │
│  :8000      │                   │  topic: logs.raw     │
└─────────────┘                   └──────────┬───────────┘
      │  WebSocket /ws/logs/*                 │ consumes
      │  (live stream to browser)             ▼
      │                           ┌──────────────────────┐
      │                           │   log-consumer       │
      │                           │   writes to:         │
      │                           │   • Redis  (hot 1hr) │
      │                           │   • PostgreSQL (cold)│
      │                           └──────────┬───────────┘
      │                                      │ publishes
      │                                      ▼  logs.anomalies
      │                           ┌──────────────────────┐
      │                           │  anomaly-consumer    │
      │                           │  calls Claude Haiku  │
      │                           │  for root cause      │
      │                           └──────────┬───────────┘
      │                                      │ publishes
      │                                      ▼  logs.alerts
      │                           ┌──────────────────────┐
      │                           │   alert-engine       │
      │                           │   dedup + webhook    │
      │                           └──────────────────────┘
      │
      ▼
┌─────────────┐
│  React UI   │  polls REST every 10-60s + WebSocket live logs
│  :3000      │
└─────────────┘
```

**What each piece does:**

| Service | What it does |
|---|---|
| **FastAPI** | Receives logs, authenticates tenants, exposes REST + WebSocket APIs, runs statistical anomaly detection every 30s |
| **log-consumer** | Reads raw logs from Kafka, writes to Redis (fast reads) and PostgreSQL (durable storage) |
| **anomaly-consumer** | Reads confirmed anomalies from Kafka, calls Claude Haiku for root cause analysis, writes result back to DB |
| **alert-engine** | Reads alerts from Kafka, deduplicates (1 per service per hour via Redis), sends HMAC-signed webhook |
| **React UI** | Shows live log stream via WebSocket, anomaly cards with Claude's analysis, service health grid with sparklines |

**End-to-end flow for a single log:**
1. Your app POSTs a log → FastAPI returns `202` in <5ms
2. Log is produced to Kafka `logs.raw` with lz4 compression
3. `log-consumer` picks it up, writes to Redis and PostgreSQL, publishes to `logs.processed`
4. Every 30s, FastAPI's background scheduler checks if any service has a volume spike, drop, or error rate surge vs the 7-day rolling baseline
5. If an anomaly is detected, it's published to `logs.anomalies`
6. `anomaly-consumer` calls Claude Haiku with the log context → gets a structured JSON root cause
7. If alert conditions are met, published to `logs.alerts`
8. `alert-engine` deduplicates and optionally sends a webhook to your configured URL
9. The React dashboard shows the anomaly card with Claude's analysis in real time

---

## Running Everything (Docker — Recommended)

**Prerequisites:** Docker Desktop running. That's it.

```bash
git clone https://github.com/pranavkrishnadanda/ai-observability-platform
cd ai-observability-platform
```

The `backend/.env` is already configured. If you have your own Anthropic API key, add it:
```bash
# Edit backend/.env and set:
# ANTHROPIC_API_KEY=sk-ant-...
```

Start everything:
```bash
docker compose up -d
```

Docker starts 10 services in dependency order. Kafka takes ~60 seconds to elect leaders. Watch progress:
```bash
docker compose ps
# Wait until fastapi shows (healthy)
```

Once fastapi is healthy, open **http://localhost:3000** in your browser.

You'll see a login screen — click **Register**, optionally enter an org name, and click **Create Account & Connect**. The platform creates a tenant and logs you straight into the dashboard.

**To stop everything:**
```bash
docker compose down
# To also wipe all data (postgres volume):
docker compose down -v
```

---

## Running for Development (Local)

Use this when you want hot-reload on backend or frontend code changes.

### Prerequisites

- Python 3.12 — install via `pyenv install 3.12`
- [uv](https://docs.astral.sh/uv/) — `curl -LsSf https://astral.sh/uv/install.sh | sh`
- [bun](https://bun.sh) — `curl -fsSL https://bun.sh/install | bash`
- Docker Desktop (for Postgres, Redis, Kafka)

### Step 1 — Start infrastructure only

```bash
docker compose up -d postgres redis zookeeper kafka-1 kafka-2 kafka-3 kafka-init
# Wait ~90s for Kafka to be ready
docker compose ps   # all should show (healthy)
```

> **Note:** If you have Homebrew's PostgreSQL installed and running, stop it first or it will steal port 5432:
> ```bash
> brew services stop postgresql@14   # or @15, @16 — whatever version you have
> ```

### Step 2 — Run database migrations

```bash
cd backend
uv sync --extra dev
uv run alembic upgrade head
# Should print: Running upgrade -> 001_initial
```

### Step 3 — Start the backend

```bash
# Still in backend/
uv run uvicorn main:app --reload --port 8000
```

You should see:
```
INFO:     Application startup complete.
INFO     main — Background schedulers started
INFO:     Uvicorn running on http://0.0.0.0:8000
```

### Step 4 — Start the workers (each in its own terminal)

```bash
# Terminal 2 — reads logs from Kafka, writes to DB
cd backend && uv run python -m app.consumers.log_consumer

# Terminal 3 — calls Claude on anomalies
cd backend && uv run python -m app.consumers.anomaly_consumer

# Terminal 4 — deduplicates and sends alerts
cd backend && uv run python -m app.services.alert_engine
```

### Step 5 — Start the frontend

```bash
cd frontend
bun install   # first time only
bun run dev
# Open http://localhost:3000
```

Vite proxies `/api/*` and `/ws/*` to the backend at port 8000, so CORS is never an issue in development.

---

## First Login & Getting an API Key

When you open http://localhost:3000 for the first time, you'll see the login screen.

**Option A — Register from the UI (easiest):**
1. Click the **Register** tab
2. Enter an org name (or leave blank for a random one)
3. Click **Create Account & Connect** — you're in

**Option B — Register via curl:**
```bash
curl -s -X POST http://localhost:8000/api/v1/auth/register \
  -H "Content-Type: application/json" \
  -d '{"name": "my-org"}' | python3 -m json.tool
```
Response:
```json
{
  "tenant_id": "...",
  "name": "my-org",
  "api_key": "aiobs_xxxxxxxxxxxxxxxx"
}
```
Copy the `api_key`, paste it in the **Sign In** tab, click **Connect**.

**Why is there a login?** This is a multi-tenant platform. Each organization gets an isolated API key — logs from one tenant are never visible to another. The key is prefixed `aiobs_` and stored in your browser's `localStorage`. Logout clears it.

---

## Watching the UI ↔ Backend Connection

### The dashboard explained

Once logged in, the dashboard has four sections:

```
┌─────────────────────────────────────────────────────────┐
│  METRICS BAR  — polled every 60s via REST               │
│  Total Logs Today │ Active Anomalies │ Alerts │ Health  │
├───────────────────────────────┬─────────────────────────┤
│  LIVE LOG STREAM  (60%)       │  ACTIVE ANOMALIES (40%) │
│  WebSocket /ws/logs/all       │  polled every 10s       │
│  • real-time as logs arrive   │  • Claude's root cause  │
│  • severity color-coded rows  │  • Acknowledge / Resolve│
│  • filter by severity/text    │  buttons                │
│  • pause / resume / clear     │                         │
├───────────────────────────────┴─────────────────────────┤
│  SERVICE HEALTH GRID  — polled every 30s                │
│  One card per service, sparkline error-rate chart       │
│  Click any card → Service Detail page                   │
└─────────────────────────────────────────────────────────┘
```

### Watching the WebSocket connection live

The **Live Log Stream** panel shows a colored dot in the top-left corner:
- 🟢 **Green pulsing** — WebSocket connected to backend, receiving logs in real time
- 🟡 **Yellow** — reconnecting (exponential backoff: 1s → 2s → 4s → 8s → 30s max)
- 🔴 **Red** — disconnected, will keep retrying

The connection goes to `ws://localhost:8000/ws/logs/all?token=YOUR_KEY`.

### Watching the connection in browser DevTools

Open **DevTools → Network → WS** tab:
1. Refresh the page
2. You'll see a WebSocket connection to `/ws/logs/all`
3. Click it → **Messages** tab
4. Every log your app ingests appears here as a JSON message in real time

To watch the REST polling:
- **Network → Fetch/XHR** — you'll see requests to `/api/v1/analytics/overview` (every 60s), `/api/v1/anomalies` (every 10s), `/api/v1/analytics/services` (every 30s)

### Watching backend logs in real time

```bash
# FastAPI request log (all HTTP + WebSocket activity)
docker compose logs fastapi -f

# Log consumer (Kafka → DB writes)
docker compose logs log-consumer -f

# Anomaly consumer (Claude API calls)
docker compose logs anomaly-consumer -f

# Alert engine (dedup + delivery)
docker compose logs alert-engine -f

# All at once, colour-coded by service
docker compose logs -f fastapi log-consumer anomaly-consumer alert-engine
```

### Health check

```bash
curl -s http://localhost:8000/health | python3 -m json.tool
```
```json
{
    "status": "healthy",
    "components": {
        "postgres": "up",
        "redis": "up",
        "kafka": "up"
    }
}
```

The navbar's three dots (top-right) are a live indicator — click them to see Postgres / Redis / Kafka status.

---

## Sending Test Data

After registering and saving your API key, send some test logs to see the full pipeline in action:

```bash
export API_KEY="aiobs_your_key_here"

# Single log
curl -s -X POST http://localhost:8000/api/v1/logs/ingest \
  -H "X-API-Key: $API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "service_name": "api-gateway",
    "severity": "ERROR",
    "message": "Connection timeout to payment service after 30s",
    "environment": "production"
  }'

# Burst of 20 logs across services (triggers anomaly detection faster)
for i in $(seq 1 20); do
  curl -s -X POST http://localhost:8000/api/v1/logs/ingest \
    -H "X-API-Key: $API_KEY" \
    -H "Content-Type: application/json" \
    -d "{
      \"service_name\": \"payment-service\",
      \"severity\": \"ERROR\",
      \"message\": \"Database connection pool exhausted (attempt $i)\",
      \"environment\": \"production\"
    }" > /dev/null
done
echo "Sent 20 error logs"
```

Watch what happens:
1. Logs appear in the **Live Log Stream** within ~1 second (WebSocket)
2. The **Service Health Grid** updates within 30s to show `payment-service`
3. After ~30s the statistical detector may flag an error rate spike
4. The **Active Anomalies** panel shows the anomaly with Claude's root cause
5. The **Metrics Bar** increments Active Anomalies

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         Clients / SDKs                          │
└─────────────────────┬───────────────────────────────────────────┘
                      │ POST /api/v1/logs/ingest (202 fire-and-forget)
                      ▼
┌─────────────────────────────────────────────────────────────────┐
│                    FastAPI (Python 3.12 async)                   │
│  • bcrypt tenant auth + 30s Redis cache                         │
│  • Token bucket rate limiting (10 alerts/tenant/hr)             │
│  • WebSocket streaming via Redis pub/sub                        │
│  • Statistical anomaly detection loop (every 30s)              │
│  • Baseline recalculation (every 5min, 7-day rolling window)   │
└──────────────┬──────────────────────────┬───────────────────────┘
               │ Produce                  │ Query
               ▼                          ▼
┌──────────────────────┐    ┌─────────────────────────────────────┐
│  Kafka Cluster        │    │  Dual-Path Storage                  │
│  3 brokers           │    │  Redis (hot, 1hr TTL, allkeys-lru)  │
│  replication-factor 3│    │  PostgreSQL (cold, GIN full-text)   │
│  5 topics, 6 parts   │    └─────────────────────────────────────┘
│  lz4 compression     │
└──────────┬───────────┘
           │ Consume
           ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Anomaly Detection (background)               │
│  Stage 1: Statistical baselines (every 30s)                     │
│    • Volume spike (>2.5× baseline)                              │
│    • Volume drop (<0.2× baseline)                               │
│    • Error rate spike (>3× baseline)                            │
│    • New pattern detection                                       │
│  Stage 2: Claude Haiku root cause analysis (on confirmed)       │
└──────────────────────────────────────────────────────────────────┘
           │
           ▼
┌─────────────────────────────────────────────────────────────────┐
│                      Alert Engine                               │
│  Redis SADD dedup (1/service/hour) + HMAC-signed webhooks       │
│  3-retry exponential backoff delivery                           │
└─────────────────────────────────────────────────────────────────┘
           │
           ▼
┌─────────────────────────────────────────────────────────────────┐
│                    React Dashboard                              │
│  TanStack Virtual log stream + Recharts anomaly charts          │
│  WebSocket live updates + React Query polling                   │
└─────────────────────────────────────────────────────────────────┘
```

### Why each design decision was made

**Kafka over direct database writes:** The ingest endpoint must return 202 in <5ms regardless of load. At 100k events/min, writing synchronously to Postgres would cause timeouts under burst traffic. Kafka absorbs the burst and lets the consumer write at a sustainable pace. The 3-broker cluster (replication-factor 3, MIN_INSYNC_REPLICAS 2) means the platform survives losing any one broker with zero data loss.

**Two-stage anomaly detection:** Running Claude on every log entry would cost ~$50/hour at 100k events/min. The statistical stage (volume spike 2.5×, drop 0.2×, error rate 3×) runs in <1s per service every 30s using only Redis reads. Claude is only called when statistics confirm something real — roughly 1 in 10,000 events. This bounds both cost and latency.

**Redis hot path + PostgreSQL cold path:** Recent logs (last hour) are served from Redis at <10ms P99. Historical queries use PostgreSQL's GIN full-text index. This avoids scanning millions of rows for live dashboard reads while keeping full history queryable.

**Redis SADD for alert deduplication:** Under concurrent load, 50 consumer instances might detect the same anomaly simultaneously. A database UNIQUE constraint generates 49 rollback exceptions. Redis `SADD` is atomic and O(1) — it returns 0 if the key already exists, allowing all 50 to check in microseconds without any locking.

---

## Tech Decisions

| Decision | Alternative Considered | Why This |
|---|---|---|
| Kafka 3-broker cluster | Redis Streams | Replication-factor 3 survives 1 broker failure; consumer group rebalancing at partition scale |
| Statistical + Claude two-stage | Pure LLM detection | Statistical at <$0.001/day; Claude only on confirmed anomalies (cost + latency bounded) |
| Redis hot path + PostgreSQL cold | PostgreSQL only | Redis <10ms P99 for recent data; PostgreSQL GIN index for full-text search on history |
| FastAPI async throughout | Flask or Django | Zero blocking calls in request path; 3× throughput on I/O-bound workloads |
| Token bucket rate limiting | Fixed window counter | Smoother — no burst at window boundary; Redis INCR + TTL reset on first call only |
| bcrypt + Redis 30s cache | JWT tokens | bcrypt is intentionally slow (security); cache reduces to ~1 hash check per 30s per tenant |

---

## API Reference

All endpoints require `X-API-Key: YOUR_KEY` header except `/health` and `/api/v1/auth/register`.

```bash
# Register a new tenant (public)
curl -X POST http://localhost:8000/api/v1/auth/register \
  -H "Content-Type: application/json" \
  -d '{"name": "my-org"}'

# Ingest a single log
curl -X POST http://localhost:8000/api/v1/logs/ingest \
  -H "X-API-Key: $KEY" -H "Content-Type: application/json" \
  -d '{"service_name":"api-gateway","severity":"ERROR","message":"timeout","environment":"production"}'

# Ingest a batch (up to 1000 logs)
curl -X POST http://localhost:8000/api/v1/logs/ingest/batch \
  -H "X-API-Key: $KEY" -H "Content-Type: application/json" \
  -d '{"logs":[{"service_name":"svc","severity":"INFO","message":"ok","environment":"prod"}]}'

# Query logs (supports service, severity, search_text, from_time, to_time, limit, offset)
curl "http://localhost:8000/api/v1/logs?service=api-gateway&severity=ERROR&limit=50" \
  -H "X-API-Key: $KEY"

# List anomalies (supports status=active|acknowledged|resolved, service, severity_min)
curl "http://localhost:8000/api/v1/anomalies?status=active" \
  -H "X-API-Key: $KEY"

# Acknowledge / resolve an anomaly
curl -X PATCH http://localhost:8000/api/v1/anomalies/{id}/acknowledge -H "X-API-Key: $KEY"
curl -X PATCH http://localhost:8000/api/v1/anomalies/{id}/resolve    -H "X-API-Key: $KEY"

# Analytics overview (logs today, error rate, active anomalies, health score)
curl http://localhost:8000/api/v1/analytics/overview -H "X-API-Key: $KEY"

# Per-service health (volume, error rate, anomaly count)
curl http://localhost:8000/api/v1/analytics/services -H "X-API-Key: $KEY"

# 24-hour timeline for one service
curl http://localhost:8000/api/v1/analytics/services/api-gateway/timeline -H "X-API-Key: $KEY"

# WebSocket — live log stream for all services
wscat -c "ws://localhost:8000/ws/logs/all?token=$KEY"

# WebSocket — live log stream for one service
wscat -c "ws://localhost:8000/ws/logs/api-gateway?token=$KEY"

# Interactive API docs
open http://localhost:8000/docs
```

---

## Running Tests

```bash
cd backend
uv sync --extra dev
uv run pytest -v
# 40 passed, 0 failed, 11 warnings
```

---

## Stack

| Layer | Technology |
|---|---|
| API | FastAPI 0.115 + Python 3.12 asyncio |
| Message broker | Apache Kafka 3-broker (Confluent 7.5) |
| Hot storage | Redis 7 + hiredis |
| Cold storage | PostgreSQL 15 + GIN full-text index |
| AI detection | Anthropic Claude Haiku |
| Frontend | React 19 + Vite + TypeScript (strict) |
| UI components | Tailwind CSS v4 + Recharts + TanStack Query/Table/Virtual |
| Package management | uv (backend), bun (frontend) |
| Infrastructure | Docker Compose (10 services) |
