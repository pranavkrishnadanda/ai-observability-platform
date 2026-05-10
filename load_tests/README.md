# Load Tests — AI Observability Platform

Locust-based load tests covering three scenarios: baseline throughput, ingestion stress, and mixed realistic workload.

---

## Prerequisites

### 1. Running stack

All services must be up before running tests:

```bash
cd /path/to/ai-observability-platform
docker compose up -d
```

Verify the stack is healthy:

```bash
curl http://localhost:8000/health
# Expected: {"status":"healthy","components":{...}}
```

### 2. Python 3.12 + Locust

```bash
pip install locust
# Verify:
python3.12 -m locust --version
```

### 3. API key

The load tests authenticate with `X-API-Key`. Get a key by registering a test tenant:

```bash
curl -s -X POST http://localhost:8000/api/v1/auth/register \
  -H "Content-Type: application/json" \
  -d '{"name":"load-test-tenant","plan_tier":"enterprise"}' \
  | python3 -m json.tool
# Copy the "api_key" field from the response
```

Then update the `api_key` constant in `locustfile.py` (appears in both `LogIngestionUser` and `AnomalySimulator`):

```python
api_key = "aiobs_YOUR_REAL_KEY_HERE"
```

Alternatively, export it as an environment variable and read it in `on_start`:

```python
import os
def on_start(self):
    self.headers = {"X-API-Key": os.getenv("AIOBS_API_KEY", self.api_key)}
```

---

## Running scenarios

All scenarios write CSV + HTML reports to `load_tests/results/<timestamp>/`.

### Baseline throughput (50 users, 60 s)

```bash
./load_tests/run_tests.sh baseline http://localhost:8000
# or with locust directly:
python3.12 -m locust -f load_tests/locustfile.py \
  --headless -u 50 -r 10 --run-time 60s \
  --host http://localhost:8000
```

### Ingestion stress (200 users, 120 s)

```bash
./load_tests/run_tests.sh stress http://localhost:8000
```

### Mixed realistic load (100 users, 180 s)

```bash
./load_tests/run_tests.sh mixed http://localhost:8000
```

### All scenarios in sequence

```bash
./load_tests/run_tests.sh all http://localhost:8000
```

### Locust web UI (interactive)

```bash
python3.12 -m locust -f load_tests/locustfile.py --host http://localhost:8000
# Open http://localhost:8089 and configure users interactively
```

---

## Scenario details and pass/fail criteria

### Scenario 1 — Baseline throughput

| Parameter | Value |
|-----------|-------|
| Virtual users | 50 |
| Spawn rate | 10/s |
| Duration | 60 s |
| Target host | http://localhost:8000 |

**What it tests:** Steady-state throughput with a realistic task mix — single ingest (weight 10), batch ingest (weight 3), log queries (weight 2), analytics (weight 2), anomaly list (weight 1), health check (weight 1).

**Pass criteria:**
- Total request rate >= 5,000 req/min
- `POST /logs/ingest` p99 < 10 ms (NFR-2)
- `GET /logs` p99 < 500 ms (NFR-3)
- Error rate (excluding 429) < 1%
- Zero 5xx responses on health check

### Scenario 2 — Ingestion stress

| Parameter | Value |
|-----------|-------|
| Virtual users | 200 |
| Spawn rate | 20/s |
| Duration | 120 s |
| Event payload | 100 events/batch |

**What it tests:** Peak batch ingestion capacity; whether Kafka back-pressure and rate limiting hold without silent data loss.

**Pass criteria:**
- Effective event throughput >= 100,000 events/min (NFR-1)
- Batch endpoint p99 < 10 ms (NFR-2)
- 429 responses appear but do not cause failures in Locust (treated as success)
- Zero 500-level errors from the ingestion endpoint itself

**Throughput math:**
- 200 users × batch-task weight (3 of 19 total weight) ≈ 32 concurrent batch calls at any moment
- Each batch = 100 events, at ~25–50 ms per call → ~40,000–80,000 events/min per sustained batch stream
- Add single ingest (weight 10): 200 × 10/19 ≈ 105 single events/s → ~6,300/min
- Combined: ~50,000–90,000 events/min. To reliably hit 100,000/min, run `--users 250` or increase batch size to 200.

### Scenario 3 — Mixed realistic load

| Parameter | Value |
|-----------|-------|
| Virtual users | 100 |
| Spawn rate | 10/s |
| Duration | 180 s |

**What it tests:** Sustained mixed read+write workload; validates Redis hot-path query latency and analytics cache hit rate under concurrent load.

**Pass criteria:**
- `GET /logs` p95 < 100 ms (Redis hot path), p99 < 500 ms (NFR-3)
- `GET /analytics/overview` p99 < 500 ms (backed by 60 s cache)
- No memory errors or OOM from Redis or PostgreSQL during the run
- Error rate (excluding 429) < 0.5%

---

## Interpreting Locust CSV results

After a run, `results/<timestamp>/` contains:

```
baseline_stats.csv          — per-endpoint aggregate stats
baseline_stats_history.csv  — time-series of req/s and latency (10 s buckets)
baseline_failures.csv       — failed request details
baseline_report.html        — self-contained visual report
baseline.log                — full locust stdout
```

### Key columns in `*_stats.csv`

| Column | Meaning |
|--------|---------|
| `Name` | Endpoint label (matches `name=` in locustfile) |
| `Request Count` | Total requests sent |
| `Failure Count` | Requests that called `resp.failure(...)` |
| `Median Response Time` | p50 latency in ms |
| `95%ile` | p95 latency in ms |
| `99%ile` | p99 latency in ms |
| `Average Response Time` | Mean latency in ms |
| `Requests/s` | Sustained throughput for this endpoint |
| `Failures/s` | Failure rate |

### What good numbers look like

| Endpoint | p50 target | p99 target | Notes |
|----------|------------|------------|-------|
| POST /logs/ingest | < 5 ms | < 10 ms | Kafka async publish |
| POST /logs/ingest/batch | < 20 ms | < 50 ms | 100-event batch |
| GET /logs | < 20 ms | < 100 ms | Redis LRANGE hot path |
| GET /analytics/overview | < 10 ms | < 50 ms | 60 s cache hit |
| GET /anomalies | < 30 ms | < 200 ms | PostgreSQL query |
| GET /health | < 5 ms | < 20 ms | Dependency ping |

### Diagnosing failures

- **High 429 rate:** The test API key's `rate_limit_per_minute` (default 1000) is being hit. Register an enterprise-tier tenant or reduce `--users`.
- **5xx on ingest:** Kafka broker unreachable. Check `docker compose ps kafka`.
- **High GET /logs latency (>500 ms):** Redis cache miss; confirm Redis is running and the key TTL has not expired.
- **`stats_history.csv` shows latency spike mid-test:** PostgreSQL connection pool saturation. Check `pg_stat_activity`.

---

## Resume bullet template

After running the full test suite against the real stack, record actual numbers and use this template:

> "Load tested to **X events/min** sustained ingestion (Kafka + PostgreSQL), p95 latency **Y ms** on query endpoints backed by Redis hot path, **Z% cache hit rate** after warm-up on analytics overview, with < 0.5% error rate at 100 concurrent virtual users over 3 minutes."

Example with realistic local-Docker numbers:

> "Load tested to **72,000 events/min** sustained ingestion, p95 latency **28 ms** on `GET /logs` (Redis hot path), **94% cache hit rate** on analytics overview endpoint after 30 s warm-up, error rate < 0.3% at 100 concurrent users over 3 minutes."

---

## Known bottlenecks and path to 10x scale

### Current bottlenecks (Docker Compose, single-node)

| Component | Current limit | Symptom |
|-----------|--------------|---------|
| Kafka (1 broker, 1 partition) | ~500,000 msg/min | None at current load; fails at 10x |
| PostgreSQL (single instance) | ~10,000 inserts/sec | Query latency degrades under write pressure |
| Redis (512 MB, single node) | ~12 GB required for 1 hr at 100k/min | LRU eviction drops recent logs before TTL |
| Claude Haiku semaphore (10) | ~50 analyses/min | Anomaly backlog grows during incident bursts |
| bcrypt auth (cache miss) | O(N tenants × 300 ms) | First request per tenant is slow |

### Changes needed for 10x (1,000,000 events/min)

1. **Kafka:** Add 3 brokers, increase `logs.raw` to 36 partitions, enable `lz4` compression, set `linger.ms=10`, `batch.size=131072`. Scale consumer group to 36 instances.

2. **PostgreSQL:** Add 2 read replicas for query endpoints. Migrate `logs` table to TimescaleDB for automatic time-series compression (expect 10-20x storage reduction). Add hypertable chunk interval of 1 hour. Add `api_key_prefix` column with B-tree index for O(1) tenant lookup instead of full-table bcrypt scan.

3. **Redis:** Switch to Redis Cluster (3 primaries, 3 replicas). Increase per-node `maxmemory` to 4 GB. Replace `LRANGE 0 9999` with cursor-based `LPOS` scans for query endpoints. Store log payloads in MessagePack instead of JSON to cut memory ~30%.

4. **Claude Haiku:** Request Anthropic tier upgrade (500 RPM). Replace semaphore with a token-bucket queue backed by Redis. Add a dead-letter queue for analyses that fail after 3 retries.

5. **FastAPI workers:** Switch from single Uvicorn process to `gunicorn -k uvicorn.workers.UvicornWorker -w $(nproc)`. Add an NGINX reverse proxy for TLS termination and connection pooling.

6. **Auth hot path:** Cache bcrypt verify result in Redis with key `auth:verify:{api_key_hash_prefix}` and 30 s TTL (already done). At 10x, add an `api_key_prefix` DB index so the bcrypt lookup is against a single row rather than a full tenant scan.
