# Performance Analysis

## Target Metrics (from requirements)

| Metric | Target | Source | Measurement |
|--------|--------|--------|-------------|
| Ingestion throughput | 100,000 events/min | NFR-1 | Batch endpoint with 100-event batches |
| Ingestion latency | < 10 ms p99 | NFR-2 | Single ingest endpoint (Kafka fire-and-forget) |
| Query latency | < 500 ms p99 | NFR-3 | GET /logs with Redis hot path |
| Analytics latency | < 500 ms p99 | FR-19 | GET /analytics/overview (60 s cache) |
| Availability | 99.9% monthly | NFR-5 | Ingestion + query endpoints |
| AI analysis latency | < 30 s | NFR-10 | Time from anomaly detection to Claude call |
| Alert rate limit | 10 alerts/tenant/hr | NFR-8 | Redis counter with 3600 s TTL |

---

## Theoretical Throughput Analysis

### Ingestion path

- **Single ingest (`POST /api/v1/logs/ingest`):** Validates payload, checks rate-limit counter in Redis (one `INCR`), then calls `publish_async` which enqueues the message to the Kafka producer's internal buffer and returns immediately. Server-side time: typically 1–3 ms. p99 < 10 ms is achievable even at 10,000 concurrent requests/s.

- **Batch ingest (`POST /api/v1/logs/ingest/batch`, 100 events):** Iterates 100 events, calls `publish_async` 100 times (non-blocking enqueue), returns 202. Time dominated by JSON deserialization (~5 ms for 100 events) + Kafka enqueue. Expected p99: 15–40 ms.

- **Throughput math at 200 users:**
  - Task weight distribution: single(10) + batch(3) + reads(7) = 20 total weight
  - At `between(0.01, 0.05)` s wait: each user issues ~20–100 req/s
  - Batch tasks: 200 users × (3/20) × 50 req/s ≈ 1,500 batch calls/min × 100 events = **150,000 events/min**
  - Single tasks: 200 × (10/20) × 50 req/s ≈ 30,000 events/min
  - Combined theoretical peak: **~180,000 events/min** — well above NFR-1

- **Kafka throughput with default local config (`linger_ms=10`, `lz4` compression):** Single broker, 1 partition: sustains >500,000 msg/min on an M-series Mac. Not the bottleneck at 100k/min.

### Query path

- **Redis hot path (`GET /logs?service=X`):** Executes `LRANGE tenant:{id}:service:{name}:logs 0 49`. Single Redis round-trip: 1–5 ms. p99 < 50 ms under concurrent load (Redis is single-threaded; throughput caps at ~100,000 ops/s per node).

- **PostgreSQL cold path:** Full-text search via `pg_trgm` GIN index on `message`. For 24-hour window at 100k events/min: up to 144M rows. Without a composite index on `(tenant_id, service_name, created_at)`, this degrades to sequential scan. With the index, p99 stays < 500 ms for queries returning ≤ 1000 rows per NFR-3.

- **Analytics overview (`GET /api/v1/analytics/overview`):** Cached for 60 s in Redis. Cache hit: 1–5 ms. Cache miss: aggregation query over 24-hour window, expected 50–200 ms.

---

## Bottlenecks at Scale

### 1. Kafka single broker — development configuration

- Current: 1 broker, `replication_factor=1`, 1 partition per topic
- Risk: Single point of failure; throughput caps at ~2M msg/min before network saturation
- **To 10x:** Add 3 brokers, set `replication_factor=3`, increase `logs.raw` to 36 partitions (matches 36 consumer instances). Enable `lz4` compression on producer, `linger.ms=10`, `batch.size=131072`. Use Kafka Streams for stateful aggregation instead of in-process counters.

### 2. PostgreSQL batch writes — consumer writes 100 logs per commit

- Current: Log-processor consumer batches 500 events or waits 1 s (FR-7 / AC-7.2), then issues a single `INSERT ... VALUES (...)` statement. At 100k events/min: ~200 batches/min, each inserting 500 rows.
- Estimated throughput: ~10,000 inserts/s on a single PostgreSQL 15 instance (M-series Mac, default config). This handles 600,000 inserts/min — comfortably above target.
- Risk: Read queries compete with write I/O on same instance.
- **To 10x:** Add streaming read replicas. All `GET /logs` and analytics queries route to a read replica. Consider TimescaleDB for automatic columnar compression of old chunks (10–20x storage reduction). Add hypertable with 1-hour chunk intervals.

### 3. Redis memory — 512 MB cap with allkeys-lru

- Current: Each log event is stored as a JSON string in a Redis list (~500 bytes per event after serialization).
- At 100k events/min × 60 min retention × 500 bytes: **3 GB required** for a single tenant across all services.
- With `maxmemory 512mb` and `allkeys-lru`: Redis will begin evicting recent entries before 1-hour TTL expires, violating FR-6 / AC-6.3.
- **To 10x:** Switch to Redis Cluster (3 primaries × 4 GB each = 12 GB total). Serialize log payloads in MessagePack instead of JSON (~30% size reduction). Replace `LRANGE` over large lists with indexed queries; consider a Redis TimeSeries module for bucketed counters.

### 4. Claude Haiku calls — semaphore(10) concurrent

- Current: `asyncio.Semaphore(10)` caps concurrent Haiku calls (AC-9.2). At Anthropic's standard tier: ~50 RPM.
- At 100k events/min: anomaly detector fires every 30 s × N (tenant × service) pairs. With 10 tenants × 5 services = 50 anomaly checks/min. Haiku call rate stays well under 50 RPM.
- Risk: Burst anomaly events (e.g., cascading failures) generate many anomalies simultaneously, exhaust the semaphore, and delay analysis beyond 30 s (NFR-10).
- **To 10x:** Request Anthropic tier upgrade (500 RPM). Replace semaphore with a priority queue backed by Redis Sorted Set (score = severity). Add circuit breaker: if Claude call latency p99 > 5 s, fall back to template immediately.

### 5. Auth performance — bcrypt verify on every cache miss

- Current: Each request calls `get_current_tenant()`, which checks a 30 s Redis cache keyed on the hashed API key. Cache miss triggers bcrypt verify (~100–300 ms at cost=12).
- Under load: If 50 concurrent users all have cache misses simultaneously (e.g., after Redis restart), bcrypt runs 50× in parallel. FastAPI's async loop will handle these on a thread pool, but the latency spike will be visible.
- **To 10x:** Add `api_key_prefix` column (first 8 chars of plaintext key) to `tenants` table with a B-tree index. On cache miss, look up by prefix first (O(1)) to get a single candidate row, then bcrypt verify — eliminating the full-table iteration that would occur if multiple tenants existed.

---

## Local Benchmark Estimates

These are theoretical estimates for a local Docker Compose setup on an Apple M-series Mac. Run `./load_tests/run_tests.sh all` against a warm stack (after at least 2 min of ingestion) to get actual numbers.

| Scenario | Est. req/min | Est. p50 latency | Est. p99 latency | Notes |
|----------|-------------|-----------------|-----------------|-------|
| Single ingest | 30,000–60,000 | 2–5 ms | 8–15 ms | Kafka async publish; p99 target = 10 ms |
| Batch ingest (100 events) | 600,000+ events/min | 15–30 ms | 40–80 ms | Kafka batching; events/min >> req/min |
| GET /logs (Redis) | 15,000–30,000 | 5–15 ms | 20–50 ms | LRANGE; Redis single-threaded |
| GET /logs (PostgreSQL cold) | 5,000–10,000 | 30–80 ms | 150–400 ms | GIN index + composite index required |
| GET /analytics/overview | 20,000–40,000 | 3–8 ms | 10–30 ms | 60 s Redis cache hit |
| GET /anomalies | 10,000–20,000 | 10–30 ms | 50–200 ms | PostgreSQL with tenant_id index |
| GET /health | 50,000–100,000 | 1–3 ms | 5–15 ms | Dependency ping (Redis + PG + Kafka) |

**Note:** Numbers degrade significantly if Docker Desktop's memory limit is < 8 GB or if all services run on the same CPU core group. Ensure Docker Desktop is allocated at least 8 GB RAM and 4 CPUs.

---

## Warm-Up Considerations

The stack requires a warm-up period before numbers are representative:

1. **Redis cache:** Analytics overview cache is empty on first request. Allow 60–90 s of traffic before benchmarking analytics latency.
2. **PostgreSQL plan cache:** The query planner caches execution plans. First queries may be slower as plans are generated.
3. **Kafka producer batch fill:** The Kafka producer coalesces messages for `linger.ms` before sending. Throughput stabilizes after ~10 s.
4. **JVM-based Kafka broker:** On startup, JVM JIT compilation causes higher latency for the first 30–60 s. Docker Compose health checks handle this.

Run `./load_tests/run_tests.sh baseline` first as a warm-up pass, then run the target scenario for accurate measurements.

---

## NFR Compliance Summary

| NFR | Target | Status | Evidence |
|-----|--------|--------|---------|
| NFR-1: Ingestion throughput | 100,000 events/min | Achievable | Math: 200 users × batch(100) ~= 150k events/min theoretical |
| NFR-2: Ingestion latency | < 10 ms p99 | Achievable | Kafka fire-and-forget; no synchronous DB call on ingest path |
| NFR-3: Query latency | < 500 ms p99 | Achievable with Redis | Redis hot path p99 ~50 ms; PostgreSQL cold path requires composite index |
| NFR-5: Availability | 99.9% | Depends on infra | Single-node Docker Compose cannot guarantee; requires k8s + replicas in prod |
| NFR-8: Alert rate limit | 10 alerts/hr | Implemented | Redis counter `alerts:rate:{tenant_id}` with 3600 s TTL |
| NFR-10: AI analysis latency | < 30 s | Risk at burst | Semaphore(10) can queue under burst; add priority queue for high-severity |

---

## Kafka Cluster Upgrade (Phase 14)

**Before:** Single broker, no replication, auto-created topics
**After:** 3-broker cluster (kafka-1:9092, kafka-2:9093, kafka-3:9094)

Configuration:
- `KAFKA_DEFAULT_REPLICATION_FACTOR: 3`
- `KAFKA_MIN_INSYNC_REPLICAS: 2`
- `KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 3`
- `KAFKA_AUTO_CREATE_TOPICS_ENABLE: false`
- Topics: logs.raw (6p/3r), logs.processed (6p/3r), logs.anomalies (3p/3r), logs.alerts (3p/3r), logs.dlq (3p/3r)

Producer settings: `acks=all, retries=5, batch_size=32768, lz4 compression`

**Durability guarantee:** The cluster can lose any 1 of 3 brokers with zero data loss. With MIN_INSYNC_REPLICAS=2, a message is only acknowledged after 2 replicas confirm it — meaning even if the leader fails immediately after ack, one follower already has the data.
