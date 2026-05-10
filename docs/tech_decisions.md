# Technology Decisions

This document records every major technology choice for the AI Observability
Platform along with the alternative considered, the tradeoff explanation, and
how the choice performs at the target scale (100k logs/min, 99.9% uptime,
sub-10 ms ingest p99, sub-500 ms query p99).

Each decision is binding for the implementation phase.

---

## 1. FastAPI vs Django / Flask

**Decision:** FastAPI (with `uvicorn` ASGI server).

**Alternatives considered:**
- Flask + gunicorn (sync WSGI).
- Django + Django REST Framework + Channels (for WebSocket).

**Tradeoff:**
- FastAPI is async-native and built on Starlette + Pydantic. The ingest path is
  I/O-dominated (Kafka produce + maybe Redis), so it benefits from `asyncio`
  concurrency without extra worker processes per request.
- Pydantic validators give us free, type-safe validation of `severity`/`environment`
  enums and length constraints — the same models double as request schemas and
  OpenAPI docs.
- Flask is sync; sustaining 100k logs/min with sync workers would require a much
  larger worker pool and more memory. WebSocket support is bolt-on (Flask-SocketIO).
- Django is heavyweight: ORM, admin, sessions, templating — none of which we need.
  Channels works but adds a second runtime (Daphne) and another infra concern.

**At scale:**
- FastAPI on uvicorn handles ~3–5k req/s per worker on commodity hardware for
  small JSON payloads, with the ingest path bound only by Kafka producer ack
  latency (~2–5 ms with `acks=all` and `linger.ms=10`).
- Built-in OpenAPI / `/docs` reduces integration friction for SDK consumers.

---

## 2. Kafka vs Redis Streams / RabbitMQ / SQS

**Decision:** Apache Kafka (Confluent images for dev, multi-broker for prod).

**Alternatives considered:**
- Redis Streams.
- RabbitMQ.
- AWS SQS.

**Tradeoff:**
- Kafka decouples ingest acknowledgement from heavy downstream work
  (Postgres writes, anomaly detection, Claude calls). Producers ack on durable
  partition write; consumers process at their own pace.
- Kafka's partition model gives us per-tenant ordering by keying on `tenant_id`,
  which neither Redis Streams nor SQS Standard provide cleanly.
- Kafka retention (`retention.ms`) lets us replay or DLQ without rebuilding
  state. Redis Streams keeps everything in memory; backlog growth = memory
  pressure on the same Redis we use for hot reads.
- RabbitMQ is queue-centric (push/ack), without log-style replay. It's also less
  suited to multi-consumer fan-out (each consumer group with independent offsets).
- SQS is simple but has 256 KB message limit, no native ordering across messages
  except FIFO queues (which limit throughput to 300 msg/sec without batching),
  and ties us to AWS.

**At scale:**
- 12 partitions on `logs.raw` at 100k events/min = ~140 events/sec/partition,
  leaving 100× headroom on a single broker.
- LZ4 compression (~3×) reduces inter-broker bandwidth to ~250 KB/sec.
- Idempotent producers (`enable.idempotence=true`) prevent duplicates from
  retries — important since we have at-least-once consumers.

---

## 3. Redis (hot path) vs ClickHouse / TimescaleDB

**Decision:** Redis 7 for the hot tier (last 1 hour of logs + counters +
baselines + dedup + pub/sub).

**Alternatives considered:**
- ClickHouse for hot-path analytics.
- TimescaleDB hypertables for time-series.

**Tradeoff:**
- The hot path is millisecond, point-style access patterns: "get latest 100 logs
  for service X", "increment 5-minute counter", "is this dedup key present".
  Redis serves these in sub-millisecond at memory cost.
- ClickHouse is excellent for OLAP scans but its insert path batches and is not
  ideal for streaming counter updates at our cadence; query latency on tiny
  result sets is dominated by overhead.
- TimescaleDB is just Postgres + extension. We already use Postgres for cold
  storage; adding hypertables doesn't replace our need for a sub-millisecond
  pub/sub layer for WebSocket fan-out.
- Redis also provides Pub/Sub for WebSocket fan-out — using one tool for hot
  reads, counters, dedup, and pub/sub keeps the operational footprint small.

**At scale:**
- 512 MB `maxmemory` with `allkeys-lru` covers ~1M small JSON log entries plus
  thousands of counter/baseline keys. The Redis list trim (`LTRIM 0 9999`)
  enforces per-service caps so a single noisy tenant cannot evict everyone else.
- `pipeline()` collapses LPUSH+LTRIM+EXPIRE+INCR into one round-trip in the
  log-processor consumer, keeping per-message Redis cost ~50 µs.

---

## 4. PostgreSQL vs MongoDB / DynamoDB

**Decision:** PostgreSQL 15 with `pg_trgm` extension.

**Alternatives considered:**
- MongoDB.
- AWS DynamoDB.

**Tradeoff:**
- We need: JSON metadata storage, free-text search on `message`, complex range
  + filter queries (service + severity + time + trace_id), and a UNIQUE
  constraint on `alerts.dedup_key`. Postgres handles all of these in one engine.
- MongoDB has good JSON ergonomics but free-text search needs Atlas Search or
  a separate Elasticsearch; range queries with multi-column composite filters
  benefit less from its index design than from Postgres B-tree composite indexes.
- DynamoDB is great for predictable key-value access but requires careful
  partition-key planning; range queries that span multiple services or trace IDs
  become expensive scans or require GSIs that double the write cost.
- Postgres `pg_trgm` GIN index gives us trigram search on `message` with
  index-only filtering; combined with a composite `(tenant_id, created_at DESC)`
  B-tree, our query latency target (NFR-3) is achievable without a separate
  search engine.

**At scale:**
- A composite B-tree on `(tenant_id, created_at DESC)` and a GIN on `message`
  serve 24-hour windowed queries at p99 < 500 ms for ≤1000 rows.
- `logs` is unpartitioned at MVP; production scale-out path is range-partitioning
  by `created_at` daily (declarative partitioning) — schema unchanged for the app.
- `asyncpg` driver yields ~30k inserts/sec via `executemany`, well above our
  1.6k/sec target.

---

## 5. Claude Haiku vs GPT-4o-mini / local model

**Decision:** Anthropic Claude Haiku (`claude-haiku`) for anomaly RCA and on-demand analysis.

**Alternatives considered:**
- OpenAI GPT-4o-mini.
- Local model via vLLM (e.g., Llama 3 8B Instruct).

**Tradeoff:**
- Claude Haiku has very low latency (typically 1–3 s for short prompts) and the
  longest reliable context window in its tier — we feed up to ~50 sample log
  lines plus an anomaly summary, and need consistent JSON-ish structured output.
- GPT-4o-mini is a peer in price/latency. We chose Haiku because Anthropic's
  prompt caching is well-suited to our repeated system prompt (the RCA template
  is fixed; only sample logs change), and Anthropic's structured-output story
  via XML tags is robust enough for our parser without enabling tool use.
- A local model via vLLM removes per-call cost but introduces GPU operations,
  cold-start latency, and a much wider quality gap on root-cause-style reasoning
  on small contexts. For an MVP/demo, infrastructure cost > API cost.

**At scale:**
- Concurrency is bounded by an `asyncio.Semaphore(10)`, well within Anthropic's
  default rate limits for our tier.
- Haiku's typical p99 latency keeps us comfortably under the 30 s NFR-10 target.
- Per-tenant per-day usage is tracked in Redis (`analysis_credits:{epoch_day}`)
  so we can surface plan caps and bill internally.
- On API failure, we degrade gracefully (templated `claude_analysis`) — the
  alert pipeline is never blocked by Claude availability.

---

## 6. Async Python (asyncio) vs Go / Node.js

**Decision:** Python 3.12 with `asyncio` for backend + consumers.

**Alternatives considered:**
- Go (`net/http`, `confluent-kafka-go`).
- Node.js (`fastify`, `kafkajs`).

**Tradeoff:**
- The team's strongest language is Python; we get the largest, most mature
  AI/ML ecosystem (Anthropic SDK, numpy for baseline stats, pandas-style
  analytics) and fewest impedance mismatches when iterating on the
  anomaly-detection logic.
- Go gives us better raw throughput per core, but our bottlenecks are network
  I/O (Kafka, Redis, Postgres, Anthropic) — async Python saturates those just
  as well per request because we're never CPU-bound on the request path.
- Node.js is competitive with async Python but its Kafka clients (`kafkajs`)
  have known reliability gaps under sustained load compared to `librdkafka`/
  `aiokafka`.
- Splitting the stack (Python API + Go consumers, for example) doubles build,
  release, and observability tooling for negligible gain at our target scale.

**At scale:**
- A single async Python uvicorn worker handles 3–5k req/s on the ingest path;
  4 workers easily exceed 100k logs/min (1.6k/sec with batching).
- `aiokafka` (built on `librdkafka`) sustains hundreds of thousands of msgs/sec
  per consumer; we are nowhere near that ceiling.
- Where we ever do hit CPU limits (e.g., bcrypt verify in auth), we cache the
  verify per-request and bcrypt cost is bounded.

---

## 7. WebSocket via FastAPI vs Server-Sent Events

**Decision:** Native WebSocket via FastAPI's `WebSocket` route handler.

**Alternatives considered:**
- Server-Sent Events (SSE) over HTTP/1.1 keep-alive.

**Tradeoff:**
- WebSocket is bidirectional, which we need to implement heartbeat ping/pong
  (NFR / AC-22.3) and to allow future client-driven filters (e.g., subscribe to
  multiple services on one connection without opening a new HTTP stream each
  time).
- SSE is simpler to debug (curl-friendly) but is one-way; we'd still need a
  separate channel for control messages and reconnection state.
- FastAPI's `WebSocket` is first-class Starlette; no extra dependency. Our
  Redis-Pub/Sub bridge model doesn't depend on which transport we expose.
- WebSocket is what every observability dashboard customer (DataDog, Honeycomb,
  Logz.io) ships, lowering integration friction for users who want to embed
  the stream.

**At scale:**
- A single FastAPI process holds tens of thousands of idle WebSocket
  connections (limited by file descriptors, not CPU). For our demo target
  (hundreds of concurrent dashboard users), this is trivially handled.
- Per-connection backpressure (`asyncio.Queue(maxsize=1000)`) prevents a slow
  client from stalling the server; overflow disconnects with code 1013.

---

## 8. SQLAlchemy async vs Tortoise ORM vs raw asyncpg

**Decision:** SQLAlchemy 2.x async (`sqlalchemy[asyncio]`) over `asyncpg` driver.

**Alternatives considered:**
- Tortoise ORM.
- Raw `asyncpg` (no ORM).

**Tradeoff:**
- SQLAlchemy 2.x's async API is mature and integrates with Alembic for
  migrations. We get migrations, sessions, and typed ORM models out of the box.
- Tortoise ORM is lighter and Django-like, but its migration tooling (`aerich`)
  is younger and we'd lose access to SQLAlchemy Core for the small number of
  hand-tuned queries (e.g., `count(*) over()` window function, JSONB ops).
- Raw `asyncpg` has the lowest overhead but means we'd hand-write every CRUD
  operation and every migration. The constant-factor speedup over SQLAlchemy
  Core is real but irrelevant when our actual ingest path doesn't touch the
  ORM at all (it goes straight to Kafka).
- We use SQLAlchemy ORM for query endpoints and SQLAlchemy Core (or raw SQL via
  `text()`) inside `log_consumer` for the bulk-INSERT hot loop, so we get the
  best of both.

**At scale:**
- Bulk INSERT via SQLAlchemy Core `insert()` with `executemany` (asyncpg driver
  underneath) sustains tens of thousands of rows/sec.
- Composite indexes are defined in the ORM model and emitted by Alembic, so
  schema and code stay in sync.

---

## 9. Docker Compose vs Kubernetes (for dev / demo)

**Decision:** Docker Compose for dev and demo; Kubernetes manifests deferred
to a follow-on phase if needed.

**Alternatives considered:**
- Kubernetes (kind / minikube / k3d) for local; Helm chart for deployment.

**Tradeoff:**
- Compose makes the entire stack reproducible with `docker compose up` —
  Postgres, Redis, Zookeeper, Kafka, the ingestion API, all consumer roles,
  and the frontend, with minimal moving parts.
- Kubernetes is the right answer for prod multi-tenant SaaS, but for
  development and an interview/demo it adds friction (pod scheduling,
  PersistentVolumeClaims, ingress controllers) without observable benefit.
- The same Docker images we build for Compose are the artifacts you'd push to
  a Kubernetes deployment — switching later is a manifest, not a rewrite.

**At scale:**
- Compose `replicas:` lets us scale the consumer services horizontally on a
  single host for demo throughput tests.
- The production migration path is well-understood: one Deployment per
  consumer role, HPA on Kafka consumer lag (via `prometheus-adapter`), a
  StatefulSet for Postgres or a managed RDS, and a managed Kafka (MSK / Confluent Cloud).

---

## 10. Token bucket (Redis) for rate limiting vs sliding window

**Decision:** Redis fixed-window counter (`INCR` + conditional `EXPIRE`) with
the same Lua-atomic pattern used as a token bucket equivalent (one bucket per
discrete window).

**Alternatives considered:**
- Sliding-window log (store every request timestamp in a Redis sorted set).
- Sliding-window counter with two adjacent buckets weighted by elapsed fraction.

**Tradeoff:**
- Fixed window is O(1) per request and uses one Redis key per (tenant, minute) —
  we never store per-request entries. At 100k logs/min that's 100k INCR ops on
  a hot key; Redis handles this comfortably.
- Sliding-window-log gives perfect accuracy but is O(n) memory per window —
  storing 100k timestamps per tenant per minute is wasteful and creates GC churn.
- Sliding-window counter is more accurate at window boundaries but doubles the
  read cost per request and complicates the code.
- For NFR-8 (alerts: 10/hour/tenant) and the API rate-limit SLO, fixed-window
  bucket-edge inaccuracy (briefly allowing 2× the limit at the boundary) is
  acceptable because we already have alert dedup and Postgres UNIQUE constraints
  as backstops, and because customers don't hit the rate limit in normal operation.

**At scale:**
- One INCR + one conditional EXPIRE per request: ~50 µs end-to-end on Redis.
- Lua script combines INCR + EXPIRE atomically so we don't have a TTL race on
  the first hit of a new window.
- Memory cost: one 8-byte counter per tenant per minute, dropped automatically
  by the 120 s TTL.
