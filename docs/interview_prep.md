# Interview Prep — AI Observability Platform

This document is the senior-engineer walkthrough of the seven design choices
that hold this system together. Each section names the problem, lays out the
chosen mechanism, contrasts it with the alternatives we rejected, and ends
with a scaling story. File and line references point at the actual code.

---

## 1. Why Kafka (not just a Redis queue)

**The problem.** The ingest API has to absorb 100,000 events/min while
preserving sub-10 ms p99 latency, *and* keep emitting messages to four
independent downstream pipelines (persist, statistical anomaly detection,
WebSocket fan-out, claude analysis), each operating at its own pace, each able
to crash and resume without losing or double-processing data.

**Why Kafka.**

- **Durable log retention with replay.** `logs.raw` is configured with
  `retention.ms = 3600000` (1 hour). If the `log-processors` group crashes for
  ten minutes, it resumes from the last committed offset — no loss, no manual
  reconciliation. See `docs/kafka_design.md` §2.1.
- **Consumer-group rebalancing.** Twelve `log-processors` instances share
  twelve `logs.raw` partitions. When one dies, the broker reassigns its
  partitions to surviving consumers automatically. No bespoke leader election.
- **Partition-based parallelism with per-tenant ordering.** Every message is
  keyed by `tenant_id` (`backend/app/api/v1/ingest.py:47`). All events for a
  tenant land on the same partition, which means a single consumer processes
  them in order and a noisy tenant cannot starve everyone — only the partition
  it hashes to.
- **At-least-once delivery via manual commit.** Consumers run with
  `enable.auto.commit=false` and call `consumer.commit()` only after the
  Postgres insert *and* Redis pipeline succeed. See
  `backend/app/consumers/anomaly_consumer.py:226` and the same pattern in
  `alert_engine.py:282`. On crash, the next consumer replays uncommitted
  offsets.
- **DLQ pattern.** Each topic has a paired `*.dlq` (7-day retention). After 3
  retries, poison messages get the original payload plus an envelope (error,
  exception_type, stack_trace, retry_count, source_topic/partition/offset)
  written to the DLQ. The main offset commits and the consumer keeps moving.
  See `docs/kafka_design.md` §6.

**Why not Redis Streams.** Redis Streams looks attractive because we already
run Redis. The dealbreakers:

1. **Memory pressure on the same Redis we use for hot reads.** Streams keep
   everything in RAM. A 30-minute backlog at 100k events/min is ~3 GB of
   stream entries — competing with our 512 MB hot-log + counter budget.
   Eviction would silently destroy unread events.
2. **No mature multi-consumer rebalancing.** Streams' consumer groups exist
   but lack Kafka's broker-side group coordinator and rebalance protocol; if
   one consumer dies you implement reassignment yourself.
3. **No native compression.** Kafka with `compression.type=lz4` cuts JSON log
   bandwidth roughly 3×; Streams stores raw values.

RabbitMQ was rejected for the same replay reason. SQS was rejected because
FIFO queues cap at 300 msg/sec without batching, and Standard queues give no
ordering at all.

**At scale.** At 100k events/min on 12 partitions = ~140 events/sec/partition.
That leaves two orders of magnitude of headroom on a single broker. The
production path is a 3-broker cluster with `replication_factor=3` and
`min.insync.replicas=2`.

---

## 2. Hot/cold path for time-series data

**The problem.** Two query shapes live in tension. The dashboard wants the
last 100 logs for a service, refreshed sub-second. The investigation tool
wants full-text search across 30 days of history. Optimizing one slows the
other.

**The chosen split.**

- **Hot path: Redis 7, last 1 hour, bounded.** The log-processor pushes each
  event with `LPUSH` then `LTRIM 0 9999` and refreshes `EXPIRE 3600`
  (`docs/data_models.md` §2.1). The hot list per service is therefore capped
  at 10,000 entries and 1 hour, no matter how loud the tenant gets.
  `GET /api/v1/logs?service=X` checks the hot path *only* when the parameter
  shape qualifies (recent + per-service, no full-text/severity/trace
  filtering, offset 0) — see the hot/cold decision tree in
  `docs/architecture.md` §3.4.
- **Cold path: Postgres 15 with `pg_trgm` GIN.** The composite B-tree on
  `(tenant_id, created_at DESC)` plus the GIN on `message gin_trgm_ops`
  (`docs/data_models.md` §1.2) serves any combination of severity, service,
  trace_id, and `message %% search_text` filtering with index-only scans. The
  same query computes `count(*) over()` as a window function so we return
  `total` without a second round-trip — staying under 500 ms p99.
- **The handoff.** When the request crosses the 1-hour boundary or includes a
  `search_text`/`trace_id` filter, the API skips Redis and goes straight to
  Postgres. There is no "tier merge" — the hot tier is a strict subset of
  what's in Postgres, and the consumer writes both atomically before
  committing.

**Why not ClickHouse for hot.** ClickHouse is excellent for OLAP scans, but
its insert path is batch-oriented and per-query overhead dominates on the
"give me 100 rows" pattern — Redis serves these in sub-millisecond. We'd also
lose Redis's pub/sub for WebSocket fan-out and would need a second tool just
for that.

**Why not TimescaleDB for hot.** TimescaleDB is Postgres + extension. We
already have Postgres for the cold tier; adding hypertables doesn't replace
the need for a sub-millisecond layer for `LRANGE` and pub/sub.

**Why bounded LRANGE is fine.** `LRANGE` is O(n) — but `n` is bounded by
`LTRIM 0 9999`. That cap is set by the consumer, not the client. A noisy
tenant can't make `LRANGE` slow because it can't grow the list.

**At scale.** The Redis budget is `maxmemory 512mb` with `allkeys-lru`. At
~500 bytes/event × 10,000 entries × 5 services = 25 MB per tenant; supports
~20 tenants worth of hot data on a single 512 MB node. The 10× scaling story
is Redis Cluster (3 primaries × 4 GB) + MessagePack for ~30% size reduction,
detailed in `docs/performance.md` §3.

---

## 3. Alert deduplication design

**The problem.** A cascading failure can fire twenty `error_rate_spike`
anomalies in five seconds. The customer should get exactly one alert per
service-anomaly pair per hour, even with two `alert-engines` consumers racing
on the same Kafka topic.

**The mechanism.**

```python
# backend/app/services/alert_engine.py:85
async def is_duplicate(tenant_id, dedup_key, redis):
    set_key = f"alerts:dedup:{tenant_id}"
    added = await redis.sadd(set_key, dedup_key)  # atomic
    if added:
        await redis.expire(set_key, 3600)
    return added == 0
```

Three things are doing the work here:

1. **`SADD` atomicity.** Redis's `SADD` is single-command-atomic. It returns
   1 when the member was new and 0 when it already existed. Two consumers
   racing on the same `dedup_key` cannot both get `1`. Whichever one gets `0`
   drops the alert and commits its Kafka offset; whichever gets `1` proceeds
   to webhook delivery.
2. **The dedup key shape:** `sha256(tenant_id:service:anomaly_type:floor(now/3600))`
   (`alert_engine.py:41`). The hour bucket is what gives us "1 alert per
   hour" for free — the next hour produces a different key and a new alert
   becomes possible.
3. **TTL of 3600 s on the dedup set.** When the hour rolls over, the set
   self-expires. We never garbage-collect. Memory is bounded.

**Plus a Postgres backstop.** The `alerts` table has
`UNIQUE INDEX uq_alerts_dedup_key ON alerts (dedup_key)`
(`docs/data_models.md` §1.4). If Redis is evicted under memory pressure, two
alerts could conceivably pass the SADD check; the database insert collision
catches them and we drop on conflict
(`alert_engine.py:217-221`). Defence in depth.

**Rate limiting is separate.** A second counter `alerts:rate:{tenant_id}` is
`INCR`'d on every accepted alert and capped at `ALERT_RATE_LIMIT_PER_HOUR=10`
(`alert_engine.py:98-104`). This prevents alert storms from a misconfigured
tenant from saturating their webhook endpoint. Note that rate-limited alerts
still get an `alerts` row written with `delivery_status="pending"` so the
customer can see what was suppressed.

**Why not a Postgres `UNIQUE` alone.** That works for correctness, but at
load you pay a network round-trip + a transaction + a rollback for *every*
duplicate. Redis SADD finishes in ~50 µs on the same data center. Under burst
load, the Postgres-only design pegs a connection pool while the Redis design
doesn't even break a sweat. That's the difference between graceful
degradation and an alert storm taking down the alerting service.

**At scale.** Redis SADD throughput is ~100k ops/sec per node — about 60×
above our peak. The Postgres `UNIQUE` is the only thing that scales with
tenant count, and at 100 tenants × 24 hours × 5 services × 4 anomaly types =
48,000 dedup keys/day, the index is irrelevant.

---

## 4. Statistical vs. AI anomaly detection

**The problem.** Claude is too slow (1–5 s per call) and too expensive to run
on every log. But pure statistics can't tell you *why* — they can only tell
you *that*. We need both, layered.

**Stage 1 — statistical, in `backend/app/services/anomaly_detector.py`.**
Four independent detectors run every 30 s per `(tenant, service)`:

- **`volume_spike`.** Sum the last 5 minutes of `vol:{epoch_5min}` Redis
  counters; compare to a pre-computed 7-day rolling baseline. If
  `current > baseline × ANOMALY_VOLUME_SPIKE_MULTIPLIER` (default 2.5×), fire.
  See `check_volume_anomaly` in `anomaly_detector.py:115`.
- **`volume_drop`.** Same window, but `current < baseline × ANOMALY_VOLUME_DROP_MULTIPLIER`
  (default 0.2×) and `current > 0`. Catches services going dark — frequently
  the first symptom of a deploy gone wrong.
- **`error_rate_spike`.** `current = errors / total` over the 5-min window;
  compare to baseline error rate. Trigger when current is `ANOMALY_ERROR_RATE_MULTIPLIER`
  (default 3×) above baseline. Special case: when baseline is zero and current
  is non-zero, that's anomalous by definition.
- **`new_error_pattern`.** Pull the last 50 logs from the Redis hot path,
  filter to `ERROR`/`CRITICAL`, normalize each message (strip UUIDs, IPs,
  numeric IDs, long quoted strings — see the `_STRIP` regex at line 70),
  truncate to 256 chars, and `SISMEMBER` against
  `tenant:{id}:service:{name}:error_patterns`. New template ⇒ new pattern.
  Severity is fixed at 0.6 (medium-high) because new errors are inherently
  significant.

Stage 1 is sub-millisecond per check. Most ticks produce nothing. When they
do, the anomaly is INSERTed into Postgres and published to `logs.anomalies`.

**Stage 2 — Claude Haiku, in `backend/app/consumers/anomaly_consumer.py`.**
The `claude-analyzers` consumer group reads `logs.anomalies`, fetches the
last 50 logs for the service from Redis, builds a prompt, and calls Claude
under `asyncio.Semaphore(10)` with a 25-second timeout. The system prompt
(`anomaly_consumer.py:44`) demands a strict JSON schema:

```json
{
  "root_cause": "One sentence describing likely cause",
  "confidence": 0.0,
  "affected_components": ["list", "of", "components"],
  "recommended_actions": ["action1", "action2", "action3"],
  "severity_assessment": "LOW|MEDIUM|HIGH|CRITICAL",
  "similar_incidents": "...",
  "estimated_resolution_time": "X minutes/hours"
}
```

The result is parsed, attached to the anomaly row (`UPDATE anomalies SET claude_analysis = ...`),
and forwarded to `logs.alerts`. Crucially, **a Claude failure does not stop
the alert**: the alert payload is still published with `claude_analysis=None`
and the alert engine generates a templated description instead
(`anomaly_consumer.py:160-187` — note the `try/except` around `producer.send`,
not around the Claude call).

**Why the two stages compose.**

- **Cost.** Stage 1 runs every 30 s for every active `(tenant, service)`
  pair. Stage 2 runs only for confirmed anomalies — typically a handful per
  hour per tenant. Per-call API spend is bounded.
- **Latency.** Stage 1 is part of the live counter loop and produces
  anomalies within seconds of the symptom. Stage 2 takes 1–5 s; even at p99
  it stays inside the 30 s NFR-10 budget.
- **Quality.** Stage 1 catches *what* changed (volume, errors, new
  patterns). Stage 2 fills in *why* — affected components, recommended
  remediation. The webhook payload contains both
  (`alert_engine.py:181-196`).
- **Failure isolation.** Anthropic's outage doesn't take down our alert
  pipeline. The pipeline gracefully degrades to Stage-1-only with templated
  descriptions.

**Per-tenant credit tracking.** Every Claude call increments
`tenant:{id}:analysis_credits:{epoch_day}` so plan caps and billing can be
enforced (`anomaly_consumer.py:148-158`).

---

## 5. Multi-tenant isolation strategy

**The problem.** A tenant must never read another tenant's data. Not in a
query, not in a rate-limit counter, not on a WebSocket channel, not via a
Kafka rebalance — anywhere.

**The defence at every layer.**

1. **Authentication boundary.** `X-API-Key` is the only place `tenant_id`
   enters the system. `get_current_tenant` (`backend/app/core/auth.py:38`)
   bcrypt-verifies the key against the cached tenant list and returns a
   `TenantContext` dataclass containing `tenant_id`. Every router takes it as
   a `Depends(get_current_tenant)` and reads `tenant.tenant_id` — *never* a
   `tenant_id` from the request body. This is structurally enforced; you
   can't accidentally trust the client.
2. **Postgres.** Every `WHERE` clause on a tenant-scoped table includes
   `tenant_id == uuid.UUID(tenant.tenant_id)`. Examples:
   `app/api/v1/anomalies.py:47`, `app/api/v1/alerts.py:47`,
   `app/services/log_service.get_historical_logs`.
3. **Redis.** Every key is namespaced. The conventions are documented in
   `docs/data_models.md` §2:
   - hot logs: `tenant:{tenant_id}:service:{service_name}:logs`
   - counters: `tenant:{tenant_id}:service:{service_name}:vol:{epoch_5min}`
   - dedup: `alerts:dedup:{tenant_id}`
   - rate: `rate:{tenant_id}:{minute}` and `alerts:rate:{tenant_id}`
   - pub/sub: `tenant:{tenant_id}:service:{name}:stream` and `tenant:{tenant_id}:anomalies:stream`
   A bug that drops the prefix would fail in the most visible way (404, key
   not found) — not silently leak.
4. **Kafka.** Every produce call passes `key=tenant.tenant_id`
   (`api/v1/ingest.py:47`). Same tenant ⇒ same partition ⇒ same consumer
   instance — both for ordering and for blast-radius isolation.
5. **WebSockets.** The connect handler authenticates via `?token=`
   (`backend/app/api/v1/websocket.py:20`), resolves `tenant_id`, and only
   subscribes to the tenant-scoped Redis channel. There is no way to
   subscribe to another tenant's channel without first holding their API key.
6. **Rate limiting.** The counter key `rate:{tenant_id}:{minute}` is
   per-tenant. One tenant exhausting their budget never blocks another.

**What can't happen.** Cross-tenant data leakage requires either (a) a bug in
auth that returns the wrong `TenantContext`, (b) a missing `WHERE tenant_id`
in a query, or (c) a missing prefix in a Redis key. (a) is a single
chokepoint that's easy to test. (b) and (c) get spotted in code review
because they're stylistically obvious — every existing query and key follows
the same pattern.

**At scale.** Adding a tenant is `O(1)` in every layer. Logically partitioning
Postgres by `tenant_id` for very large tenants is straightforward
declarative partitioning that the code doesn't need to know about. Redis
Cluster shards on the key, and our keys all start with `tenant:{id}` — so
each tenant's data co-locates on the same shard automatically.

---

## 6. WebSocket real-time streaming design

**The problem.** Browsers want a live tail of logs as they're ingested.
Pushing the whole ingest stream into the WebSocket layer would couple log
persistence to slow clients; one bad WiFi connection would back-pressure the
entire ingest pipeline.

**The mechanism.** The pipeline dictates the order: Kafka ⇒ log-processor ⇒
Postgres + Redis hot ⇒ `logs.processed` ⇒ websocket-streamer ⇒ Redis pub/sub
⇒ FastAPI WebSocket route ⇒ browser.

- **Decoupled fan-out.** Persistence is the source of truth. The
  log-processor commits to Postgres and Redis *first*, then produces to
  `logs.processed`. The websocket-streamer consumer reads `logs.processed`
  and `PUBLISH`es onto `tenant:{tid}:service:{svc}:stream`. So a slow client
  cannot stall persistence — it can only stall the pub/sub fan-out, and
  Redis pub/sub *drops* on slow subscribers (`fire-and-forget` semantics).
- **Connection lifecycle in `backend/app/api/v1/websocket.py`.** Authenticate
  the `?token=` query parameter via the same bcrypt path as REST. Subscribe
  to the tenant-scoped channel. Run a single loop that calls `pubsub.get_message(timeout=1.0)`
  and forwards each message as `{"type": "log", "data": {...}}`. Every 30 s,
  send `{"type": "ping", "ts": <unix>}` and reset the timer. On
  `WebSocketDisconnect`, unsubscribe and close the pubsub connection in a
  `finally` block (`websocket.py:93-101`).
- **Anomaly stream is parallel, not derived.** The claude-analyzer
  publishes the *enriched* anomaly (with `claude_analysis` attached) onto
  `tenant:{tid}:anomalies:stream` (`anomaly_consumer.py:170-177`). The
  browser subscribed to `/ws/anomalies` sees the post-Claude object, not the
  Stage-1 raw anomaly — exactly what the dashboard wants.

**Why not SSE.** SSE is one-way. We need:

- Bidirectional pong handling for half-open detection (the loop already
  drains client `pong` frames at `websocket.py:78-89`).
- Future client-driven filter changes ("now subscribe me to the orders
  service too") on a single connection.
- Mature browser tooling; every observability dashboard ships WebSocket.

SSE would have been simpler to debug with `curl`, but the moment you need a
control channel, you re-invent half of WebSocket badly.

**Why publish from the consumer, not from the API.** Two reasons:

1. **Ordering guarantee.** Persistence happens *before* publish, in the same
   process. A subscriber cannot see a message that hasn't been written yet —
   they can refetch from Postgres if they reconnect mid-stream.
2. **Backpressure isolation.** The API's job is to absorb 100k events/min and
   return 202. Coupling it to fan-out would mean every slow client adds
   latency to the ingest path.

**At scale.** Tens of thousands of idle WebSocket connections are FD-limited,
not CPU-limited, on a single FastAPI process. Per-connection memory is
~50 KB. For the current scope (hundreds of dashboard users), nothing here
sweats. Production would put the WebSocket route on its own dedicated
deployment with sticky sessions at the load balancer.

---

## 7. Two-phase write pattern (Kafka → consumer)

**The problem.** A naive ingest endpoint writes synchronously to Postgres on
the request path. Every slow query becomes user-visible latency. Every
network hiccup becomes a 5xx. The whole system collapses under load because
the slowest dependency dictates the speed of the fastest call.

**The two-phase write.**

- **Phase 1: ingest API → Kafka.** `POST /api/v1/logs/ingest` validates the
  body, checks the per-minute rate limit (one Redis `INCR`), enqueues the
  message into the Kafka producer's local buffer with `publish_async`, and
  returns HTTP 202 with `event_id`. See `backend/app/api/v1/ingest.py:31-49`.
  No Postgres call. No Redis hot-path call. Server-side time: typically 1–3 ms.
- **Phase 2: log-processor consumer → Postgres + Redis.** A separate process
  group (`log-processors`, 12 instances) polls `logs.raw` in batches of up
  to 500 (`max.poll.records=500`) or 1 second whichever comes first, does
  the heavy work, and `commit()`s offsets only after success.

**Why ingest returns 202, not 201/200.** 202 ("Accepted") is exactly the
right verb. We are *not* asserting the data is durably stored — we are
asserting the platform has *taken responsibility* for storing it. Kafka
produce with `acks=all` and `enable.idempotence=true` makes that guarantee.

**At-least-once delivery, in detail.**

1. The producer ACKs only after the broker writes to all in-sync replicas
   (`acks=all`). Network failure before ACK ⇒ producer retries; idempotence
   makes the retry a no-op if the broker already saw it.
2. The consumer polls a batch with `enable.auto.commit=false`.
3. The consumer does the side effects: bulk Postgres INSERT, Redis pipeline,
   produce to `logs.processed`.
4. *Only if all three succeed*, `consumer.commit()`.
5. On crash before commit, the next consumer instance polls from the last
   committed offset and replays the same batch. The Postgres insert is
   idempotent because each event carries a UUID generated at ingest
   (`event_id` in `_build_kafka_message`, `ingest.py:16-28`). Redis writes
   are idempotent too (LPUSH+LTRIM produces the same hot list).

**Crash semantics, walked through.**

- *Consumer dies mid-batch.* Offsets not committed. The broker reassigns the
  partition; the new owner replays the batch. At-least-once.
- *Postgres is down.* The batch insert fails; the consumer logs and retries
  with exponential backoff (100 ms, 500 ms, 2.5 s). On final failure, the
  whole batch goes to `logs.raw.dlq` and the offset commits — the alternative
  is head-of-line blocking forever.
- *Redis is down.* Logged warning; hot-path writes skipped; the message still
  produces to `logs.processed`. This is graceful degradation per NFR-12.
  Anomaly detection breaks for that window; persistence does not.
- *Kafka is unreachable from the API.* `publish_async` raises; the API
  returns 503 (we explicitly do not silently drop — see NFR-12 in
  `docs/architecture.md` §4.4).

**The DLQ pattern.** Each consumer pairs with `{topic}.dlq`, 7-day retention.
A poison message (schema invalid, persistent DB failure, persistent Claude
4xx) gets the original payload plus an envelope written to the DLQ:

```json
{
  "original_payload": { /* original */ },
  "error": "ValidationError: severity must be one of ...",
  "exception_type": "ValidationError",
  "stack_trace": "...",
  "first_seen_at": "...",
  "last_seen_at": "...",
  "retry_count": 3,
  "source_topic": "logs.raw",
  "source_partition": 7,
  "source_offset": 12345
}
```

There's no automatic re-drive — DLQ messages are inspected manually via a
CLI tool. The point is *triage without head-of-line blocking*: one bad
message can't stop a billion good ones.

**At scale.** The two-phase pattern is what lets us hit 100k events/min on a
single host without the request path ever touching a database. The
consumer-side batch insert (`executemany`, 500 rows) sustains
~10,000 inserts/sec on commodity hardware — comfortably above the 1,667/sec
target. The 10× story is documented in `docs/performance.md` §1–§5: more
brokers, partitioning `logs` by `created_at` daily, Redis Cluster for the
hot tier, and an Anthropic tier upgrade with a priority queue for
high-severity anomalies.
