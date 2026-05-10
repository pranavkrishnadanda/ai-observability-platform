# Kafka Design

This document specifies all Kafka topics, consumer groups, producer configuration, and dead-letter handling for the AI Observability Platform.

---

## 1. Topology Overview

```
client ── HTTP ──> ingest API ──> [logs.raw]
                                       │
                                       ▼
                                 log-processors  ── PostgreSQL (batched insert)
                                       │           ── Redis hot path
                                       ▼
                                 [logs.processed]
                                       │
                ┌──────────────────────┼──────────────────────┐
                ▼                                             ▼
          anomaly-detectors                          websocket-streamers
                │                                              │
                ▼                                              ▼
          [logs.anomalies]                           Redis Pub/Sub channels
                │
                ▼
          claude-analyzers ── Anthropic API
                │
                ▼
          [logs.alerts]
                │
                ▼
          alert-engines ── webhook delivery
```

---

## 2. Topics

### 2.1 logs.raw
| Property | Value |
|----------|-------|
| Partitions | 12 |
| Replication factor | 1 (dev) / 3 (prod) |
| Retention | 1 hour (`retention.ms = 3600000`) |
| Cleanup policy | delete |
| Message key | `tenant_id` (UTF-8 string) |
| Format | JSON |
| Schema | `{tenant_id, service_name, severity, message, metadata, trace_id, span_id, source_ip, environment, ingested_at}` |

`logs.raw` is transient — PostgreSQL is the durable store. Partitioning on `tenant_id` ensures ordered processing per tenant and even distribution across consumers.

**Sample message:**
```json
{
  "tenant_id": "8b1e0c3a-5b41-4a4f-9b0c-1f2a3b4c5d6e",
  "service_name": "checkout-api",
  "severity": "ERROR",
  "message": "Failed to charge card: gateway timeout",
  "metadata": { "user_id": "u_123", "order_id": "ord_456" },
  "trace_id": "9b2f3c4d5e6a7b8c9d0e1f2a3b4c5d6e",
  "span_id": "1a2b3c4d5e6f7a8b",
  "source_ip": "203.0.113.45",
  "environment": "prod",
  "ingested_at": "2026-05-10T12:00:00.123Z"
}
```

### 2.2 logs.processed
| Property | Value |
|----------|-------|
| Partitions | 12 |
| Replication factor | 1 / 3 |
| Retention | 30 minutes |
| Cleanup policy | delete |
| Message key | `tenant_id` |
| Format | JSON |
| Purpose | Validated, persisted logs available to downstream consumers (anomaly-detector, websocket-streamer) |

Schema is the same as `logs.raw` plus `log_id` (the PostgreSQL UUID assigned during persist) and `persisted_at`.

### 2.3 logs.anomalies
| Property | Value |
|----------|-------|
| Partitions | 4 |
| Replication factor | 1 / 3 |
| Retention | 4 hours |
| Cleanup policy | delete |
| Message key | `tenant_id` |
| Format | JSON |
| Purpose | Statistical anomalies awaiting Claude analysis |

**Schema:**
```json
{
  "anomaly_id": "uuid",
  "tenant_id": "uuid",
  "service_name": "string",
  "anomaly_type": "volume_spike|volume_drop|new_error_pattern|error_rate_spike",
  "severity_score": 0.82,
  "detected_at": "iso8601",
  "window_start": "iso8601",
  "window_end": "iso8601",
  "baseline_value": 0.01,
  "observed_value": 0.18,
  "deviation_pct": 1700.0,
  "sample_log_ids": ["uuid", "uuid"]
}
```

### 2.4 logs.alerts
| Property | Value |
|----------|-------|
| Partitions | 4 |
| Replication factor | 1 / 3 |
| Retention | 4 hours |
| Cleanup policy | delete |
| Message key | `tenant_id` |
| Format | JSON |
| Purpose | Analyzed anomalies ready for alert generation and webhook delivery |

**Schema:** anomaly schema above + `claude_analysis: string`.

---

## 3. Consumer Groups

### 3.1 log-processors
| Property | Value |
|----------|-------|
| Subscribes to | `logs.raw` |
| Parallelism | 12 (one per partition) |
| Commit strategy | Manual commit after successful batch write (at-least-once) |
| Batch | 500 logs OR 1 second since first event in batch |
| DLQ | `logs.raw.dlq` after 3 retries |

**Responsibilities:**
1. Validate message schema; route invalid messages directly to `logs.raw.dlq`.
2. Enrich with `log_id = gen_random_uuid()` and `persisted_at`.
3. Bulk-INSERT into PostgreSQL `logs`.
4. `LPUSH` + `LTRIM` to Redis hot path; bump `EXPIRE 3600`.
5. `INCR` Redis volume counter; if severity in `(ERROR, CRITICAL)`, also `INCR` error counter.
6. `SADD tenant:{tenant_id}:services` with `service_name`.
7. Produce enriched message to `logs.processed`.
8. Commit Kafka offsets only after all of the above succeed for the batch.

### 3.2 anomaly-detectors
| Property | Value |
|----------|-------|
| Subscribes to | `logs.processed` |
| Parallelism | 4 |
| Tick cadence | every 30 seconds per `tenant × service` |
| Output | `logs.anomalies` |
| DLQ | `logs.processed.dlq` |

**Responsibilities:**
1. Maintain per-`(tenant, service)` rolling baselines using EWMA over 24 hours of 5-minute buckets.
2. Every 30 s evaluate: `volume_spike`, `volume_drop`, `error_rate_spike` via 3-sigma test against baseline.
3. Compute normalized error template; if not in `error_patterns` set → `new_error_pattern`.
4. Insert row into PostgreSQL `anomalies`.
5. Produce message to `logs.anomalies`.

### 3.3 claude-analyzers
| Property | Value |
|----------|-------|
| Subscribes to | `logs.anomalies` |
| Parallelism | 2 consumers |
| Concurrency cap | semaphore of 10 in-flight Claude calls |
| Output | `logs.alerts` |
| DLQ | `logs.anomalies.dlq` |

**Responsibilities:**
1. Pull anomaly + sample logs (`sample_log_ids`) from PostgreSQL.
2. Call Claude Haiku with prompt template; timeout 25 s.
3. UPDATE `anomalies.claude_analysis`.
4. Increment `tenant:{tenant_id}:analysis_credits:{epoch_day}`.
5. Produce to `logs.alerts`. On Claude failure, publish with templated fallback `claude_analysis`.

### 3.4 alert-engines
| Property | Value |
|----------|-------|
| Subscribes to | `logs.alerts` |
| Parallelism | 2 |
| Output | webhook HTTP POST |
| DLQ | `logs.alerts.dlq` |

**Responsibilities:**
1. Compute `dedup_key = sha256(tenant_id:service:anomaly_type:epoch_hour)`.
2. `SISMEMBER alerts:dedup:{tenant_id}` → if hit, drop (count metric).
3. `INCR alerts:rate:{tenant_id}` → if > 10, mark `delivery_status = "pending"` and skip webhook.
4. Otherwise INSERT into `alerts` table (UNIQUE constraint also enforces dedup).
5. POST webhook with HMAC-SHA256 signature; retry 3 times (1 s, 5 s, 25 s backoff).
6. Update `alerts.delivery_status`, `delivered_at`, `retry_count`, `last_error`.

### 3.5 websocket-streamers
| Property | Value |
|----------|-------|
| Subscribes to | `logs.processed` |
| Parallelism | 4 |
| Output | Redis Pub/Sub |
| DLQ | none (ephemeral) |

**Responsibilities:**
1. For each message, `PUBLISH tenant:{tenant_id}:service:{service_name}:stream` with the JSON log payload.
2. Drop silently on Redis unavailability — streaming is best-effort.

---

## 4. Producer Configuration

All services produce with the following librdkafka / kafka-python settings:

| Parameter | Value | Rationale |
|-----------|-------|-----------|
| `acks` | `all` | Wait for all in-sync replicas. |
| `retries` | `3` | Bounded retry to avoid head-of-line blocking. |
| `retry.backoff.ms` | `100` | |
| `linger.ms` | `10` | Small batching delay for throughput. |
| `batch.size` | `16384` (bytes) | Default; balances throughput and latency. |
| `compression.type` | `lz4` | Fast compression suitable for JSON logs. |
| `enable.idempotence` | `true` | Avoid duplicates from producer retries. |
| `max.in.flight.requests.per.connection` | `5` | Compatible with idempotence. |
| `delivery.timeout.ms` | `30000` | Hard cap on producer ack wait. |

---

## 5. Consumer Configuration

| Parameter | Value | Rationale |
|-----------|-------|-----------|
| `enable.auto.commit` | `false` | Manual commit after side effects. |
| `auto.offset.reset` | `latest` | Start at tail on new consumer. |
| `max.poll.records` | `500` | Aligns with PostgreSQL batch size. |
| `max.poll.interval.ms` | `300000` | 5 min — generous for slow Claude calls in claude-analyzers. |
| `session.timeout.ms` | `30000` | |
| `fetch.min.bytes` | `1` | Low-latency fetch. |
| `fetch.max.wait.ms` | `100` | |
| `isolation.level` | `read_committed` | |

---

## 6. Dead Letter Queue Pattern

Every consumer pairs with a DLQ topic named `{topic}.dlq`:

| Source topic | DLQ topic | Retention |
|--------------|-----------|-----------|
| logs.raw | logs.raw.dlq | 7 days |
| logs.processed | logs.processed.dlq | 7 days |
| logs.anomalies | logs.anomalies.dlq | 7 days |
| logs.alerts | logs.alerts.dlq | 7 days |

**Routing rule:** after 3 retries (or schema-validation failure on first read), the consumer publishes to the DLQ with the original payload plus an envelope:
```json
{
  "original_payload": { /* original message */ },
  "error": "string error message",
  "exception_type": "ValidationError | DBError | ClaudeError | ...",
  "stack_trace": "string",
  "first_seen_at": "iso8601",
  "last_seen_at": "iso8601",
  "retry_count": 3,
  "source_topic": "logs.raw",
  "source_partition": 7,
  "source_offset": 12345
}
```

DLQ messages are inspected manually via a tooling CLI; no automatic re-drive is configured at MVP.

---

## 7. Throughput Sizing

Target: **100,000 events/min = ~1,667 events/sec**.

- `logs.raw` 12 partitions → ~140 events/sec/partition; trivially handled.
- log-processors at 12 consumers, batch 500 → 1 batch every ~360 ms, well within p99 latency targets.
- Average JSON event size assumed ~500 B → ~833 KB/sec raw, ~250 KB/sec compressed (lz4 ~3x).
- claude-analyzers: anomalies are rare (< 1/min/tenant typical); semaphore of 10 covers 100+ tenants.

---

## 8. Operational Notes

- All topics are created with `--config min.insync.replicas=2` in production.
- Topic creation is idempotent and managed via a one-shot bootstrap job (`scripts/kafka_bootstrap.py`).
- Consumer lag is monitored via Prometheus exporter; alert at lag > 10,000 for `logs.raw`.
- Kafka cluster reachability is part of `/health` (NFR-12) — when Kafka is down, ingest endpoints return HTTP 503 rather than silently dropping events.
