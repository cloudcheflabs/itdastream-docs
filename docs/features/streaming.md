# Streaming (Kafka → Iceberg / Multi-Sink)

ItdaStream includes a built-in, Flink-style streaming engine. The **controller broker acts as
the streaming master** and **every other broker acts as a worker**, so streaming runs on the
same cluster that stores your topics — no separate compute cluster to operate.

Two ways to run a pipeline:

- **No-code** — in the Admin UI pick a source topic, a parallelism, a format (JSON/Avro), and a
  target (e.g. an Iceberg table referenced through a named connection); records flow continuously
  into the sink. No job to compile or deploy. See [No-Code Kafka → Iceberg](streaming-no-code.md).
- **SDK** — submit a Java job with `filter` / `select` / `map` / `flatMap` transforms to any of
  the supported sinks. See the [Streaming SDK](streaming-sdk.md).

Both paths produce the same job specification and run on the same engine. For configuration,
monitoring, scaling, and recovery see [Operations & Tuning](streaming-operations.md).

---

## Key properties

- **Exactly-once to transactional sinks** (Iceberg, JDBC): the sink commit and the Kafka offset
  commit are aligned at a checkpoint barrier, so a failure never loses or double-writes records.
  Non-transactional sinks (Kafka, Elasticsearch, HTTP, console) are at-least-once.
- **Parallelism = consumer-group size**: a job runs N consumer threads, spread across the worker
  brokers, all in the consumer group `stream-<jobId>`. ItdaStream's existing group coordinator
  distributes the topic partitions across them and rebalances on membership change.
- **Durable checkpoints on S3**: each checkpoint snapshots the Kafka offsets to S3 (the
  ExchangeManager), KMS-encrypted, so a job reassigned to a different broker resumes exactly
  where it left off.
- **Connection registry**: credentials (S3, Iceberg catalog, Kafka, Elasticsearch, JDBC,
  NeorunBase) are registered once and referenced by `connectionId` — jobs never inline secrets.
  See [Connections](connections.md).

---

## Architecture

Every broker runs the same `StreamingService`, which plays one or both roles:

- **Master (the controller broker).** Watches `/streaming/jobs`, computes a per-broker thread
  assignment — a `{brokerId: threadCount}` map — and writes it to `/streaming/assignments/<jobId>`.
  It also runs one `CheckpointCoordinator` per job, triggering checkpoint barriers to the workers
  over the internal NIO channel.

- **Worker (every broker).** Reconciles the set of running executor threads to match this broker's
  assignment. Each thread is a `StreamingJobExecutor`: a Kafka consumer in the group
  `stream-<jobId>` that deserializes (JSON/Avro), applies `filter` / `select` / `map` / `flatMap`,
  and writes to the sink (Iceberg, Kafka, …).

The data flows topic → executor threads → sink; in parallel, each executor snapshots its Kafka
offsets to the S3 ExchangeManager and commits the sink at every checkpoint. So the **control
plane** (assignments) travels through ZooKeeper, the **checkpoint barriers** through internal NIO,
and the **data plane** is the consumer group reading partitions and writing to the sink.

The control plane is **ZooKeeper-declarative**: the master writes the desired per-broker thread
count, and each worker is a level-triggered reconciler that converges its running executor
threads to its assignment (and heals any missed watch on a periodic tick). The runtime
**checkpoint barriers** travel over the internal NIO channel, which now runs on every broker.

### Job lifecycle

1. A job spec is written to `/streaming/jobs/<jobId>` (by the Admin UI or the SDK, through the
   admin API).
2. The controller's reconciler computes an assignment and writes
   `/streaming/assignments/<jobId>` (a single `{brokerId: threadCount}` map).
3. Each worker starts/stops `StreamingJobExecutor` threads to match its assignment. Each thread
   is one consumer in the group `stream-<jobId>`.
4. The controller's `CheckpointCoordinator` triggers checkpoint barriers on an interval; each
   executor also runs a time-based self-checkpoint as a fallback.
5. Deleting the job removes the spec; the reconcilers stop the executors.

---

## Exactly-once

For a transactional sink the poll loop **never advances Kafka offsets**. At each checkpoint the
executor performs, in order:

1. **commit the sink** — for Iceberg, the data files written since the last checkpoint are
   committed as one snapshot; for JDBC, the transaction is committed;
2. **snapshot the Kafka offsets** to the S3 ExchangeManager under the checkpoint id;
3. **commit the Kafka offsets** (the commit point);
4. **acknowledge** the controller's coordinator.

Because the offset only advances after the sink commit and the durable offset snapshot, a crash
either replays from the last committed checkpoint (sink not yet committed) or resumes cleanly
(sink committed, offsets advanced) — exactly-once. On startup the executor restores the last
completed checkpoint's offsets from S3 and seeks the consumer there.

For idempotent updates, set **upsert keys** on the Iceberg sink: each commit writes the data plus
an equality-delete on the keys, so re-processed rows collapse to the latest value.

---

## The two ways to run

- **[No-Code Kafka → Iceberg](streaming-no-code.md)** — a step-by-step walkthrough of configuring
  a topic-to-Iceberg pipeline entirely in the Admin UI.
- **[Streaming SDK](streaming-sdk.md)** — submit richer Java jobs with transforms and any sink.

Both reference credentials through the [Connection Registry](connections.md) and run on the same
engine; both produce the same job spec (see the REST API below).

---

## Supported sinks

| Sink | Type | Semantics | Notes |
|---|---|---|---|
| Iceberg | `table` | exactly-once | append or equality-delete upsert; REST catalog (Polaris / vanilla REST / Glue) |
| JDBC | `jdbc` | exactly-once | transactional; PostgreSQL driver bundled, others added to the classpath |
| Kafka | `kafka` | at-least-once | produce to another topic/cluster |
| Elasticsearch | `elasticsearch` | at-least-once | bulk index |
| HTTP | `http` | at-least-once | webhook POST |
| Console | `console` | at-least-once | debug |
| NeorunBase | `neorunbase` | at-least-once | REST or JDBC mode |

---

## REST API

Job management is exposed under the admin API (JWT or IAM user-token auth; the admin endpoint
forwards mutations to the controller, so any broker works):

- `POST /admin/streaming/jobs` — submit a job spec, returns `{jobId}`
- `GET  /admin/streaming/jobs` — list job specs
- `GET  /admin/streaming/jobs/{id}` — spec + per-broker status
- `DELETE /admin/streaming/jobs/{id}` — stop and delete

Example job spec:

```json
{
  "name": "purchases-to-iceberg",
  "parallelism": 4,
  "kafka": { "topic": "events", "format": "json" },
  "operations": [
    { "type": "FILTER", "value": "event_type = 'purchase'" },
    { "type": "SELECT", "value": "id,name,amount" }
  ],
  "sink": {
    "type": "table",
    "connectionId": "prod-iceberg",
    "table": "analytics.purchases",
    "upsertKeys": ["id"]
  },
  "commitIntervalMs": 5000,
  "checkpointIntervalMs": 5000
}
```

---

## Configuration, monitoring, and recovery

Broker-level defaults (commit/checkpoint cadence, reconcile interval, exchange prefix, …) live in
`itdastream.properties` and are overridden per job. For the full property reference, the status
API, scaling/rebalance behavior, failure recovery, and troubleshooting, see
[Operations & Tuning](streaming-operations.md).
