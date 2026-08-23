# Streaming — Operations & Tuning

How to run, observe, scale, and troubleshoot streaming jobs in production.

---

## Configuration

Broker-level defaults live in `itdastream.properties` (every key is described in
[Configuration → Streaming Engine Settings](configuration.md#streaming-engine-settings));
a per-job spec (from the Admin UI or the
SDK) overrides any value it sets explicitly. See
[Flexible Configuration System](configuration.md) for the resolution order (system property >
environment variable > file > default).

| Property | Default | Meaning |
|---|---|---|
| `itdastream.streaming.reconcile.interval.ms` | `5000` | how often the reconciler runs (assignment + executor convergence) |
| `itdastream.streaming.exchange.prefix` | `streaming/exchange` | S3 prefix for durable checkpoint state |
| `itdastream.streaming.commit.interval.ms` | `10000` | default sink commit cadence (exactly-once cadence for tx sinks) |
| `itdastream.streaming.checkpoint.interval.ms` | `10000` | default checkpoint barrier interval |
| `itdastream.streaming.commit.row.threshold` | `10000` | rows that also trigger a commit (bounds buffered rows) |
| `itdastream.streaming.source.poll.timeout.ms` | `100` | Kafka poll timeout inside the loop |
| `itdastream.streaming.progress.log.interval.ms` | `30000` | progress log interval |
| `itdastream.streaming.iceberg.coordinated.commit.enabled` | `true` | single-committer for append-only Iceberg sinks (see [below](#single-committer-append-only-iceberg)) |
| `itdastream.streaming.autosink.ingest.ts.enabled` | `true` | auto-add an ingestion-time column on the no-code Iceberg sink (see [No-Code Kafka → Iceberg](streaming-no-code.md#automatic-time-partitioning-_ingest_ts)) |
| `itdastream.streaming.autosink.ingest.ts.column` | `_ingest_ts` | name of that synthetic column |
| `itdastream.streaming.autosink.ingest.ts.partition` | `hour` | hidden-partition transform for it (`hour`/`day`/`month`/`year`) |
| `itdastream.streaming.joblogs.enabled` | `true` | capture + flush per-job logs for the admin log viewer (see [Job Logs & Lifecycle](streaming-job-logs.md)) |
| `itdastream.streaming.joblogs.prefix` | `jobLogs` | object-store key prefix for per-job logs |
| `itdastream.streaming.joblogs.flush.interval.ms` | `3000` | how often each broker flushes a job's logs to the object store |
| `itdastream.streaming.joblogs.max.lines` | `5000` | in-memory log ring-buffer size per job per broker |

Lower commit/checkpoint intervals reduce end-to-end latency but produce more, smaller Iceberg
snapshots — schedule periodic Iceberg compaction accordingly.

### Single-committer (append-only Iceberg) {#single-committer-append-only-iceberg}

When a job writes to an **append-only** Iceberg table (no upsert keys) with parallelism > 1, the
brokers do **not** each commit to the table. Instead every broker only **writes and drains its
Parquet data files** at the checkpoint barrier, and the **controller performs ONE atomic Iceberg
commit per checkpoint** aggregating every broker's files. Because only the controller commits a
given table, concurrent writers can never collide — there is no
`CommitFailedException`/"concurrently modified" at any parallelism.

- Enabled by default (`itdastream.streaming.iceberg.coordinated.commit.enabled=true`). Set `false`
  to fall back to per-broker self-commit (each broker commits its own files; can conflict at high
  parallelism). **Upsert** sinks always use the per-broker path (their equality-delete files are
  per-writer).
- One commit per checkpoint → one Iceberg snapshot per checkpoint interval regardless of
  parallelism (fewer, larger snapshots than per-broker commit).
- **Exactly-once across restart.** Each commit stamps two things into that same snapshot, atomically
  with the data files: a **checkpoint watermark** (`itdastream.max-committed-checkpoint-id` +
  `itdastream.job-id`) and the **Kafka source offsets** (`itdastream.kafka.offsets`). The controller
  commits checkpoints in strict id order and, on (re)start, seeds its checkpoint sequence from the
  committed watermark — so a checkpoint already committed in a prior run is detected and **skipped**
  rather than double-applied. Each source restores its offsets from the latest committed snapshot and
  seeks there, so it never re-reads already-committed records. The Iceberg commit is therefore the
  single source of truth for both *what data is committed* and *how far the source was consumed*.

!!! note "Arrow off-heap (required)"
    The engine processes records as Apache Arrow batches in off-heap memory, which needs
    `--add-opens=java.base/java.nio=ALL-UNNAMED` and `-Dio.netty.tryReflectionSetAccessible=true`.
    These ship as **defaults in `conf/jvm.conf`**, so a stock broker just works. Keep them if you
    edit `conf/jvm.conf` (or pass equivalents from a custom launcher) — without them the broker
    boots fine but a streaming job fails at first Arrow allocation with `InaccessibleObjectException`.

---

## Monitoring

- **Admin UI → Streaming** lists every job with topic, parallelism, format, sink, and status.
- `GET /admin/streaming/jobs` — all job specs.
- `GET /admin/streaming/jobs/{id}` — the spec plus aggregated status: per-broker thread counts
  (from the ephemeral `/streaming/status/<jobId>/<brokerId>` znodes) and the controller's last
  globally-completed checkpoint id.
- Each running executor logs progress (records processed) every
  `itdastream.streaming.progress.log.interval.ms`.
- The job runs as a normal consumer group `stream-<jobId>`, so consumer lag is visible through
  the standard consumer-group tooling and the **Consumer Groups** admin view.

---

## Scaling and rebalancing

Two independent mechanisms keep a job balanced:

1. **Thread assignment (control plane).** The controller computes a `{brokerId: threadCount}`
   map distributing the job's parallelism across the live brokers, and writes it to
   `/streaming/assignments/<jobId>`. Each broker's reconciler converges its running executor
   threads to its share. On broker join/leave the controller recomputes the assignment.
2. **Partition assignment (data plane).** All of a job's threads are consumers in the group
   `stream-<jobId>`, so ItdaStream's group coordinator distributes the topic partitions across
   them and rebalances on membership change.

To change parallelism, edit the job (or resubmit with a new `parallelism`); the controller
re-spreads the threads. Keep `parallelism ≤ partitions` so every thread owns at least one
partition.

---

## Failure and recovery (exactly-once)

For a **coordinated Iceberg sink** (the default append-only path above), the Iceberg snapshot is the
authoritative recovery point: each per-checkpoint commit stamps the checkpoint watermark **and** the
Kafka offsets into the snapshot, atomically with the data. On recovery the controller seeds its
checkpoint sequence from the watermark (skipping already-committed checkpoints) and each source
restores its offsets from the latest snapshot. For other (standalone) sinks, the executor instead
snapshots the Kafka offsets to S3 (the ExchangeManager, under `streaming/exchange/<jobId>/`,
KMS-encrypted) and commits the consumer-group offset after the sink commit.

- **Worker broker dies** → its partitions rebalance to other workers' consumers, which resume
  from the committed position; the controller re-spreads thread assignments after a short
  stabilization. No rows are lost; the committed watermark keeps the Iceberg commit idempotent so a
  replayed checkpoint is never double-applied.
- **Controller dies** → a new controller is elected, re-reads `/streaming/jobs`, re-publishes
  assignments, and **re-seeds the checkpoint coordinator from the committed watermark**, resuming
  commits exactly past the last durable checkpoint. The data plane keeps running independently of
  who is controller.
- **Job reassigned to a fresh broker** → for coordinated Iceberg sinks the source restores its
  offsets from the latest committed Iceberg snapshot and seeks there; for standalone sinks it
  restores from the S3 exchange. Either way it picks up exactly where it left off.

This holds across a rolling restart of the whole cluster (verified end-to-end: produce → commit →
restart all brokers → produce more yields the exact row count, no duplicates, no loss).

For change streams, set **upsert keys** so any boundary replays collapse to the latest value.

---

## Troubleshooting

| Symptom | Cause / fix |
|---|---|
| Iceberg writes fail with *"No route for request: GET v1/iceberg/namespaces/..."* | The connection uses the default `polaris` flavor against a vanilla REST catalog. Set `catalog.rest.flavor=rest` on the Iceberg connection. |
| Job runs, consumer lag drains, but the Iceberg table stays empty | The sink commits on the checkpoint interval — wait at least one `commitIntervalMs`. Also confirm the target **namespace** exists (the table is auto-created, the namespace is not). |
| `Connection not found: <id>` | The job references a `connectionId` that is not registered (or not yet synced to the worker). Create it in the Connection Registry; it syncs to all brokers. |
| `InaccessibleObjectException` when a job starts | The Arrow `--add-opens` JVM flag is missing from the launcher (see the note above). |
| A long-running job starts failing every Iceberg call with *"Not authorized"* | The catalog session's token expired. The broker re-authenticates and retries once on its own (`IcebergCatalogSession`), so a job that stays broken means it has nothing to sign in with — a connection configured with a static `catalog.rest.token` and no `catalog.rest.client_id`/`client_secret`. Add the client credential. See [Iceberg Catalog Sessions](iceberg-catalog-session.md). |
| Iceberg **commits** fail with a vended-credential / unreachable-endpoint error, while reads work | The sink connection is missing `catalog.type=rest`, so the write path did not install the direct S3 FileIO that bypasses credential vending. |
| JDBC / NeorunBase(jdbc) sink fails with *"No suitable driver"* | The driver is not on the broker classpath. PostgreSQL is bundled; add other JDBC drivers to `lib/`. |
| `map`/`flatMap` job fails to start | The referenced `MapFunction`/`FlatMapFunction` class is not on the broker classpath or lacks a public no-arg constructor. |

---

## Capacity notes

Each running consumer thread holds an Arrow allocator and (for Iceberg) a Parquet writer that
buffers a row group on-heap. Size broker memory for the expected number of concurrent
jobs × threads, and bound buffering with `commit.row.threshold` / `commit.interval.ms`. The
data-lake libraries (Arrow, Iceberg, Parquet, Hadoop) run in the broker process, so plan heap and
direct-memory accordingly.
