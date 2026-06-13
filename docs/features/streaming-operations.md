# Streaming — Operations & Tuning

How to run, observe, scale, and troubleshoot streaming jobs in production.

---

## Configuration

Broker-level defaults live in `itdastream.properties`; a per-job spec (from the Admin UI or the
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

!!! note "Arrow off-heap (required)"
    The engine processes records as Apache Arrow batches in off-heap memory. The broker launch
    script passes `--add-opens=java.base/java.nio=ALL-UNNAMED` (and
    `-Dio.netty.tryReflectionSetAccessible=true`) so Arrow can allocate direct buffers. If you
    use a custom launcher, keep these flags or jobs fail at first allocation with
    `InaccessibleObjectException`.

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

At each checkpoint an executor commits the sink, snapshots the Kafka offsets to S3 (the
ExchangeManager, under `streaming/exchange/<jobId>/`, KMS-encrypted), then commits the Kafka
offsets. Because the offset only advances **after** the sink commit and the durable snapshot:

- **Worker broker dies** → its partitions rebalance to other workers' consumers, which resume
  from the last committed offset; the controller re-spreads thread assignments after a short
  stabilization. No rows are lost; transactional sinks (Iceberg/JDBC) do not double-write.
- **Controller dies** → a new controller is elected, re-reads `/streaming/jobs`, and re-publishes
  assignments + checkpoint coordination. The data plane keeps running independently of who is
  controller.
- **Job reassigned to a fresh broker** → the executor restores the last completed checkpoint's
  offsets from S3 and seeks the consumer there, so it picks up exactly where it left off.

For change streams, set **upsert keys** so any boundary replays collapse to the latest value.

---

## Troubleshooting

| Symptom | Cause / fix |
|---|---|
| Iceberg writes fail with *"No route for request: GET v1/iceberg/namespaces/..."* | The connection uses the default `polaris` flavor against a vanilla REST catalog. Set `catalog.rest.flavor=rest` on the Iceberg connection. |
| Job runs, consumer lag drains, but the Iceberg table stays empty | The sink commits on the checkpoint interval — wait at least one `commitIntervalMs`. Also confirm the target **namespace** exists (the table is auto-created, the namespace is not). |
| `Connection not found: <id>` | The job references a `connectionId` that is not registered (or not yet synced to the worker). Create it in the Connection Registry; it syncs to all brokers. |
| `InaccessibleObjectException` when a job starts | The Arrow `--add-opens` JVM flag is missing from the launcher (see the note above). |
| JDBC / NeorunBase(jdbc) sink fails with *"No suitable driver"* | The driver is not on the broker classpath. PostgreSQL is bundled; add other JDBC drivers to `lib/`. |
| `map`/`flatMap` job fails to start | The referenced `MapFunction`/`FlatMapFunction` class is not on the broker classpath or lacks a public no-arg constructor. |

---

## Capacity notes

Each running consumer thread holds an Arrow allocator and (for Iceberg) a Parquet writer that
buffers a row group on-heap. Size broker memory for the expected number of concurrent
jobs × threads, and bound buffering with `commit.row.threshold` / `commit.interval.ms`. The
data-lake libraries (Arrow, Iceberg, Parquet, Hadoop) run in the broker process, so plan heap and
direct-memory accordingly.
