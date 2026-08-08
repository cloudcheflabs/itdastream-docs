# Streaming Job Logs, Kill & Iceberg Sinks

Every streaming job — whether submitted with the [SDK](streaming-sdk.md)/`submit.sh` or created as a
[no-code auto Iceberg sink](streaming-no-code.md) — is the **same streaming job type** running in the
same engine. So the same operational surface applies to both: **live per-job logs**, **log history**,
and **kill**. The Admin UI also has a dedicated **Iceberg Sinks** page for configuring many
topic → table mappings at once.

---

## Live per-job logs

A streaming job runs as consumer-thread executors spread across the brokers. itdastream captures the
log lines each executor emits (poll loop, sink writes, checkpoints, errors) **per job** and lets you
tail a single job without grepping the shared broker log.

**Admin UI** — on the **Streaming Jobs** page (or **Iceberg Sinks**), click the terminal icon on a
job. A dark terminal opens and **live-tails** the job's logs (polls every 2 s, auto-scroll, download,
severity colouring, a `LIVE` badge). Lines are tagged with the broker that produced them (`[b<id>]`),
so a distributed job's logs from every broker appear merged in one view, in time order.

**REST** — the same data:

```bash
# tail one job's logs (merged across all brokers)
curl -s "http://broker:8080/admin/streaming/jobs/<jobId>/log" \
  -H "Authorization: Bearer $JWT" | jq -r '.lines[]'
# → {"jobId":"...","totalLines":740,"from":0,"lines":["[b152456] 2026-... committed 200 rows", ...]}
```

### How it works (and why it survives a restart)

- Each broker buffers a job's lines in memory (ring buffer, `itdastream.streaming.joblogs.max.lines`,
  default 5000) keyed by the job id, and **flushes them to the object store** every
  `itdastream.streaming.joblogs.flush.interval.ms` (default 3 s) at
  `<prefix>/<jobId>/broker-<brokerId>.log` (`prefix` = `itdastream.streaming.joblogs.prefix`,
  default `jobLogs`).
- The log endpoint **merges every broker's flushed file with the serving broker's live in-memory
  buffer**, sorts by timestamp, and returns it. So it works for a distributed job served from any
  broker, and — because the logs live in the object store — **keeps working after the job is killed**.

Disable the whole feature with `itdastream.streaming.joblogs.enabled=false` (jobs still log to the
broker's main log file).

---

## Log history

Killed/ended jobs disappear from the live job list, but their logs are retained in the object store.
The **Streaming Jobs → Log History** tab lists every job that has persisted logs (running or ended,
with the brokers it ran on, size, and last-modified time); click one to open the same viewer.

**REST**:

```bash
curl -s "http://broker:8080/admin/streaming/joblogs" -H "Authorization: Bearer $JWT" | jq
# → [{"jobId":"...","size":266450,"lastModified":...,"brokers":[152456,202732,781607],"active":true}]
```

The `/admin/streaming/jobs/<jobId>/log` endpoint also works for an already-killed job id (it reads
the object store, not ZooKeeper), so a dashboard or script can fetch a finished job's full logs.

---

## Kill a job

Killing a job stops its executors on every broker and removes it from the cluster. The job's **log
history is retained** in the object store.

**Admin UI** — the trash icon on the job (admin only); confirm the dialog (uncommitted records since
the last checkpoint may be lost).

**REST**:

```bash
curl -s -X DELETE "http://broker:8080/admin/streaming/jobs/<jobId>" \
  -H "Authorization: Token $ITOK"
```

Mechanically, the spec znode is deleted; the reconciler on each broker then stops that job's
executors. For an append-only Iceberg sink the in-flight data files are re-read from the last
committed Kafka offsets on the next run (see
[Single-committer](streaming-operations.md#single-committer-append-only-iceberg)).

---

## Iceberg Sinks page — configure many topics → many tables

Real deployments map **many topics to many Iceberg tables**. The Admin UI **Iceberg Sinks** page is a
dedicated surface for the no-code auto Iceberg sink: configure all the mappings at once instead of
creating jobs one by one.

- **Add Iceberg Sinks** opens a form with a shared **Iceberg connection**, **parallelism**, **commit
  interval**, and **time partition** (`hour`/`day`/`month`/`year`/off), plus a repeatable list of
  rows: **topic → target table** (+ optional upsert keys). **Create** provisions one no-code
  auto-sink job per row.
- Each mapping is an ordinary streaming job with a `table` sink and **no MAP/FLATMAP code**, so the
  engine auto-adds the [`_ingest_ts` hidden time partition](streaming-no-code.md#automatic-time-partitioning-_ingest_ts)
  and (for append-only) uses the
  [single-committer](streaming-operations.md#single-committer-append-only-iceberg).
- The **Active Sinks** table shows every mapping (topic, table, format, partition/upsert, connection)
  with inline **live logs** and **kill** per row.

This is just a specialised view over the same `POST /admin/streaming/jobs` API and the same engine —
nothing here is a separate job type. Custom-transform pipelines stay on the
[Streaming Jobs](streaming-sdk.md) page.

---

## Cluster dashboard

The **Dashboard** aggregates per-broker telemetry — **CPU %**, **JVM heap used vs max**, **throughput
(API TPS)**, and **network in/out (pkt/s)** — for **every** broker, collected by the controller from
each broker's `/admin/metrics` and stored in the leader's metrics store. Each chart overlays one
series per broker so you can spot an outlier (a hot broker, a memory-heavy node) at a glance.

---

## See also

- [Operations & Tuning](streaming-operations.md) — config keys, single-committer, recovery.
- [No-Code Kafka → Iceberg](streaming-no-code.md) — the auto-sink + `_ingest_ts` partitioning.
- [Streaming SDK](streaming-sdk.md) / [Tutorial](streaming-job-tutorial.md) — code pipelines + `submit.sh`.
