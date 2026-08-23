# No-Code Kafka → Iceberg

The most common pipeline — *"messages produced to a topic land in an Iceberg table"* — needs no
code at all. You configure it **once** in the Admin UI; from then on every message produced to the
topic flows into the table with exactly-once semantics. This page is a full walkthrough.

> The basic case is exactly this: produce to an ItdaStream topic, and it goes straight into
> Iceberg. For richer transforms or multiple sinks, use the [Streaming SDK](streaming-sdk.md).

!!! tip "Configuring many topics → many tables"
    To map **many topics to many Iceberg tables at once**, use the Admin UI **Iceberg Sinks** page:
    one screen creates a no-code auto-sink job per topic→table row, and manages them (live logs,
    kill) as a table. See [Job Logs, Kill & Iceberg Sinks](streaming-job-logs.md#iceberg-sinks-page-configure-many-topics-many-tables).

---

## Prerequisites

- An Iceberg **REST catalog** reachable from the brokers (Apache Polaris, the `iceberg-rest`
  image, Nessie, or AWS Glue), and an S3 bucket for the warehouse.
- The source **topic** exists (or will be created).

---

## Step 1 — Register an Iceberg connection

Credentials are stored once in the [Connection Registry](connections.md) and referenced by id —
jobs never inline secrets. In the Admin UI open **Connections → New Connection**, choose type
**ICEBERG**, and fill in:

| Field | Example | Notes |
|---|---|---|
| Connection ID | `prod-iceberg` | referenced by jobs |
| `catalog.rest.uri` | `http://iceberg-rest:8181` | REST catalog endpoint |
| `catalog.rest.flavor` | `rest` | `polaris` (default), `rest` (vanilla iceberg-rest / Nessie), or `glue` |
| `catalog.warehouse` | `s3://warehouse/` | Iceberg warehouse (for `polaris`, the catalog **name**) |
| `catalog.type` | `rest` | required on a sink connection — see the warning below |
| `s3.endpoint` / `s3.region` | `http://minio:9000` / `us-east-1` | S3 FileIO |
| `s3.accessKey` / `s3.secretKey` | … | masked after save |
| `s3.pathStyle` | `true` | for MinIO and most S3-compatible stores |

!!! warning "Pick the right catalog flavor"
    A **vanilla** Iceberg REST catalog (the `iceberg-rest` image, Nessie) serves
    `/v1/namespaces/...` and needs `catalog.rest.flavor=rest`. The default `polaris` flavor
    addresses catalogs by a URL prefix (`/v1/{prefix}/namespaces/...`); using it against a
    vanilla catalog yields *"No route for request"*.

!!! warning "`catalog.type=rest` on a sink connection"
    The direct S3 FileIO that bypasses catalog credential vending is installed on the **write**
    path only when the connection carries `catalog.type=rest`. Without it the sink commits with
    whatever FileIO the catalog returns — under Polaris, vended credentials aimed at the
    catalog's own view of the S3 endpoint. Reads are unaffected, so the symptom is a job that
    reads fine and fails at its first commit.

For a Polaris catalog, add the OAuth2 client id/secret as well; those are also what let the
session renew its token and re-authenticate if a renewal fails — see
[Iceberg Catalog Sessions](iceberg-catalog-session.md).

The same form is available over REST:

```bash
curl -X POST http://broker:8080/admin/connections \
  -H "Authorization: Bearer $JWT" -H 'Content-Type: application/json' \
  -d '{"connectionId":"prod-iceberg","type":"ICEBERG","properties":{
        "catalog.rest.uri":"http://iceberg-rest:8181","catalog.rest.flavor":"rest",
        "catalog.warehouse":"s3://warehouse/","catalog.type":"rest",
        "s3.endpoint":"http://minio:9000","s3.region":"us-east-1",
        "s3.accessKey":"...","s3.secretKey":"...","s3.pathStyle":"true"}}'
```

---

## Step 2 — Create the target namespace

The streaming sink creates the **table** automatically (inferring the schema from the first
batch), but the Iceberg **namespace** must exist. Create it once via your catalog, e.g. for a
REST catalog:

```bash
curl -X POST http://iceberg-rest:8181/v1/namespaces \
  -H 'Content-Type: application/json' -d '{"namespace":["analytics"]}'
```

---

## Step 3 — Create the streaming job

Open **Streaming → New Job** and fill in:

| Field | Example | Meaning |
|---|---|---|
| Job Name | `events-to-iceberg` | display name |
| Source Topic | `events` | topic to consume |
| Parallelism | `4` | consumer threads in the job's group, spread across brokers |
| Format | `json` | or `avro` (uses the configured Schema Registry) |
| Sink Type | `Iceberg` | |
| Iceberg Connection | `prod-iceberg` | the connection from Step 1 |
| Target Table | `analytics.events` | `namespace.table` |
| Upsert Keys | `id` | *(optional)* idempotent upsert by key |
| Commit / Checkpoint Interval | `5000` | exactly-once cadence (ms) |
| Filter / Select | *(optional)* | simple transforms |

Click **Create Job**. Within a few seconds the controller assigns the job's consumer threads to
the worker brokers and they join the consumer group `stream-<jobId>`.

---

## Step 4 — Produce and verify

Produce JSON to the topic with any Kafka client:

```bash
echo '{"id":1,"name":"alice","amount":120,"event_type":"purchase"}' \
  | kafka-console-producer.sh --bootstrap-server broker:9092 --topic events
```

After one commit interval the rows are committed to the Iceberg table as a new snapshot. Query it
with any Iceberg engine (Spark, Trino, PyIceberg), or inspect the latest snapshot via the REST
catalog (`total-records` in the snapshot summary).

The job's live status (per-broker thread counts, last completed checkpoint) is available at
`GET /admin/streaming/jobs/{id}` and on the Streaming page.

---

## How it behaves

- **Parallelism = consumer-group size.** With parallelism N, N consumer threads share the topic
  partitions. Set N ≤ the partition count to keep every thread busy; ItdaStream's group
  coordinator rebalances automatically when brokers join or leave. For an **append-only** table the
  brokers do not each commit — the controller does one commit per checkpoint aggregating every
  broker's files ([single-committer](streaming-operations.md#single-committer-append-only-iceberg)),
  so parallelism > 1 never causes Iceberg commit conflicts.
- **Format.** `json` infers columns from the message fields; `avro` decodes via the Schema
  Registry. For a fixed target table, keep the message schema stable.
- **Append vs upsert.** Without upsert keys the sink **appends**. With upsert keys, each commit
  writes the data plus an equality-delete on those keys, so re-sent or updated rows collapse to
  the latest value (idempotent) — ideal for change streams.
- **Exactly-once.** The Iceberg commit and the Kafka offset commit are aligned at each checkpoint,
  so a broker failure never loses or double-writes rows. See the
  [Overview](streaming.md#exactly-once) for the mechanism.

---

## Automatic time partitioning (`_ingest_ts`)

Raw Kafka messages rarely carry a clean event-time column, yet an Iceberg table with **no
partitioning** quickly degrades: every query scans every file. To keep the no-code path fast out
of the box, the auto-sink **synthesises an ingestion-time column and hidden-partitions the table
by it** — you get time-pruned scans without writing a schema or a partition spec.

When a no-code Iceberg job **auto-creates** its table, ItdaStream:

1. Adds a synthetic column **`_ingest_ts`** — an Iceberg `timestamptz` (microsecond, UTC) stamped
   with the ingestion time, one value per micro-batch. The name is chosen not to collide with your
   own fields, and the column is **additive** — all your message fields are written unchanged.
2. Creates the table **hidden-partitioned by `hour(_ingest_ts)`**. Iceberg stores the partition
   value (the hour bucket) without adding a visible column; the hidden partition field is named
   `_ingest_ts_hour`.

So a stream of `{"id":1,"name":"alice","amount":120}` lands as a table with columns
`id, name, amount, _ingest_ts`, transparently partitioned by the hour. A reader can prune by time:

```sql
-- Trino: only the matching hour-partitions are scanned
SELECT count(*) FROM analytics.events
WHERE _ingest_ts >= TIMESTAMP '2026-06-13 12:00:00 UTC'
  AND _ingest_ts <  TIMESTAMP '2026-06-13 13:00:00 UTC';

-- inspect the hidden partitions
SELECT partition, record_count FROM analytics."events$partitions";
-- → {_ingest_ts_hour=494820}  30
```

!!! note "No-code path only — custom jobs are never touched"
    This injection happens **only** on the no-code auto-sink path (a pure topic→Iceberg job with no
    `MAP`/`FLATMAP` user code). A [Streaming SDK](streaming-sdk.md) job with a custom transform
    follows **your** code exactly — its schema and partitioning are whatever your job and target
    table define. The injection is also skipped for **upsert** sinks, which require an
    unpartitioned target.

### Tuning it

Three broker-level properties control the behaviour (per-job spec values override them). Defaults
suit most pipelines; change them in `conf/itdastream.properties`:

| Property | Default | Meaning |
|---|---|---|
| `itdastream.streaming.autosink.ingest.ts.enabled` | `true` | Inject the column + hidden partition. Set `false` to write data columns as-is, unpartitioned. |
| `itdastream.streaming.autosink.ingest.ts.column` | `_ingest_ts` | Name of the synthetic ingestion-time column. |
| `itdastream.streaming.autosink.ingest.ts.partition` | `hour` | Partition transform — one of `hour`, `day`, `month`, `year`. The hidden field is named `<column>_<transform>`. |

For example, set `...partition=day` for a high-retention table where hourly partitions would be too
fine, or `...enabled=false` if a downstream job re-partitions the data itself.

---

## Other no-code sinks

The same flow works for the other sink types — pick the sink and a matching connection:

| Sink | Connection type | Target field |
|---|---|---|
| Kafka | KAFKA | target topic |
| JDBC | JDBC | table name |
| Elasticsearch | ELASTICSEARCH | index |
| NeorunBase | NEORUNBASE | table name |
| HTTP | *(none)* | webhook URL |
| Console | *(none)* | — (debug) |

Only Iceberg and JDBC are transactional (exactly-once); the others are at-least-once.
