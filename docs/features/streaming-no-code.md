# No-Code Kafka → Iceberg

The most common pipeline — *"messages produced to a topic land in an Iceberg table"* — needs no
code at all. You configure it **once** in the Admin UI; from then on every message produced to the
topic flows into the table with exactly-once semantics. This page is a full walkthrough.

> The basic case is exactly this: produce to an ItdaStream topic, and it goes straight into
> Iceberg. For richer transforms or multiple sinks, use the [Streaming SDK](streaming-sdk.md).

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
| `catalog.warehouse` | `s3://warehouse/` | Iceberg warehouse |
| `catalog.rest.auth` | `none` | or `oauth2` / `sigv4` |
| `s3.endpoint` / `s3.region` | `http://minio:9000` / `us-east-1` | S3 FileIO |
| `s3.accessKey` / `s3.secretKey` | … | masked after save |
| `s3.pathStyle` | `true` | for MinIO and most S3-compatible stores |

!!! warning "Pick the right catalog flavor"
    A **vanilla** Iceberg REST catalog (the `iceberg-rest` image, Nessie) serves
    `/v1/namespaces/...` and needs `catalog.rest.flavor=rest`. The default `polaris` flavor
    addresses catalogs by a URL prefix (`/v1/{prefix}/namespaces/...`); using it against a
    vanilla catalog yields *"No route for request"*.

The same form is available over REST:

```bash
curl -X POST http://broker:8082/admin/connections \
  -H "Authorization: Bearer $JWT" -H 'Content-Type: application/json' \
  -d '{"connectionId":"prod-iceberg","type":"ICEBERG","properties":{
        "catalog.rest.uri":"http://iceberg-rest:8181","catalog.rest.flavor":"rest",
        "catalog.warehouse":"s3://warehouse/","catalog.rest.auth":"none",
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
  coordinator rebalances automatically when brokers join or leave.
- **Format.** `json` infers columns from the message fields; `avro` decodes via the Schema
  Registry. For a fixed target table, keep the message schema stable.
- **Append vs upsert.** Without upsert keys the sink **appends**. With upsert keys, each commit
  writes the data plus an equality-delete on those keys, so re-sent or updated rows collapse to
  the latest value (idempotent) — ideal for change streams.
- **Exactly-once.** The Iceberg commit and the Kafka offset commit are aligned at each checkpoint,
  so a broker failure never loses or double-writes rows. See the
  [Overview](streaming.md#exactly-once) for the mechanism.

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
