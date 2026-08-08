# Schema Registry

ItdaStream ships with a small **built-in schema registry** exposed through the admin API. It
stores versioned schemas (Avro by default) under a subject name and hands each registered schema a
cluster-unique integer **schema id**. The streaming engine uses it to decode Avro records on the
[no-code](streaming-no-code.md) and [SDK](streaming-sdk.md) Kafka → Iceberg paths, and any
application can register and look up schemas over REST.

There is no separate service to deploy — the registry is part of the broker's admin HTTP server
(default port `8080`), and its data is shared across the whole cluster.

## How it is stored

Registration is coordinated through ZooKeeper so every broker sees the same schemas:

- A single monotonic **schema-id counter** lives at `<root>/schema-counter` in ZooKeeper, so every
  registered schema gets a globally unique id across the cluster.
- Each subject's versions are persisted under `<root>/schemas/<subject>/<version>` and an id→schema
  index under `<root>/schema-ids/<id>`, and mirrored into the broker's local metadata RocksDB store
  for fast lookup.
- Registering the same subject again creates the **next version** (`version = current versions + 1`);
  ids are never reused.

*(Source: `RocksDbMetadataStore.registerSchema` in `itdastream-metadata`.)*

## REST API

All endpoints live under `/admin/schemas` on the admin HTTP server and require the standard admin
auth (`Authorization: Bearer <JWT>` or `Authorization: Token <ITOK…>` — see
[IAM](iam.md)).

| Method | Path | Purpose |
|---|---|---|
| `GET`  | `/admin/schemas/subjects` | List all registered subject names. |
| `POST` | `/admin/schemas/subjects/{subject}` (also accepts the `/versions` suffix) | Register a new schema version for the subject. Body `{"schema": "<schema text>", "schemaType": "AVRO"}` (`schemaType` defaults to `AVRO`). Returns `{"id": <int>}`. |
| `GET`  | `/admin/schemas/subjects/{subject}/latest` | Get the latest `SchemaInfo` for the subject. `404` if the subject is unknown. |
| `GET`  | `/admin/schemas/ids/{id}` | Get the `SchemaInfo` for a schema id. `404` if the id is unknown. |

A `SchemaInfo` object has the shape:

```json
{ "subject": "events-value", "version": 3, "id": 42, "schema": "{\"type\":\"record\", ...}", "schemaType": "AVRO" }
```

### Example

```bash
# Register an Avro schema for the "events-value" subject
curl -s -X POST http://localhost:8080/admin/schemas/subjects/events-value \
  -H "Authorization: Bearer $JWT" -H 'Content-Type: application/json' \
  -d '{"schema":"{\"type\":\"record\",\"name\":\"Event\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"}]}","schemaType":"AVRO"}'
# → {"id":42}

# List subjects, fetch latest, fetch by id
curl -s http://localhost:8080/admin/schemas/subjects            -H "Authorization: Bearer $JWT"
curl -s http://localhost:8080/admin/schemas/subjects/events-value/latest -H "Authorization: Bearer $JWT"
curl -s http://localhost:8080/admin/schemas/ids/42              -H "Authorization: Bearer $JWT"
```

## Using it with Avro streaming jobs

When a streaming source is configured with `format=avro`, the executor decodes each Kafka record
using the **Confluent-style wire format** (a magic byte followed by a 4-byte schema id), resolving
the id against a schema registry. Point the source at the registry with the `schemaRegistryUrl`
field on the source node of the job spec (or `Source.kafka(topic).format("avro").property(...)` in
the SDK). The streaming Avro client fetches a schema with `GET {schemaRegistryUrl}/schemas/ids/{id}`,
so `schemaRegistryUrl` must be the base that serves that path — for the built-in registry that is
`http://<broker>:8080/admin`.

```json
{
  "name": "avro-events-to-iceberg",
  "parallelism": 4,
  "kafka": {
    "topic": "events",
    "format": "avro",
    "schemaRegistryUrl": "http://broker:8080/admin"
  },
  "sink": { "type": "table", "connectionId": "prod-iceberg", "table": "analytics.events" },
  "commitIntervalMs": 5000, "checkpointIntervalMs": 5000
}
```

*(Source: `SchemaRegistryClient` and `KafkaStreamSource` in `itdastream-streaming`.)*
