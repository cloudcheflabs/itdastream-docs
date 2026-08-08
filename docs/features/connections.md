# Connection Registry

The connection registry stores external system credentials (S3, Iceberg catalogs, Kafka,
Elasticsearch, JDBC, NeorunBase) **once**, under a named `connectionId`. Streaming jobs and the
SDK reference a connection by id and never inline secrets — the same model as an Airflow
connection or a Kafka Connect config provider.

- **Per-broker, leader-authoritative**: the controller is the source of truth; credentials are
  stored KMS-encrypted in RocksDB and synced to every broker, so a job running on any worker can
  resolve them.
- **Encrypted at rest and in transit**: connection values are envelope-encrypted with the
  cluster KMS key (see [Envelope Encryption and KMS](kms.md)); the leader→follower sync ships a
  KMS-encrypted snapshot over the internal NIO channel.
- **Secrets never leave the cluster in clear**: list responses mask sensitive fields (anything
  whose key contains `secret`, `password`, `key`, or `token`) as `***`.

---

## How resolution works

A sink (or source) configuration carries a `connectionId`. At runtime the connector calls
`ConnectionStore.resolve(config)`:

1. the stored connection's properties become the base configuration;
2. any inline properties on the job override the stored ones;
3. the merged map is handed to the connector (e.g. the Iceberg sink builds its REST catalog from
   `catalog.rest.uri`, `catalog.warehouse`, `s3.*`).

If a referenced connection is missing on a worker, it is pulled from the controller on demand.

---

## Connection types and fields

Managed on the Admin UI **Connections** page, or via the REST API.

=== "S3"

    | Key | Example |
    |---|---|
    | `s3.endpoint` | `http://minio:9000` |
    | `s3.region` | `us-east-1` |
    | `s3.accessKey` | … |
    | `s3.secretKey` | … (masked) |
    | `s3.pathStyle` | `true` |

=== "Iceberg"

    | Key | Example |
    |---|---|
    | `catalog.rest.uri` | `http://iceberg-rest:8181` |
    | `catalog.rest.flavor` | `polaris` (default), `rest` (vanilla iceberg-rest / Nessie), or `glue` |
    | `catalog.warehouse` | `s3://warehouse/` |
    | `catalog.rest.auth` | `none` / `oauth2` / `sigv4` |
    | `s3.endpoint`, `s3.region`, `s3.accessKey`, `s3.secretKey`, `s3.pathStyle` | S3 FileIO credentials |

    !!! warning "REST catalog flavor"
        For a vanilla Iceberg REST catalog (the `iceberg-rest` image, Nessie, …) set
        `catalog.rest.flavor=rest`. The default `polaris` flavor addresses catalogs by a URL
        prefix (`/v1/{prefix}/namespaces/...`); a vanilla catalog serves `/v1/namespaces/...`
        and a forced prefix yields *"No route for request"*.

=== "Kafka"

    | Key | Example |
    |---|---|
    | `bootstrapServers` | `host1:9092,host2:9092` |

=== "JDBC"

    | Key | Example |
    |---|---|
    | `jdbcUrl` | `jdbc:postgresql://db:5432/mydb` |
    | `username` | … |
    | `password` | … (masked) |

=== "Elasticsearch"

    | Key | Example |
    |---|---|
    | `endpoint` | `http://es:9200` |

=== "NeorunBase"

    | Key | Example |
    |---|---|
    | `mode` | `rest` or `jdbc` |
    | `endpoint` / `jdbcUrl` | per mode |
    | `username`, `password` | … (password masked) |

---

## REST API

JWT or IAM user-token auth; mutations are forwarded to the controller.

- `GET /admin/connections` — list (secrets masked)
- `POST /admin/connections` — create `{connectionId, type, description, properties}`
- `PUT /admin/connections/{id}` — update (a field left as `***` keeps its stored value)
- `DELETE /admin/connections/{id}` — delete

```bash
curl -X POST http://broker:8080/admin/connections \
  -H "Authorization: Bearer $JWT" -H 'Content-Type: application/json' \
  -d '{
    "connectionId":"prod-iceberg","type":"ICEBERG",
    "properties":{
      "catalog.rest.uri":"http://iceberg-rest:8181",
      "catalog.rest.flavor":"rest",
      "catalog.warehouse":"s3://warehouse/",
      "catalog.rest.auth":"none",
      "s3.endpoint":"http://minio:9000","s3.region":"us-east-1",
      "s3.accessKey":"...","s3.secretKey":"...","s3.pathStyle":"true"
    }}'
```
