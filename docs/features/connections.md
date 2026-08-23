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

    An Iceberg connection describes **two** things: how to reach the REST catalog, and how to
    reach the object store holding the data. Both halves are required — ItdaStream reads and
    writes S3 directly with the connection's own keys rather than with credentials the catalog
    vends (see [Iceberg Catalog Sessions](iceberg-catalog-session.md)).

    **Catalog, all flavors**

    | Key | Example | Notes |
    |---|---|---|
    | `catalog.rest.uri` | `http://polaris:8181/api/catalog` | REST catalog endpoint |
    | `catalog.rest.flavor` | `polaris` (default), `rest`, `glue` | Selects the auth model below |
    | `catalog.warehouse` | `regdemo_catalog` or `s3://warehouse/` | Polaris: the **catalog name**; vanilla REST: the warehouse path |
    | `catalog.name` | `prod` | Catalog name; defaults to the warehouse value |
    | `catalog.type` | `rest` | **Set this.** See the warning below |
    | `catalog.rest.auth` | `sigv4` | Only `sigv4` is meaningful — it forces Glue/SigV4 signing on any REST endpoint. Any other value (including `none`/`oauth2`) leaves the flavor to decide |
    | `catalog.rest.prefix` | `prod` | URL prefix. Defaults to the catalog name for `polaris`, unset for `rest` |

    **Polaris / OAuth2 flavor**

    | Key | Example |
    |---|---|
    | `catalog.rest.client_id` | `root` |
    | `catalog.rest.client_secret` | … (masked) |
    | `catalog.rest.scope` | `PRINCIPAL_ROLE:ALL` (default) |
    | `catalog.rest.oauth2.server.uri` | `http://polaris:8181/api/catalog/v1/oauth/tokens` |
    | `catalog.rest.token` | a pre-issued token *instead of* id/secret — cannot be renewed, see below |
    | `catalog.rest.oauth2.token-refresh-enabled` / `catalog.rest.oauth2.token-exchange-enabled` | per-connection override of the [token-renewal switches](iceberg-catalog-session.md#how-the-token-is-renewed) |

    **AWS Glue flavor** (`catalog.rest.flavor=glue`, or `catalog.rest.auth=sigv4`)

    | Key | Example |
    |---|---|
    | `catalog.rest.signing.name` | `glue` (default) |
    | `catalog.rest.signing.region` | `us-east-1` — falls back to `s3.region` |
    | `catalog.rest.aws.accessKey` / `.secretKey` / `.sessionToken` | optional; fall back to the `s3.*` keys, then to the AWS provider chain (IAM role / env / SSO) |

    `catalog.warehouse` for Glue is the AWS account id, or
    `<account-id>:s3tablescatalog/<bucket>` for S3 Tables. No `prefix` is sent — the endpoint
    derives the catalog path from `warehouse` in its `GET /v1/config` response.

    **Object store (all flavors)**

    | Key | Example |
    |---|---|
    | `s3.endpoint` | `http://minio:9000` |
    | `s3.region` | `us-east-1` |
    | `s3.accessKey` | … |
    | `s3.secretKey` | … (masked) |
    | `s3.pathStyle` | `true` |

    Bare (`accessKey`, `secretKey`, `endpoint`, `region`, `pathStyle`) and `s3.`-prefixed
    spellings are interchangeable: each missing spelling is filled in from its counterpart, so
    an S3 connection reused by id (which stores the bare keys) and an inline `s3.*` block behave
    the same.

    !!! warning "REST catalog flavor"
        For a vanilla Iceberg REST catalog (the `iceberg-rest` image, Nessie, …) set
        `catalog.rest.flavor=rest`. The default `polaris` flavor addresses catalogs by a URL
        prefix (`/v1/{prefix}/namespaces/...`); a vanilla catalog serves `/v1/namespaces/...`
        and a forced prefix yields *"No route for request"*.

    !!! warning "Set `catalog.type=rest` on connections used as a sink"
        On the **write** path the direct S3 FileIO that bypasses catalog credential vending is
        only installed when the connection carries `catalog.type=rest` (the internal default is
        the legacy `hadoop`). Without it, commits fall back to whatever FileIO the catalog
        returns — which under Polaris means vended credentials pointing at the catalog's own
        view of the S3 endpoint, typically unreachable from the broker. The **read** path
        installs it whenever `s3.accessKey` is present, so a connection missing this key can
        read perfectly well and still fail on the first commit.

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
      "catalog.type":"rest",
      "catalog.warehouse":"s3://warehouse/",
      "s3.endpoint":"http://minio:9000","s3.region":"us-east-1",
      "s3.accessKey":"...","s3.secretKey":"...","s3.pathStyle":"true"
    }}'
```

A Polaris connection, where the catalog name is both the warehouse and the URL prefix:

```bash
curl -X POST http://broker:8080/admin/connections \
  -H "Authorization: Bearer $JWT" -H 'Content-Type: application/json' \
  -d '{
    "connectionId":"prod-polaris","type":"ICEBERG",
    "properties":{
      "catalog.rest.uri":"http://polaris:8181/api/catalog",
      "catalog.rest.flavor":"polaris",
      "catalog.type":"rest",
      "catalog.warehouse":"prod_catalog",
      "catalog.rest.client_id":"...","catalog.rest.client_secret":"...",
      "catalog.rest.scope":"PRINCIPAL_ROLE:ALL",
      "catalog.rest.oauth2.server.uri":"http://polaris:8181/api/catalog/v1/oauth/tokens",
      "s3.endpoint":"http://minio:9000","s3.region":"us-east-1",
      "s3.accessKey":"...","s3.secretKey":"...","s3.pathStyle":"true"
    }}'
```

The client id and secret are what lets the session renew its token — and re-authenticate if a
renewal ever fails. See [Iceberg Catalog Sessions and OAuth2 Token Renewal](iceberg-catalog-session.md).
