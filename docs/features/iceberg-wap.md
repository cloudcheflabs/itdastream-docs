# Iceberg Write-Audit-Publish (WAP)

**Write-Audit-Publish** is a data-quality pattern for Iceberg tables: instead of writing streamed
records straight to the table's `main` branch, the job stages them on an isolated **audit branch**.
Downstream readers — querying `main` — see nothing while the data is being staged. You then **audit**
the staged snapshot (row counts, schema, business checks) and, only when it passes, **publish** it
onto `main` in a single atomic ref move.

```
Kafka ──stream──▶  Iceberg `audit` branch     (Write)
                          │
                          ▼   inspect via REST catalog
                       Audit                  (validate the staged snapshot)
                          │
                          ▼   POST /admin/iceberg/publish (fast_forward)
                   Iceberg `main` branch       (Publish — now visible to readers)
```

Because the audit branch and `main` are separate Iceberg refs over the same table, audit isolation
is guaranteed: a reader on `main` never observes an unaudited snapshot.

---

## Enabling WAP — the `itdastream.iceberg.wap.branch` sink key

Add the sink property `itdastream.iceberg.wap.branch` to any Kafka → Iceberg streaming job. Its value
is the branch name the sink commits to (commonly `audit` or `staging`). All commits the job makes go
to that branch; `main` is untouched until you publish.

```json
{
  "name": "wap-demo",
  "parallelism": 2,
  "kafka": { "topic": "events", "format": "json" },
  "sink": {
    "type": "table",
    "connectionId": "ice",
    "table": "db.events",
    "itdastream.iceberg.wap.branch": "audit"
  },
  "commitIntervalMs": 5000,
  "checkpointIntervalMs": 5000
}
```

Submit it like any other streaming job:

```bash
curl -X POST http://broker:8082/admin/streaming/jobs \
  -H "Authorization: Bearer $JWT" -H 'Content-Type: application/json' \
  -d @wap-demo.json
```

> Without the WAP key, the same job writes directly to `main` (the standard
> [No-Code Kafka → Iceberg](streaming-no-code.md) behaviour). WAP is purely additive: set the
> branch and the write path becomes audited.

---

## Auditing the branch via the Iceberg REST catalog

While the job runs, query the table through the Iceberg REST catalog to watch the audit branch fill
up while `main` stays empty:

```bash
curl -s http://localhost:8181/v1/namespaces/db/tables/events | jq '{
  refs: .metadata.refs,
  current: .metadata."current-snapshot-id"
}'
```

- `.metadata.refs.audit."snapshot-id"` is the snapshot the audit branch points at.
- `.metadata.refs.main` (or `.metadata."current-snapshot-id"`) is what readers on `main` see.
- The row count for any snapshot is its summary's `total-records`:

```bash
META=$(curl -s http://localhost:8181/v1/namespaces/db/tables/events)
AUDIT_SID=$(echo "$META" | jq -r '.metadata.refs.audit."snapshot-id"')
echo "$META" | jq -r --arg s "$AUDIT_SID" \
  '.metadata.snapshots[] | select((."snapshot-id"|tostring)==$s) | .summary."total-records"'
```

This is your audit gate — run whatever validation you need against the audited snapshot before
publishing.

---

## Publishing — `POST /admin/iceberg/publish`

Once the audit branch passes, publish it onto `main`. The endpoint supports two operations.

### fast_forward

Move `main` up to the tip of the audit branch (the common case — append-only staging).

```bash
curl -X POST http://broker:8082/admin/iceberg/publish \
  -H "Authorization: Bearer $JWT" -H 'Content-Type: application/json' \
  -d '{
    "operation":"fast_forward",
    "schema":"db",
    "table":"events",
    "branch":"main",
    "to":"audit",
    "connectionId":"ice"
  }'
# -> {"status":"ok","result":"..."}
```

### cherrypick

Apply a single audited snapshot onto `main` (useful when you want one specific snapshot rather than
the whole branch tip):

```bash
curl -X POST http://broker:8082/admin/iceberg/publish \
  -H "Authorization: Bearer $JWT" -H 'Content-Type: application/json' \
  -d '{
    "operation":"cherrypick",
    "schema":"db",
    "table":"events",
    "branch":"main",
    "snapshotId": 1234567890123456789,
    "connectionId":"ice"
  }'
```

| Field | fast_forward | cherrypick | Meaning |
|---|---|---|---|
| `operation` | `fast_forward` | `cherrypick` | publish strategy |
| `schema` | ✓ | ✓ | Iceberg namespace |
| `table` | ✓ | ✓ | table name |
| `branch` | ✓ | ✓ | target branch (usually `main`) |
| `to` | ✓ | — | source branch to fast-forward from (e.g. `audit`) |
| `snapshotId` | — | ✓ | the snapshot to cherry-pick |
| `connectionId` | ✓ | ✓ | the registered ICEBERG connection |

After publishing, re-query the REST catalog — `main` now points at the audited snapshot and the rows
are visible to readers.

---

## Examples

### Java (SDK)

[`IcebergWapStreamingExample.java`](https://github.com/cloudcheflabs/itdastream/blob/master/itdastream-sdk/src/main/java/com/cloudcheflabs/itdastream/sdk/examples/IcebergWapStreamingExample.java)
submits the job with the WAP branch set on the sink and then issues the `fast_forward` publish:

```java
String jobId = session.streamSource(Source.kafka("events").format("json"))
        .sink(Sink.iceberg("ice", "db.events")
                .property("itdastream.iceberg.wap.branch", "audit"))
        .name("wap-demo")
        .parallelism(2)
        .commitInterval(5_000)
        .start();
// ... audit the branch, then POST /admin/iceberg/publish (fast_forward main <- audit)
```

Run it:

```bash
java -cp itdastream-sdk-*.jar:jackson-databind-*.jar:jackson-core-*.jar:jackson-annotations-*.jar \
     com.cloudcheflabs.itdastream.sdk.examples.IcebergWapStreamingExample \
     http://localhost:8082 ITOK...usertoken...
```

### Python (`requests`)

Drive the same loop over the admin REST API — create the job with the WAP branch on the sink,
audit the `audit` branch via the Iceberg REST catalog (it accrues records while `main` stays
empty), then publish:

```python
import requests

ADMIN_URL  = "http://localhost:8082"   # broker admin API
REST_URL   = "http://localhost:8181"   # Iceberg REST catalog
WAP_BRANCH = "audit"
headers    = {"Authorization": "Bearer " + token, "Content-Type": "application/json"}

# 1. WRITE — create the streaming job; the sink stages every record on the 'audit' branch
spec = {
    "name": "wap-demo", "parallelism": 2,
    "kafka": {"topic": "events", "format": "json"},
    "sink": {
        "type": "table", "connectionId": "ice", "table": "db.events",
        "itdastream.iceberg.wap.branch": WAP_BRANCH,   # <- writes go to 'audit', not main
    },
    "commitIntervalMs": 5000, "checkpointIntervalMs": 5000,
}
job_id = requests.post(f"{ADMIN_URL}/admin/streaming/jobs", headers=headers, json=spec).json()["jobId"]

# 2. AUDIT — read table metadata from the Iceberg REST catalog: the 'audit' branch ref
#    accrues records while main's current snapshot stays empty.
meta = requests.get(f"{REST_URL}/v1/namespaces/db/tables/events").json()["metadata"]
audit_snap = meta["refs"][WAP_BRANCH]["snapshot-id"]
branch_records = next(int(s["summary"]["total-records"])
                      for s in meta["snapshots"] if s["snapshot-id"] == audit_snap)
print("audit branch records =", branch_records, " main =", meta.get("current-snapshot-id"))

# 3. PUBLISH — fast-forward main to the audited branch
resp = requests.post(f"{ADMIN_URL}/admin/iceberg/publish", headers=headers, json={
    "operation": "fast_forward", "schema": "db", "table": "events",
    "branch": "main", "to": WAP_BRANCH, "connectionId": "ice",
})
print("publish:", resp.json())   # {"status":"ok","result":"Fast-forwarded branch 'main' to 'audit' ..."}
```

The full runnable script (login, optional connection registration, record polling) is
[`examples/python/iceberg_wap_streaming_example.py`](https://github.com/cloudcheflabs/itdastream/blob/master/examples/python/iceberg_wap_streaming_example.py):

```bash
pip install requests
ADMIN_URL=http://localhost:8082 REST_URL=http://localhost:8181 \
  python examples/python/iceberg_wap_streaming_example.py
```
