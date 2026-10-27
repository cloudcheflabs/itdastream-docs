# Admin UI Guide

The Admin UI (console) is served by **every broker** on its admin port (`itdastream.admin.ui.port`, default `8080`) under `/admin/`. All brokers serve the same cluster: open the console on any of them. Writes that must happen on the leader (IAM, KMS, SSO, connections, backup, topic changes) are forwarded to the leader automatically, and the broker you are on catches up with the leader before it answers, so the page you are looking at shows your change immediately.

This guide walks through every page and component. The section headings match the sidebar.

!!! note "Roles"
    What you can do depends on your IAM policies, not on your user name. A user is an **administrator** when one of their groups carries the `AdministratorAccess` policy (for example `admin-group`). The header shows an **Administrator** or **User** badge. Non-administrators see the pages read-only: buttons that change the cluster are hidden, operators-only pages say **Access Restricted**, and the server refuses the corresponding API calls with `403` regardless of what the console shows (see [Admin REST API](../features/admin-rest-api.md)).

---

## Sign in

- **Username / Password** — a local IAM user. A wrong password shows *Invalid credentials*.
- **First sign-in** — a new account (including the bootstrap `admin` / `admin`) must change its initial password before it gets a session. The **Password Change Required** dialog asks for the current password and the new one twice (minimum 6 characters, must differ from the current one). After the change you are returned to the sign-in form; sign in with the new password. Until the change, the server issues no token — not even the user's API tokens work.
- **Single sign-on** — when OIDC or SAML is configured on the [Single Sign-On](#single-sign-on) page, *Sign in with OpenID Connect* / *Sign in with SAML* buttons appear. With LDAP / Active Directory configured, directory users sign in with the same form.
- **Deep links** — a bookmarked or reloaded page (for example `/admin/topics`) opens the sign-in form first if needed, then the page.
- **Sign out** (icon at the top right) ends the session **on the server**: the token stops working on every broker immediately, so a copied token cannot be reused.

The login page footer and the sidebar show the ItdaStream version the console was built for.

---

## Dashboard

- **Active Brokers** — the number of registered brokers.
- **CPU Usage (%)**, **Memory Usage (MB)** (heap used vs. heap max), **Throughput (TPS)**, **Network In/Out (pkt/s)** — the last 30 minutes, one series per broker. The leader collects metrics from every broker, so the charts include all of them whichever broker serves the console. The page refreshes at `itdastream.admin.ui.refresh.interval.ms`.

---

## Topics

| Component | What it does |
|---|---|
| **Refresh** | Reloads the topic list and sizes. |
| **New Topic** | Side panel with *Topic Name* and *Number of Partitions*. Names follow Kafka's rule (letters, digits, `.`, `_`, `-`, up to 249 characters); an existing name is refused with *already exists*. |
| Topic table | Name, partitions, **Data Size** (bytes flushed to object storage), replication and retention (cluster defaults). |
| Click a topic name | Opens the **Partition Breakdown** panel below the table: one card per partition with its stored size. The ✕ closes it. |
| **Manage Partitions** (pencil) | Side panel to **increase** the partition count. *Continue to Confirmation* opens a confirmation; *Cancel* changes nothing, *Confirm Update* applies it. Decreasing is refused. Kafka clients see the new partitions at their next metadata refresh. |
| **Delete Topic** (bin) | Confirmation dialog; **Purge Topic** removes the topic's metadata and its segments from object storage. |

---

## Brokers (Cluster Topology)

- **Header** — *N of M Nodes Serving*. A yellow banner appears while a broker drains.
- **Broker table** — id, host / IP, Kafka port, admin port, status (*Serving* / *Draining*).
- **Drain / Resume** (administrators) — opens a confirmation explaining the effect. A draining broker is left out of the metadata clients receive and gives up partition leadership; requests already on their way are still answered. **Resume** puts it back. The last serving broker cannot be drained (button disabled). When a broker resumes, it catches up with whatever the other brokers wrote to its partitions meanwhile. See [Broker Drain](../features/broker-drain.md).
- **Log View** (administrators) — a live tail of that broker's log, fetched through the broker you are on. *LIVE STREAM / PAUSED* toggles polling, the refresh icon reloads from the start, the footer shows buffered lines, the offset and whether auto-scroll is on (scroll up to pause it).
- **ZooKeeper Ensemble** — the connect string and chroot, *N of M Servers Up* with quorum, and per server: role (leader / follower / observer / standalone), status (a server that does not answer shows *Down* with the reason), version, connections, latency, zxid and znode count. The footer shows this broker's own ZooKeeper session (state, server, session id, timeout).

---

## Log Storage

Operators only (`kafka:BrowseStorage`). A read-only browser over the bucket holding the topic log.

- **Buckets** (left) — click the storage bucket to open it.
- **Objects** (right) — `data/` (segments) and `index/` (offset indexes), then topic → partition folders. Files show their size and modification time.
- **Breadcrumb** — click any segment (or the bucket name) to jump to that level; **..** goes up one level; the refresh icon reloads the current level.

---

## Connections

Named connections that streaming jobs reference by **connection id**, so job definitions never carry credentials. See [Connection Registry](../features/connections.md).

- **New Connection** (administrators) — *Connection ID*, *Type* (`S3`, `ICEBERG`, `KAFKA`, `ELASTICSEARCH`, `JDBC`, `NEORUNBASE`), *Description* and the type's properties.
    - For **ICEBERG**, *Catalog Flavor* selects the fields: **Apache Polaris (OAuth2)** shows OAuth client id / secret / scope / token URI; **AWS Glue (SigV4)** shows signing region, service and optional AWS keys; **Generic REST** shows no auth fields. The S3 fields are always the static keys used to read and write data — ItdaStream never uses catalog credential vending, so create Polaris catalogs with `stsUnavailable: true`.
    - A connection id that already exists is refused.
- **Cards** — id, type badge, description and the first properties. Secret values (keys, passwords, tokens) are shown as `***` and are never returned by the API.
- **Edit** (pencil) — id and type are fixed; secret fields show as password fields holding `***`. Leaving a secret at `***` keeps the stored value; type a new value to replace it.
- **Delete** (bin) — after a browser confirmation.

---

## Streaming Jobs

No-code Kafka pipelines: a source topic, optional transforms and a sink. Parallelism is the number of consumers in the job's group; they are spread across the brokers and share the topic's partitions.

### New Job (administrators)

| Field | Meaning |
|---|---|
| Job Name | Display name (defaults to the topic). |
| Source Topic, Parallelism, Format | Topic to consume, consumer count, `json` or `avro`. |
| Sink Type | **Iceberg**, Kafka, JDBC, Elasticsearch, NeorunBase, HTTP Webhook, Console. Sinks that need credentials ask for a connection of the matching type. |
| Iceberg: Target Table | `namespace.table`; the table is created from the JSON shape when missing. |
| Iceberg: Upsert Keys | Optional; makes the sink an idempotent upsert (equality deletes). |
| Iceberg: WAP branch | Optional; stages every commit on this branch instead of `main` ([Write-Audit-Publish](../features/iceberg-wap.md)). |
| Kafka: Target Topic, Key Field | Destination topic and optional record key field. |
| JDBC / NeorunBase: Table Name; Elasticsearch: Index, ID Field; HTTP: Webhook URL | Sink-specific targets. |
| Commit / Checkpoint Interval (ms) | How often offsets and sink data are committed together. |
| Filter, Select | Optional transforms: a predicate such as `amount > 100`, and a comma-separated field list. |

### Active Jobs

Each card shows topic, parallelism, format, status, sink, target and connection.

- **View live logs** (terminal icon) — opens the job log viewer: the merged log of every broker running the job, refreshed every 2 seconds, with a **LIVE** badge, a line count, *auto-scroll*, **Download** (saves `<job name>.log`) and **Close**.
- **Kill job** (bin, administrators) — after a confirmation, stops and deletes the job. The call returns once every broker has stopped the job's threads (bounded by `itdastream.admin.streaming.kill.wait.ms`), so nothing produced after the kill is processed. Records not yet committed since the last checkpoint are not written. The job's log history is kept.

### Log History

Every job's logs are flushed to object storage, so the history lists running and ended jobs alike: job id, *running* / *ended*, the brokers that ran it, size and time. The terminal icon opens the same log viewer (without the LIVE badge for ended jobs).

---

## Iceberg Sinks

Configure many **no-code auto Iceberg sinks** at once: each topic → table mapping becomes an ordinary streaming job with a table sink and no MAP/FLATMAP transform. The engine adds an `_ingest_ts` column and hidden-partitions the table by it. See [No-Code Kafka to Iceberg](../features/streaming-no-code.md).

- **Table** — every auto Iceberg sink (including table-sink jobs created on the Streaming Jobs page): topic → table, format, partition (`hour(_ingest_ts)`, `day(...)`, ..., `none`, or *upsert (unpartitioned)*), connection, **View live logs**, **Kill sink**.
- **Add Iceberg Sinks** (administrators):
    - Shared settings: *Iceberg Connection*, *Time Partition* (`hour`, `day`, `month`, `year`, or `off`), *Parallelism*, *Commit / Checkpoint (ms)*, optional *WAP branch*.
    - *Topic → Table mappings*: choose a topic (the table name is pre-filled as `default.<topic>` — change the namespace to an existing one), optional upsert keys. **Add row** adds a mapping, the bin removes one.
    - The submit button counts the complete rows (*Create N sink(s)*) and reports any row that failed.

---

## Identity Control

Users, groups and policies in the AWS IAM style. See [AWS IAM-Compatible Access Control](../features/iam.md) and the [IAM Policy Reference](../features/iam-policy.md).

### Users

- **New User** — *User Identifier* (letters, digits, `. _ @ + -`) and an initial password (min. 6 characters). The user must change it at first sign-in. An existing id (including `admin`) is refused — it is never overwritten.
- **Join** — adds the user to a group.
- **Generate Key** — optional expiration date, then a one-time reveal of the **Access Key**, **Secret Key** and **User Token** (`ITOK...`, for SDK submission and the `Authorization: Token` scheme), each with **Copy**. Afterwards only the access key is listed (secret masked), with its expiry.
- **Download credentials CSV** (download icon) — access key, secret key and user token. A user can download their own keys; downloading another user's key needs `kafka:CreateKey`.
- **Revoke key** (✕) — the key and its token stop working immediately.
- **Delete User** — not offered for `admin` or for yourself. Deleting a user also invalidates their tokens and sessions on every broker.

### Groups

- **New Group** — an existing name (such as `admin-group`) is refused, so a group's policies can never be wiped by re-creating it.
- Click a group to expand it: **Affiliated Principals** (hover a member and click ✕ to remove it) and **Policy Compliance** (✕ detaches a policy; **Attach Custom** attaches one).
- **Delete Group** (bin) — also removes the group from its members.

### Policies

- **New Policy** — a name and an IAM JSON document; *Governance Template* fills in a starting document (AdministratorAccess, FullAccess, ReadOnly, ProducerAccess, ConsumerAccess, topic-specific producer/consumer). Invalid JSON or a document without `Statement` is refused with the reason.
- **Edit** (hover the card) — the name is fixed; *Apply Governance Changes* saves the document.
- **Delete** (hover the card) — not available for `AdministratorAccess`.

---

## Single Sign-On

Configure OIDC, SAML 2.0 and LDAP / Active Directory without a restart; every broker picks the settings up from the replicated IAM snapshot. See [Single Sign-On](../features/sso.md). Changing SSO needs `kafka:ManageSso` (administrators); others see the page read-only.

- **Tabs** — *OpenID Connect*, *SAML 2.0*, *LDAP / AD*, *Identity mapping*; a green dot marks an enabled provider.
- **Fields** — every value is a named field. Secret fields (OIDC client secret, LDAP bind password, SAML SP private key) show a **set / not set** badge and are never sent back to the browser; leave them empty to keep the stored value.
- **Save (N)** — counts the changed fields; nothing is saved until you click it.
- **Test the connection / Check the configuration / Test the bind** — asks the server to try the provider and shows the result or the provider's error.
- **SAML: This cluster's SP metadata** — downloads `itdastream-sp-metadata.xml` for your identity provider. **Generate an SP keypair** creates the signing key. Paste the provider's metadata XML and click **Read the provider's metadata** to fill the IdP entity id, sign-on URL and signing certificate.
- **Identity mapping** — map provider groups to ItdaStream groups (`ops=admin-group`), whether unmapped identities may sign in, and the federated session length.

---

## Security & KMS

Administrators only (`kafka:ListKms` to view).

- **Create New Master Key** — type a key id, **Generate Key**, confirm. The key is created on the leader and replicated to every broker.
- **Key table** — id, current version, last rotation. **Manage Key** opens the detail.
- **Key detail** — total versions, last rotation, the **Version Lifecycle** (the newest version is `ACTIVE` for encrypt + decrypt; older ones are `RETIRED`, decrypt only) and the configuration snippet. **Rotate Key Now** (with confirmation) creates a new version. **Back to List** returns.
- **Change Admin Password** — the *Update Password* dialog changes the signed-in user's password (current password required).

---

## Backup & Restore

Operators only (`kafka:ManageBackup`). Snapshots the KMS keystore and IAM state to S3, the IAM state KMS-encrypted (a backup is refused while the KMS is not initialized). See [Backup & Restore](../features/backup.md).

- **Configuration** — *Enable backup feature*, S3 endpoint / region / bucket / prefix / access key / secret key, interval (minutes), retention (days), cron (5-field, optional; an invalid expression is refused) and path-style addressing (required for ShannonStore / MinIO). The secret is stored but never shown again: the field reads *(unchanged — paste to overwrite)*. The configuration is stored cluster-wide, encrypted, so every broker shows it and a new leader keeps it.
- **Save** stores the configuration; **Backup Now** (enabled only when backups are enabled) runs a backup on the leader.
- **Available Backups** — backup ids in the bucket, newest first. **Restore** (with confirmation) replaces the KMS and IAM state on every broker with that snapshot. The current backup configuration is kept.
- **Recent Runs** — status (`OK` / `ERROR` / `RUNNING`), trigger (`manual`, `scheduled`, `cron`), start time, uploaded / reused stores and bytes. A failed run shows its error message under the status.
