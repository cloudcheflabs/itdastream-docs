# Flexible Configuration System

ItdaStream provides a layered configuration system with multiple override
levels for deployment flexibility. This page is the complete reference for every
configuration property, organized to mirror the sections of the shipped
`itdastream.properties` file.

## Configuration Precedence

Every property is resolved using the following precedence, from **highest to
lowest**:

1. **System properties** &mdash; `-Ditdastream.*` JVM arguments
2. **Environment variables** &mdash; `ITDASTREAM_*`
3. **Properties file** &mdash; `itdastream.properties` (under `conf/`)
4. **Built-in defaults**

A higher level overrides a lower one for the same key. This lets you keep a
base `itdastream.properties` in version control, layer per-environment overrides
through environment variables (handy for containers), and apply last-minute or
per-process tweaks with `-D` JVM flags.

### Environment Variable Naming Convention

Any `itdastream.*` property can also be supplied as an environment variable.
The convention is:

- Take the full property key.
- **Uppercase** it.
- Replace each **dot (`.`) with an underscore (`_`)**.

For example:

| Property | Environment variable |
| --- | --- |
| `itdastream.broker.id` | `ITDASTREAM_BROKER_ID` |
| `itdastream.kafka.port` | `ITDASTREAM_KAFKA_PORT` |
| `itdastream.storage.s3.bucket.name` | `ITDASTREAM_STORAGE_S3_BUCKET_NAME` |

Internally, ItdaStream loads every `ITDASTREAM_*` environment variable,
lowercases it, and turns underscores back into dots to reconstruct the property
key &mdash; so the property keys themselves must use dots (not underscores),
which all keys below do.

### Placeholder Substitution

Property values support recursive `${...}` placeholder substitution that
references other properties. Most on-disk path properties default to a
subdirectory under `itdastream.base.data.dir`, for example:

```properties
itdastream.base.data.dir=./data
itdastream.metadata.db.path=${itdastream.base.data.dir}/metadata-rocksdb
```

Changing `itdastream.base.data.dir` relocates every dependent path at once.

---

## Service Settings (Common)

| Property | Default | Description |
| --- | --- | --- |
| `itdastream.kafka.port` | `9092` | TCP port on which the broker accepts Kafka wire-protocol connections from producers, consumers, and admin clients. The public, client-facing port. |
| `itdastream.kafka.bind.host` | `0.0.0.0` | Network interface the Kafka listener binds to. `0.0.0.0` binds all interfaces; set a specific IP to restrict it. |
| `itdastream.advertised.host` | `localhost` | Hostname/IP advertised back to clients in metadata responses so they know where to reconnect. Set to a reachable address behind Docker/NAT/load balancers. |
| `itdastream.cluster.id` | `itdastream-cluster` | Logical name of the cluster; all brokers in the same logical cluster share this value. |
| `itdastream.broker.id.stateless` | `true` | When `true`, the broker derives its numeric ID automatically from `hash(host:port)`; when `false`, the fixed `itdastream.broker.id` is used. |
| `itdastream.broker.id` | `1` | Explicit numeric broker ID, used only when `itdastream.broker.id.stateless=false`. Must be unique across the cluster. |
| `itdastream.base.data.dir` | `./data` | Root directory for all local on-disk state. Other path properties default to subdirectories under it. Relative paths resolve against the process working directory. |
| `itdastream.metadata.db.path` | `${itdastream.base.data.dir}/metadata-rocksdb` | RocksDB store directory for cluster metadata (topics, partitions, offsets, producer IDs, transaction state). Local to each broker. |
| `itdastream.cleanup.history.path` | `${itdastream.base.data.dir}/cleanup-history` | RocksDB store directory recording log-retention cleanup history (which segments were deleted and when) for auditing. |
| `itdastream.kms.db.path` | `${itdastream.base.data.dir}/kms-rocksdb` | RocksDB store directory for the cluster KMS (encrypted KEKs/DEKs). Followers overwrite it from the leader's snapshot during bootstrap. Protect this directory. |
| `itdastream.metrics.db.path` | `${itdastream.base.data.dir}/metrics-rocksdb` | RocksDB store directory for collected broker/topic metrics time series shown in the Admin UI. |
| `itdastream.metrics.retention.days` | `7` | How many days of metrics history to keep in the metrics store before older points are pruned. |
| `itdastream.nio.port` | `9000` | TCP port for the internal NIO protocol used for broker-to-broker communication (control plane, KMS/IAM sync, internal data forwarding). Not for Kafka clients. |

---

## Broker Specific Settings

| Property | Default | Description |
| --- | --- | --- |
| `itdastream.broker.net.num.acceptors` | `1` | Number of acceptor threads that accept new inbound socket connections on the Kafka listener. `1` suffices for most workloads. |
| `itdastream.broker.net.concurrency` | `3` | Number of processor/selector threads handling network I/O on the client-facing NIO server. Increase for higher connection/throughput parallelism. |
| `itdastream.broker.net.request.queue.size` | `500` | Maximum number of decoded requests buffered between the network layer and the worker handler pool. Acts as backpressure. |
| `itdastream.broker.worker.concurrency` | `8` | Number of worker threads executing request business logic (produce/fetch/admin). The main request-processing parallelism knob. |
| `itdastream.broker.init.timeout.ms` | `60000` | Maximum time (ms) the broker waits during startup for cluster initialization (controller/KMS/IAM sync) before giving up. |
| `itdastream.network.compression.type` | `SNAPPY` | Compression codec on the internal broker-to-broker protocol. Allowed: `SNAPPY` or `NONE`. Does not affect client-side Kafka message compression. |
| `itdastream.topic.auto.create` | `true` | When `true`, producing to or fetching from a non-existent topic auto-creates it (using `itdastream.topic.default.partitions`); when `false`, topics must be created explicitly. |

---

## Storage Engine Settings

| Property | Default | Description |
| --- | --- | --- |
| `itdastream.storage.engine.type` | `s3` | Storage backend for partition log data. `s3` is object-storage backed (S3 or any S3-compatible store such as MinIO). |
| `itdastream.storage.write.buffer.bytes` | `16777216` | Size (bytes, 16 MiB) of the in-memory write buffer that accumulates produced records before flushing toward storage. |
| `itdastream.storage.segment.max.size` | `16777216` | Maximum size (bytes, 16 MiB) a log segment accumulates before it is closed and uploaded to S3 as an object. |
| `itdastream.storage.read.cache.max.bytes` | `268435456` | Maximum heap (bytes, 256 MiB) used by the per-broker read cache holding recently read segment data, to serve fetches without re-downloading from S3. |
| `itdastream.storage.s3.flush.timeout.ms` | `300000` | Maximum age (ms, 5 min) before an in-progress segment is flushed/uploaded to S3 even if it has not reached `segment.max.size`. Bounds durability lag on low-throughput topics. |
| `itdastream.storage.write.flush.interval.ms` | `5000` | Interval (ms) at which the in-memory write buffer is flushed downstream regardless of fill level. |

### Object Storage Backend Settings (S3)

| Property | Default | Description |
| --- | --- | --- |
| `itdastream.storage.s3.endpoint.url` | `http://localhost:9000` | S3 (or S3-compatible) service endpoint URL. For real AWS S3 leave blank/region-derived; for MinIO set the explicit URL. |
| `itdastream.storage.s3.bucket.name` | `itdastream-data` | Name of the S3 bucket where all partition log segments/objects are stored. Must be writable with the credentials below. |
| `itdastream.storage.s3.region.id` | `us-east-1` | S3 region identifier used when building the client and signing requests. |
| `itdastream.storage.s3.access.key.id` | `any-access-key` | Access key ID for authenticating to the S3 endpoint. Treat as a secret; prefer supplying via environment variable. |
| `itdastream.storage.s3.secret.access.key` | `any-secret-key` | Secret access key paired with the access key ID. Treat as a secret; prefer an environment variable over committing a real value. |
| `itdastream.storage.s3.prefix` | _(empty)_ | Optional key prefix prepended to every object key, letting multiple clusters/tenants share one bucket. Empty means objects are written at the bucket root. |
| `itdastream.storage.s3.path.style.access` | `true` | When `true`, use path-style addressing (`http://endpoint/bucket/key`). Required for most S3-compatible stores like MinIO; usually `false` for real AWS S3. |
| `itdastream.storage.s3.staging.path` | `${itdastream.base.data.dir}/s3-staging` | Local staging directory for segment objects before/during upload to S3. Needs enough disk to hold in-flight segments. |
| `itdastream.storage.s3.staging.encrypt` | `true` | When `true`, data written to the local staging directory is envelope-encrypted at rest before staging, so plaintext is never persisted to local disk. |

---

## Storage Maintenance Settings

| Property | Default | Description |
| --- | --- | --- |
| `itdastream.storage.log.retention.ms` | `604800000` | How long (ms, 7 days) partition log data is retained before being eligible for deletion by the cleanup task. |
| `itdastream.storage.cleanup.interval.ms` | `3600000` | How often (ms, 1 hour) the background retention/cleanup task runs to find and delete logs older than the retention window. |
| `itdastream.storage.cleanup.history.retention.ms` | `15552000000` | How long (ms, ~180 days) entries in the cleanup-history store are kept before being purged. |

---

## Cluster Coordination Settings (ZooKeeper)

| Property | Default | Description |
| --- | --- | --- |
| `itdastream.zookeeper.server.list` | `localhost:2181` | ZooKeeper connection string for cluster coordination (controller election, broker registration, sync signaling). Comma-separated `host:port` list. |
| `itdastream.zookeeper.root.path` | `/itdastream` | Root znode path under which all of this cluster's ZooKeeper nodes are created. Lets multiple clusters share one ensemble. |
| `itdastream.zookeeper.session.timeout.ms` | `30000` | ZooKeeper session timeout (ms): max time without a heartbeat before the session expires and re-election is triggered. |
| `itdastream.zookeeper.connection.timeout.ms` | `10000` | Max time (ms) to wait when establishing the initial TCP connection to ZooKeeper before failing. |
| `itdastream.zookeeper.retry.base.sleep.ms` | `1000` | Initial backoff (ms) between retries in the Curator exponential-backoff retry policy. |
| `itdastream.zookeeper.retry.max.retries` | `3` | Number of times a ZooKeeper operation is retried before giving up. |

---

## Security & Key Management Settings

| Property | Default | Description |
| --- | --- | --- |
| `itdastream.security.kms.type` | `cluster` | KMS implementation. `cluster` uses the built-in distributed KMS: key material in the KMS RocksDB store, coordinated across brokers via ZooKeeper (leader holds the authoritative copy). |
| `itdastream.kms.master.key.env` | `ITDASTREAM_MASTER_KEY` | Name of the OS environment variable that supplies the Master Encryption Key (root secret). The key is read from this env var at startup and is never stored in the file. |
| `itdastream.kms.default.key.id` | `default` | Identifier of the default encryption key used to envelope-encrypt objects/segments written to S3 (and staging) when no per-resource key is specified. |
| `itdastream.sasl.enabled` | `false` | When `true`, enable SASL/PLAIN authentication for Kafka clients. When `false`, the listener is unauthenticated. Combine with `itdastream.ssl.enabled` for SASL_SSL. |
| `itdastream.ssl.enabled` | `false` | When `true`, wrap the Kafka listener in TLS (clients use `security.protocol=SSL`, or SASL_SSL when combined with SASL). Requires the keystore properties below. |
| `itdastream.ssl.protocol` | `TLSv1.3` | TLS protocol version for the SSL listener (e.g. `TLSv1.3`, `TLSv1.2`). Only takes effect when `itdastream.ssl.enabled=true`. |

### SSL/TLS Keystore Properties (optional)

These are commented out by default (plaintext mode). Set them when
`itdastream.ssl.enabled=true`.

| Property | Default | Description |
| --- | --- | --- |
| `itdastream.ssl.keystore.path` _(optional)_ | _(unset)_ | Path to the JKS keystore holding the server's TLS certificate and private key. Required when SSL is enabled. |
| `itdastream.ssl.keystore.password` _(optional)_ | _(unset)_ | Password to open/unlock the JKS keystore file. Required when SSL is enabled and a keystore is configured. |
| `itdastream.ssl.key.password` _(optional)_ | _(unset)_ | Password protecting the private key entry inside the keystore (may differ from the keystore password). |
| `itdastream.ssl.truststore.path` _(optional)_ | _(unset)_ | Path to a JKS truststore of trusted client CA certificates, used only for mutual TLS. Omit if client-certificate auth is not required. |
| `itdastream.ssl.truststore.password` _(optional)_ | _(unset)_ | Password to open the truststore. Only needed when a truststore (mutual TLS) is configured. |

---

## Control Center (Admin UI) Settings

| Property | Default | Description |
| --- | --- | --- |
| `itdastream.admin.ui.port` | `8080` | HTTP port for the Admin UI / Control Center (REST API + web console). Separate from the Kafka and internal NIO ports. |
| `itdastream.admin.ui.static.path` | `admin-ui` | Directory from which the Admin UI's static web assets (HTML/JS/CSS) are served. Change only if you relocate the bundled assets. |
| `itdastream.admin.ui.refresh.interval.ms` | `30000` | How often (ms, 30s) the Admin UI front-end auto-refreshes its charts/metrics views. Delivered to the UI to drive its polling cadence. |

---

## Logging Settings

| Property | Default | Description |
| --- | --- | --- |
| `itdastream.log.mode` | `ASYNC_FILE` | Logging output mode. `ASYNC_FILE`: asynchronous appender writing log files on disk under `itdastream.log.path`. `RING_BUFFER`: in-memory ring buffer optimized for low-overhead real-time tailing. |
| `itdastream.log.path` | `${itdastream.base.data.dir}/logs` | Directory where the broker writes its system log files when `log.mode=ASYNC_FILE`. |

---

## Operational Tuning

### Internal NIO (Broker-to-Broker)

Tuning for the internal control/data protocol between brokers
(`itdastream.nio.port`).

| Property | Default | Description |
| --- | --- | --- |
| `itdastream.internal.nio.worker.threads` | `32` | Number of worker threads servicing internal NIO connections. |
| `itdastream.internal.nio.connect.timeout.ms` | `5000` | Max time (ms) to establish an outbound TCP connection to a peer broker before failing. |
| `itdastream.internal.nio.read.timeout.ms` | `30000` | Max time (ms) to wait for a response/read on an internal NIO request before timing out. |
| `itdastream.internal.nio.socket.timeout.ms` | `30000` | Underlying socket read (`SO_TIMEOUT`) timeout (ms) for internal NIO connections. |
| `itdastream.internal.nio.max.frame.bytes` | `10485760` | Maximum allowed size (bytes, 10 MiB) of a single internal NIO protocol frame; larger frames are rejected. |

### Admin Server

Tuning for the Admin UI / REST HTTP server.

| Property | Default | Description |
| --- | --- | --- |
| `itdastream.admin.executor.threads` | `16` | Thread-pool size for executing admin request handler logic. |
| `itdastream.admin.net.worker.threads` | `4` | Number of network (Netty) worker threads handling admin HTTP I/O. |
| `itdastream.admin.http.max.content.length` | `67108864` | Maximum accepted HTTP request body size (bytes, 64 MiB) for admin endpoints; larger requests are rejected. Sized to accept streaming-job dependency JAR uploads (`POST /admin/deps`); the Netty aggregator only buffers bytes actually sent, so a large cap is free for small requests. |
| `itdastream.admin.cors.max.age.seconds` | `86400` | Value (seconds, 24h) sent in the CORS `Access-Control-Max-Age` header, telling browsers how long to cache a preflight result. |
| `itdastream.admin.cleanup.history.max.records` | `50` | Maximum number of cleanup-history records returned/retained for display by admin cleanup-history endpoints. |
| `itdastream.admin.monitoring.default.window.ms` | `3600000` | Default time window (ms, 1 hour) used by admin monitoring endpoints when a caller does not specify one. |
| `itdastream.topic.default.partitions` | `10` | Default partition count applied when a topic is created without an explicit count (including auto-created topics). |

### Metrics

Collection and retention scheduling for the metrics subsystem.

| Property | Default | Description |
| --- | --- | --- |
| `itdastream.metrics.collector.connect.timeout.ms` | `2000` | Connect timeout (ms) when the metrics collector reaches out to peer brokers to pull their metrics. |
| `itdastream.metrics.collector.read.timeout.ms` | `5000` | Read timeout (ms) for the metrics collector's per-broker metrics fetch. |
| `itdastream.metrics.collector.initial.delay.ms` | `10000` | Delay (ms, 10s) after startup before the first metrics collection run. |
| `itdastream.metrics.collector.interval.ms` | `30000` | Period (ms, 30s) between successive metrics collection runs. |
| `itdastream.metrics.retention.initial.delay.hours` | `1` | Delay (hours) after startup before the first metrics-retention (pruning) run. |
| `itdastream.metrics.retention.interval.hours` | `6` | Period (hours) between metrics-retention runs that prune points older than `itdastream.metrics.retention.days`. |

### Cluster Coordination

| Property | Default | Description |
| --- | --- | --- |
| `itdastream.internal.kms.iam.sync.timeout.ms` | `10000` | Timeout (ms) for an internal KMS/IAM state-sync request between brokers (follower pulling authoritative state from the leader). |
| `itdastream.broker.controller.poll.interval.ms` | `1000` | How often (ms, 1s) a broker polls ZooKeeper for controller/leadership changes during its controller-watch loop. |
| `itdastream.group.coordinator.join.default.timeout.ms` | `30000` | Default JoinGroup rebalance timeout (ms, 30s) for the consumer group coordinator when a client does not supply one. |
| `itdastream.group.coordinator.sync.timeout.ms` | `30000` | SyncGroup timeout (ms, 30s): how long the coordinator waits for the group leader to send the partition assignment. |
| `itdastream.cluster.bootstrap.fetch.max.retries` | `30` | Number of fetch attempts for the mandatory KMS/IAM bootstrap pull on non-leader startup. Exhausting the budget exits with code 1 so the supervisor restarts the broker. |
| `itdastream.cluster.bootstrap.fetch.retry.delay.ms` | `1000` | Wait (ms) between bootstrap fetch attempts. |
| `itdastream.cluster.sync.signal.write.max.retries` | `5` | Number of attempts for the leader-side `/sync-signal` write, so a transient ZK glitch during key rotation or IAM mutation does not drop the notification. |
| `itdastream.cluster.sync.signal.write.retry.delay.ms` | `500` | Wait (ms) between `/sync-signal` write attempts. |
| `itdastream.cluster.periodic.self.sync.interval.ms` | `300000` | Non-leader periodic self-check sync (ms) as a backstop for any missed `/sync-signal` notification. `0` disables. |
| `itdastream.coordinator.leader.deference.ms` | `3000` | Sticky-leader startup deference window (ms): a non-incumbent broker waits this long before joining the election if the previous leader is still alive. `0` disables. |

### S3 Storage Timeouts

HTTP client timeouts for the S3 object-storage client.

| Property | Default | Description |
| --- | --- | --- |
| `itdastream.storage.s3.connection.timeout.ms` | `120000` | Max time (ms, 120s) to establish a TCP connection to the S3 endpoint. |
| `itdastream.storage.s3.socket.timeout.ms` | `120000` | Max idle time (ms, 120s) on an established socket waiting for data. Raise for slow/distant object stores or large objects. |

### Timer

| Property | Default | Description |
| --- | --- | --- |
| `itdastream.timer.scheduler.threads` | `2` | Number of threads in the broker's internal scheduled-timer pool used to run periodic/delayed background tasks. |

---

## Misc Operational Tuning

| Property | Default | Description |
| --- | --- | --- |
| `itdastream.security.jwt.expiration.ms` | `86400000` | JWT token expiration window (ms, 24h). Lower for tighter admin session lifetimes; raise for longer-lived sessions. |
| `itdastream.metadata.producer.id.block.size` | `1000` | Number of producer IDs allocated per ZooKeeper refill. Larger blocks reduce ZK write pressure but waste more IDs on broker restart. |
| `itdastream.metadata.transaction.load.cache.ttl.ms` | `1000` | Minimum interval (ms) between full ZK reloads of the transaction state map (cache TTL). Mutating ops bypass this cache. |
| `itdastream.broker.id.stateless.max` | `1000000` | Stateless broker ID modulus. When `itdastream.broker.id.stateless=true`, the broker ID is `hash(host:port) % this value`. Caps the ID space. |
| `itdastream.executor.shutdown.await.seconds` | `5` | Executor shutdown grace period (seconds): how long `stop()` waits for in-flight scheduled tasks to drain before forcing termination. |
