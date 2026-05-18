# Installation

This page describes the requirements for running ItdaStream and the different
ways to install it &mdash; on a single machine for evaluation, or as a
multi-broker cluster for production-like workloads.

If you just want to try ItdaStream as fast as possible, jump straight to the
[Getting Started](../intro/intro.md) guide.

## System Requirements

ItdaStream is a Kafka-compatible distributed streaming platform written in
Java. Before installing, make sure the following are available.

| Requirement | Details |
| --- | --- |
| **Java** | Java 17 or later (the broker is built and tested on Java 17). |
| **ZooKeeper** | Used for cluster coordination &mdash; broker membership, controller (leader) election, topic metadata, consumer group offsets. A bundled ZooKeeper is included in the distribution for local use. |
| **S3-compatible storage** | All log data is persisted to an object store. Amazon S3, MinIO, or any S3-API-compatible service works. |
| **Operating system** | Linux or macOS. |
| **Memory** | At least 2 GB of free RAM per broker for evaluation; size up for production. |

ItdaStream uses a **disaggregated compute-storage architecture**: brokers are
stateless and all durable data lives in the object store. This means brokers
can be added, removed, or restarted freely as long as ZooKeeper and the S3
bucket are reachable.

## Step 1. Download the Distribution

Download and extract the latest release archive.

```bash
curl -L -O https://github.com/cloudcheflabs/itdastream-pack/releases/download/itdastream-archive/itdastream-1.0.0.tar.gz

tar zxvf itdastream-1.0.0.tar.gz

cd itdastream-1.0.0
```

The extracted directory has the following layout.

```
itdastream-1.0.0/
├── bin/        # start/stop scripts and Kafka-compatible CLI tools
├── conf/       # configuration files
├── lib/        # broker and dependency JARs
└── logs/       # broker log output
```

## Step 2. Prepare Object Storage

Create an S3 bucket that ItdaStream will use to store all topic data, along
with a set of credentials (access key / secret key) that can read and write to
it.

You will need these five values:

- Endpoint URL
- Bucket name
- Region ID
- Access key ID
- Secret access key

## Step 3. Configure the Broker

ItdaStream resolves every configuration property using the following
precedence, from highest to lowest:

1. **System properties** &mdash; `-Ditdastream.*` JVM arguments
2. **Environment variables** &mdash; `ITDASTREAM_*`
3. **Configuration file** &mdash; under `conf/`
4. **Built-in defaults**

For a quick start, environment variables are the simplest option.

```bash
# Master key used for envelope encryption. Must be at least 32 characters.
export ITDASTREAM_MASTER_KEY="MustBeChanged01234567890123456789012345678901"

# S3-compatible object storage connection info.
export ITDASTREAM_STORAGE_S3_ENDPOINT_URL=endpoint
export ITDASTREAM_STORAGE_S3_BUCKET_NAME=bucket
export ITDASTREAM_STORAGE_S3_REGION_ID=any-region
export ITDASTREAM_STORAGE_S3_ACCESS_KEY_ID=access-key
export ITDASTREAM_STORAGE_S3_SECRET_ACCESS_KEY=secret-key
```

> The `ITDASTREAM_MASTER_KEY` environment variable **must be at least 32
> characters** and must be exported before starting any broker. Keep this value
> secret and identical across every broker in a cluster &mdash; it is the root
> of the encryption key hierarchy.

Commonly used properties (set as `itdastream.*` properties or the equivalent
`ITDASTREAM_*` environment variable):

| Property | Description | Default |
| --- | --- | --- |
| `itdastream.kafka.port` | Port that Kafka clients connect to. | `9092` |
| `itdastream.nio.port` | Internal broker-to-broker port. | &mdash; |
| `itdastream.zookeeper.server.list` | Comma-separated `host:port` list of ZooKeeper servers. | `localhost:2181` |
| `itdastream.storage.engine.type` | Storage backend type. | `s3` |

## Step 4. Start ZooKeeper

For a local installation, start the bundled ZooKeeper.

```bash
bin/start-zk.sh
```

For production, point ItdaStream at an existing, externally managed ZooKeeper
ensemble via `itdastream.zookeeper.server.list`.

## Step 5. Start the Broker

With the environment variables exported, start the broker.

```bash
bin/start-broker.sh
```

The broker performs its startup sequence &mdash; connecting to ZooKeeper,
electing or joining the controller, registering itself, initializing storage,
and opening the Kafka and admin network listeners.

## Step 6. Verify the Installation

Open the admin UI in a browser:

```
http://localhost:8080/admin
```

The default administrator credentials are `admin` / `admin`. You will be asked
to change the password on first login.

<img width="1200" src="../../images/getting-started/dashboard.png"/>

You can also verify the broker by producing and consuming a message &mdash; see
[Getting Started](../intro/intro.md) for a full walkthrough.

## Running a Multi-Broker Cluster

Because brokers are stateless, scaling out is straightforward:

1. Point every broker at the **same ZooKeeper ensemble** and the **same S3
   bucket**.
2. Export the **same `ITDASTREAM_MASTER_KEY`** on every broker.
3. Start each broker with `bin/start-broker.sh`.

Brokers discover one another through ZooKeeper, one is elected as the
controller, and clients are automatically routed to partition leaders. No
broker carries a persistent identity &mdash; a broker ID is derived from its
IP and port.

## Installing with Docker

A container image can be built from the source repository's `Dockerfile`.
Run the container with the same `ITDASTREAM_*` environment variables, and
publish the Kafka port (`9092`) and the admin port (`8080`). A reachable
ZooKeeper and S3 endpoint are still required.

## Stopping the Servers

Stop the broker and ZooKeeper when finished.

```bash
bin/stop-broker.sh
bin/stop-zk.sh
```

## Next Steps

- Follow the [Getting Started](../intro/intro.md) guide to send and receive
  your first messages.
- Review the [Architecture](../architecture/architecture.md) overview to
  understand how ItdaStream works internally.
- Browse the **Features** section for details on transactions, encryption,
  access control, caching, retention, and more.
