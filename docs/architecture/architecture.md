# Architecture

ItdaStream is a high-performance, distributed streaming platform designed to provide a Kafka-compatible interface while leveraging Cloud-Native Tiered Storage. It decoupling compute from
storage by using S3-compatible object storage as its primary persistence layer, while maintaining extreme performance for real-time workloads through a custom NIO-based storage engine.


## ItdaStream Architecture

<img width="600" src="../../images/architecture/itdastream-architecture.png" align="center"/>


### Core Philosophy

* API Compatibility: Full support for the Kafka Wire Protocol, allowing existing applications to migrate without code changes.
* Tiered Storage by Default: Seamless movement of data from high-speed memory/local SSDs to cost-effective S3 storage.
* Stateless Scalability: By offloading logs to S3, brokers maintain minimal local state, enabling rapid scaling and recovery.
* Security First: Integrated Key Management Service (KMS) with envelope encryption for cache data on local.


### High-Level Components

#### Broker
The Broker is the entry point for Kafka clients. It implements the Kafka request/response protocol using NIO.

* Responsibility: Handling Produce, Fetch, and Metadata requests.
* Concurrency Model: Utilizes a multi-threaded event loop (Acceptor/Processor/Handler) to handle thousands of concurrent connections with minimal latency.


#### Log Store
A custom-built distributed log engine that manages the lifecycle of a message.

* Write Path: Messages are first written to a Memory Write Buffer and persisted to a local RocksDB instance for immediate durability.
* S3 Flush: Background workers aggregate memory segments into immutable objects and flush them to S3 once they reach a size threshold or time limit.
* Read Path: Optimized for "Tail Reads" (reading recent data from memory) and "Historical Reads" (streaming objects directly from S3).


#### Metadata & Coordination
ItdaStream uses Apache ZooKeeper for cluster coordination and RocksDB for local metadata caching.

* Leader Election: Manages partition leadership and broker registration.
* Controller Model: A designated controller node handles administrative tasks like topic creation and partition rebalancing.
* KMS Integration: Manages data encryption keys (DEK) derived from a Master Key, ensuring every topic can have its own encryption lifecycle.


#### Streaming Engine
A built-in, Flink-style streaming layer that runs on the brokers themselves.

* The controller broker acts as the **master** (job assignment + checkpoint coordination) and every other broker acts as a **worker** (running consumer-thread executors).
* Pipelines move data from a topic to Iceberg and other sinks with exactly-once semantics for transactional sinks, configured no-code in the Admin UI or via the Java SDK. See [Streaming](../features/streaming.md).

#### Admin UI (Control Center)
Management console.

* Features: Real-time monitoring of TPS (Transactions Per Second), topic management, IAM (Identity and Access Management) configuration, streaming jobs, connection registry, and a built-in log browser.



### Data Flow Architecture


The Write Path (Produce)

* Client Request: A Kafka Producer sends messages to the Broker.
* Validation: The Broker validates the request.
* Local Append: Data is appended to the active Write Buffer.
* Local Durability: The segment is indexed in the local RocksDB. An ACK is returned to the producer as soon as the data is safe in the local node.
* Background Upload: Once the segment is full (e.g., 16MB), it is encrypted and uploaded to S3.


The Read Path (Fetch)

* Client Request: A Kafka Consumer requests data at a specific offset.
* Cache Hit (Hot Data): If the data is in the Memory Buffer or Local RocksDB, it is served instantly (sub-millisecond latency).
* Cache Miss (Cold Data): If the data is older, the LogStore fetches the relevant segment from S3.
* Streaming Delivery: Data is streamed back to the consumer using Zero-Copy principles where possible.


### Key Advantages

Separation of Storage and Compute

* Unlike traditional Kafka, where disk space limits retention, ItdaStream allows you to store petabytes of data on S3 while keeping your broker fleet lean. You only scale CPU/RAM for active
traffic.


Cost Efficiency

* By utilizing S3 for 99% of data storage, ItdaStream reduces infrastructure costs compared to clusters relying solely on expensive local NVMe/SSD storage.

