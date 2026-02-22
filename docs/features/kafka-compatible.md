# Full Kafka Protocol Compatibility

ItdaStream implements 24 standard Apache Kafka APIs with multi-version support (Kafka 2.x/3.x wire protocol), enabling seamless integration with all existing Kafka clients, tools, and
ecosystems. Applications can connect to ItdaStream using any standard Kafka client library (Java, Python, Go, Node.js, etc.) without code changes — simply point the bootstrap.servers to
the ItdaStream cluster.

## Supported APIs include

- Producer APIs: Produce (v0-v9) with configurable acks, batching, and idempotent/transactional writes
- Consumer APIs: Fetch (v0-v12) with isolation levels (READ_COMMITTED / READ_UNCOMMITTED), session management, and compact encoding
- Metadata APIs: Metadata (v0-v9) for broker/topic/partition discovery with controller identification
- Offset Management: OffsetCommit (v0-v7) and OffsetFetch (v0-v5) for consumer group offset persistence
- Consumer Group Coordination: JoinGroup, SyncGroup, Heartbeat, LeaveGroup, FindCoordinator, DescribeGroups, ListGroups — full rebalance protocol support
- Topic Management: CreateTopics (v0-v4), DeleteTopics (v0-v3), with optional auto-creation on produce
- Transaction APIs: InitProducerId, AddPartitionsToTxn, AddOffsetsToTxn, EndTxn, TxnOffsetCommit — full exactly-once semantics
- Authentication APIs: SaslHandshake (v0-v1) and SaslAuthenticate (v0-v1) for SASL/PLAIN and SASL_SSL