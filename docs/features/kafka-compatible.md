# Full Kafka Protocol Compatibility

ItdaStream implements 24 standard Apache Kafka APIs with multi-version support (Kafka 2.x/3.x wire protocol). Applications can connect using any standard Kafka client library without code changes — simply point `bootstrap.servers` to the ItdaStream cluster.

- **Producer/Consumer**: Produce (v0-v9), Fetch (v0-v12) with isolation levels
- **Metadata & Offsets**: Metadata, OffsetCommit, OffsetFetch
- **Consumer Groups**: JoinGroup, SyncGroup, Heartbeat, LeaveGroup, FindCoordinator, DescribeGroups, ListGroups
- **Topic Management**: CreateTopics, DeleteTopics with optional auto-creation
- **Transactions**: InitProducerId, AddPartitionsToTxn, AddOffsetsToTxn, EndTxn (full exactly-once semantics)
- **Authentication**: SaslHandshake, SaslAuthenticate (SASL/PLAIN, SASL_SSL)
