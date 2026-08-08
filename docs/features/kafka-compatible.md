# Full Kafka Protocol Compatibility

ItdaStream implements 24 standard Apache Kafka APIs with multi-version support (Kafka 2.x/3.x wire protocol). Applications can connect using any standard Kafka client library without code changes — simply point `bootstrap.servers` to the ItdaStream cluster.

- **Producer/Consumer**: Produce, Fetch (with isolation levels), ListOffsets
- **Metadata & Offsets**: Metadata, OffsetCommit, OffsetFetch
- **Consumer Groups**: JoinGroup, SyncGroup, Heartbeat, LeaveGroup, FindCoordinator, DescribeGroups, ListGroups
- **Topic Management**: CreateTopics, DeleteTopics with optional auto-creation
- **Transactions**: InitProducerId, AddPartitionsToTxn, AddOffsetsToTxn, EndTxn, TxnOffsetCommit (full exactly-once semantics)
- **Cluster & Config**: ApiVersions, DescribeConfigs
- **Authentication**: SaslHandshake, SaslAuthenticate (SASL/PLAIN, SASL_SSL)

The 24 APIs are the request handlers registered in `RequestDispatcher` plus `SaslAuthenticate` (processed inline by the network auth layer). Standard client operations — producing, consuming, consumer-group membership, topic administration, and transactions — all work against these handlers.
