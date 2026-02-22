# Exactly-Once Transactional Semantics

ItdaStream provides full exactly-once delivery semantics through its transaction management system, matching Apache Kafka's transactional guarantees. This enables atomic writes across
multiple partitions and atomic offset commits within the same transaction.

## Transaction lifecycle

- InitProducerId: Allocates producer ID with epoch management; block-based ID allocation (1000 per block) stored in ZooKeeper
- AddPartitionsToTxn: Registers topic-partitions in the transaction scope
- AddOffsetsToTxn: Includes consumer group offset commits atomically with data writes
- EndTxn (Commit/Abort): Writes COMMIT or ABORT control markers to each partition; markers are immediately flushed to S3 for visibility

## Isolation levels

- read_uncommitted: All records visible immediately (default)
- read_committed: Only committed records and non-transactional records visible; server-side filtering of aborted transactions during Fetch

Transaction state machine: EMPTY → ONGOING → PREPARE_COMMIT/PREPARE_ABORT → COMPLETE_COMMIT/COMPLETE_ABORT → DEAD, persisted in ZooKeeper for crash recovery.

