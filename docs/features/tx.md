# Exactly-Once Transactional Semantics

ItdaStream provides full exactly-once delivery semantics through its transaction management system, matching Apache Kafka's transactional guarantees.

- **Transaction lifecycle**: InitProducerId → AddPartitionsToTxn → AddOffsetsToTxn → EndTxn (Commit/Abort)
- **Isolation levels**: `read_uncommitted` (all records visible, default) and `read_committed` (only committed/non-transactional records visible)
- **State machine**: EMPTY → ONGOING → PREPARE_COMMIT/PREPARE_ABORT → COMPLETE_COMMIT/COMPLETE_ABORT, persisted in ZooKeeper for crash recovery
