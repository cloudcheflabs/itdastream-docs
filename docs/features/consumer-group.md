# Consumer Group Coordination

ItdaStream implements the full Kafka consumer group rebalancing protocol, enabling automatic partition assignment across consumer instances with dynamic scaling.

## Rebalance protocol state machine

- EMPTY → PREPARING_REBALANCE → COMPLETING_REBALANCE → STABLE → (member join/leave triggers new rebalance)
- First member joining becomes the group leader; leader computes partition assignments
- Generation ID incremented on each rebalance cycle for consistency
- Member IDs generated as clientId-UUID for uniqueness

## Session management

- Heartbeat-based liveness detection with configurable session timeout
- Background thread monitors expired members and triggers automatic rebalance
- Graceful leave with batch support (v3+) for cooperative shutdown

## Offset management

- Per-group, per-topic-partition offset storage in ZooKeeper
- Leader epoch tracking for offset validation (v5+)
- Batch offset fetch for efficient group state retrieval
