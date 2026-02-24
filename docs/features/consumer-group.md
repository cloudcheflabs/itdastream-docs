# Consumer Group Coordination

ItdaStream implements the full Kafka consumer group rebalancing protocol, enabling automatic partition assignment with dynamic scaling.

- **Rebalance protocol**: EMPTY → PREPARING_REBALANCE → COMPLETING_REBALANCE → STABLE, with generation ID tracking
- **Session management**: Heartbeat-based liveness detection with configurable timeout; automatic rebalance on member join/leave
- **Offset management**: Per-group, per-partition offset storage in ZooKeeper with leader epoch tracking
