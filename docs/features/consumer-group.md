# Consumer Group Coordination

ItdaStream implements the full Kafka consumer group rebalancing protocol, enabling automatic partition assignment with dynamic scaling.

- **Rebalance protocol**: EMPTY → PREPARING_REBALANCE → COMPLETING_REBALANCE → STABLE, with generation ID tracking
- **Session management**: Heartbeat-based liveness detection with configurable timeout; automatic rebalance on member join/leave
- **Offset management**: Per-group, per-partition offset storage in ZooKeeper with leader epoch tracking

## One coordinator per group, cluster-wide

Group membership lives in the coordinating broker's memory, so every member of a group must talk to the same broker. `FindCoordinator` answers with the same broker for a group id no matter which broker is asked: it picks one by **rendezvous hashing** of the group id over the live brokers, skipping draining brokers while any other is serving. A broker joining or leaving moves only the groups it gains or loses.

`JoinGroup`, `SyncGroup`, `Heartbeat` and `LeaveGroup` sent to any other broker answer `NOT_COORDINATOR`, which makes Kafka clients look the coordinator up again. That is how members converge when the coordinator moves (a broker drains, stops or joins).

This matters for streaming jobs too: each executor's consumer bootstraps to the broker it runs on, and with parallelism above one the executors sit on different brokers. They still form one group, share the partitions, and each record is processed once.

Committed offsets are stored in ZooKeeper, so `OffsetCommit` / `OffsetFetch` work on any broker and survive a coordinator move.
