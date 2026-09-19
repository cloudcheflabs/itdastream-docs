# Disaggregated Compute-Storage Architecture

ItdaStream separates compute (brokers) from storage (S3-compatible object storage), enabling independent scaling and cost optimization. Brokers are stateless — all persistent data resides in S3.

- **Three-tier write path**: In-memory WriteBuffer (16MB/partition) → RocksDB Staging Store (local crash recovery) → S3 Object Storage (permanent)
- **S3 compatibility**: Works with AWS S3, MinIO, ShannonStore, and any S3-compatible object storage
- **Flush triggers**: Buffer full (16MB) or time-based flush (5s interval, 5min max latency)

## What statelessness actually buys

The point of keeping the log in object storage is not storage cost. It is that **a broker owns nothing**, so removing one is not a handover.

- **Scaling out** is starting a process. It registers, and the next metadata response spreads partitions across it. There is no data to copy first.
- **Scaling in** is stopping a process, and [draining](broker-drain.md) makes it invisible to clients before it stops rather than after.
- **Losing a broker** costs whatever was in its write buffer and not yet flushed — which its [staging store](staging-store.md) replays when it comes back.

The cost is on the read path: reads are served from flushed segments, so a record is acknowledged before it is readable. A consumer at the very tail trails the producer by at most one flush interval (`itdastream.storage.write.flush.interval.ms`, default 5s). That is the trade the design makes — durability and broker-independence at the cost of a few seconds of read visibility.
