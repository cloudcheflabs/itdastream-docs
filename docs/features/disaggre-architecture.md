# Disaggregated Compute-Storage Architecture

ItdaStream separates compute (brokers) from storage (S3-compatible object storage), enabling independent scaling and cost optimization. Brokers are stateless — all persistent data is
stored in S3, making horizontal scaling and broker replacement straightforward.

## Three-tier write path ensures both performance and durability

1. In-memory WriteBuffer (16MB per partition, configurable): Records are accumulated in memory with automatic offset rewriting and CRC-32C recalculation for continuous sequencing
2. RocksDB Staging Store (local disk): Before S3 upload, records are encrypted and persisted locally for crash recovery. On broker restart, unsynced data is automatically recovered and
   uploaded
3. S3 Object Storage (permanent): Segment and index files are uploaded when the buffer reaches max size (16MB) or a time-based flush triggers (5s interval, 5min maximum latency)

## S3-compatible storage support

- Works with AWS S3, MinIO, ShannonStore, and any S3-compatible object storage
- Path-style access support for non-AWS systems
- Configurable endpoint, region, bucket, and prefix
