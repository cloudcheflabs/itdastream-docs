# Disaggregated Compute-Storage Architecture

ItdaStream separates compute (brokers) from storage (S3-compatible object storage), enabling independent scaling and cost optimization. Brokers are stateless — all persistent data resides in S3.

- **Three-tier write path**: In-memory WriteBuffer (16MB/partition) → RocksDB Staging Store (local crash recovery) → S3 Object Storage (permanent)
- **S3 compatibility**: Works with AWS S3, MinIO, ShannonStore, and any S3-compatible object storage
- **Flush triggers**: Buffer full (16MB) or time-based flush (5s interval, 5min max latency)
