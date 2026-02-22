# Log Retention and Lifecycle Management

ItdaStream provides configurable time-based log retention with automatic cleanup of expired segments from S3 storage.

## Retention configuration

- Default retention: 7 days (itdastream.storage.log.retention.ms)
- Cleanup check interval: 1 hour (itdastream.storage.cleanup.interval.ms)
- Cleanup history retention: 180 days (itdastream.storage.cleanup.history.retention.ms)

## Cleanup process (controller-only)

1. Calculate cutoff time: now - retentionMs
2. Iterate all topics and partitions
3. For each segment: check createdTimestamp < cutoffTime
4. Delete eligible segments from S3 (both .seg and .idx files)
5. Remove segment metadata from ZooKeeper
6. Record cleanup job history: segments deleted, bytes freed, duration

## Operational features

- Manual cleanup trigger via Admin API: POST /admin/maintenance/cleanup/trigger
- Cleanup history viewer: GET /admin/maintenance/cleanup/history (up to 50 recent jobs)
- RocksDB-backed history store with automatic pruning of old records