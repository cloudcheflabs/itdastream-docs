# Log Retention and Lifecycle Management

ItdaStream provides configurable time-based log retention with automatic cleanup of expired segments from S3 storage.

- **Defaults**: 7-day retention (`itdastream.storage.log.retention.ms`, cluster-wide), 1-hour cleanup interval, 180-day cleanup history
- **Unlimited**: `-1` (or any value ≤ 0) keeps data forever. Earlier versions treated it as "now + 1 ms" and deleted every segment
- **Standby clusters** do not run retention: their bucket is a replica ([Disaster Recovery](disaster-recovery.md))
- **Cleanup process**: Controller-only; deletes expired segments (.seg/.idx) and their entries in the bucket's segment list
- **Operations**: Manual trigger via Admin API (`POST /admin/maintenance/cleanup/trigger`), cleanup history viewer (`GET /admin/maintenance/cleanup/history`)
