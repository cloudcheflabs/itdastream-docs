# Log Retention and Lifecycle Management

ItdaStream provides configurable time-based log retention with automatic cleanup of expired segments from S3 storage.

- **Defaults**: 7-day retention, 1-hour cleanup interval, 180-day cleanup history
- **Cleanup process**: Controller-only; deletes expired segments (.seg/.idx) from S3 and removes metadata from ZooKeeper
- **Operations**: Manual trigger via Admin API (`POST /admin/maintenance/cleanup/trigger`), cleanup history viewer (`GET /admin/maintenance/cleanup/history`)
