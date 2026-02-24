# Intelligent Segment Caching

ItdaStream uses a weight-based LRU cache (Caffeine) to minimize S3 reads and reduce consumer fetch latency.

- **Eviction**: Weight-based LRU with configurable max size (default 256MB)
- **Fetch path**: Cache lookup → S3 fallback on miss → binary search in index → record extraction → merge with in-memory buffer
- **Immediate population**: Newly flushed segments are cached right after S3 upload, keeping hot data always cache-resident
