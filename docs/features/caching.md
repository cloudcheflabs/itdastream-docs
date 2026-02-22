# Intelligent Segment Caching

ItdaStream implements a weight-based LRU cache using Caffeine to minimize S3 reads and optimize consumer fetch latency.

## Cache architecture

- Library: Caffeine (high-performance Java caching)
- Eviction: Weight-based LRU; weight = byte array size
- Max size: 256MB (configurable readCacheMaxSizeBytes)
- Keys: S3 object keys (segment + index files)
- Values: Raw byte arrays

## Fetch path with caching

1. Look up segment metadata from MetadataStore
2. Check cache for index file → S3 download on miss
3. Binary search in IndexFile for target offset position
4. Check cache for segment data → S3 download on miss
5. Extract matching record batches from segment
6. Merge with unflushed WriteBuffer data

Immediate cache population: Newly flushed segments are added to the cache immediately after S3 upload, ensuring hot data is always cache-resident.

