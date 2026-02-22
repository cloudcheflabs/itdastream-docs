# Network Compression

ItdaStream supports configurable compression for network data transfer, reducing bandwidth usage and improving throughput.

## Supported codecs

- SNAPPY (default): Xerial Snappy library — optimized for speed over compression ratio
- NONE: Pass-through, no compression overhead

## Integration

- Compression type stored in Kafka RecordBatch attributes (bits 0-2)
- Applied at the transport layer; transparent to application logic
- Factory pattern: CompressorFactory.getCompressor(type) returns singleton compressor