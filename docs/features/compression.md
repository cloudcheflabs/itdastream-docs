# Network Compression

ItdaStream supports configurable compression for network data transfer, reducing bandwidth usage and improving throughput.

- **Supported codecs**: SNAPPY (default, speed-optimized) and NONE (pass-through)
- **Integration**: Compression type stored in Kafka RecordBatch attributes; transparent to application logic
