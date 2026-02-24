# Monitoring and Metrics

ItdaStream collects and exposes comprehensive system and application metrics for monitoring and alerting.

- **Metrics collected**: CPU, memory, disk I/O, network throughput, per-API TPS/latency, cache hit rate, S3 latencies
- **Storage**: Time-series in RocksDB, controller aggregates from all brokers every 30s, 7-day retention
- **Prometheus**: `/metrics` endpoint with Gauges, Counters, and Histograms grouped by broker ID
- **JMX**: Automatic JMX Reporter for Java management tools
