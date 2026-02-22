# Monitoring and Metrics

ItdaStream collects and exposes comprehensive system and application metrics for monitoring and alerting.

## Metrics collection (Dropwizard Metrics)

- System metrics: CPU usage, memory (used/max/free/system), disk I/O
- Network metrics: Packet send/receive counts, byte throughput
- Kafka API metrics: Per-API TPS (transactions per second), request/response latency timers
- Storage metrics: Segment cache hit rate, S3 operation latencies
- JMX export: Automatic JMX Reporter for Java management tools

## Metrics storage and aggregation

- Time-series storage in RocksDB with binary key format: [timestamp][nodeId][metricName]
- Controller broker collects from all brokers every 30 seconds via HTTP
- 7-day retention with automatic pruning (every 6 hours)

## Prometheus integration

- /metrics endpoint exports Prometheus text format
- Metrics grouped by broker ID and metric name
- Gauges (CPU%, memory), Counters (bytes sent/received), Histograms (latency percentiles)

