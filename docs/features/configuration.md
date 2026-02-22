# Flexible Configuration System

ItdaStream provides a layered configuration system with multiple override levels for deployment flexibility.

## Configuration priority (highest to lowest)

1. System properties (-Ditdastream.*): Highest precedence, ideal for per-instance overrides
2. Properties file (itdastream.properties): Base configuration with all settings
3. Hardcoded defaults: Sensible production defaults for all settings

## Property placeholder support

- Recursive substitution: ${itdastream.base.data.dir}/metadata-rocksdb
- Enables DRY configuration with shared base paths

## Major configuration categories

- Service settings: ports (Kafka 9092, internal NIO 7000, Admin UI 8080), bind host, cluster ID
- Storage: S3 endpoint/bucket/region/credentials, segment size, buffer size, cache size
- Concurrency: acceptor threads, processor threads, handler threads, request queue size
- Security: SASL enable, SSL enable, keystore/truststore paths, KMS master key
- Retention: log retention period, cleanup interval, history retention
- ZooKeeper: connection string, timeouts, retry policy
