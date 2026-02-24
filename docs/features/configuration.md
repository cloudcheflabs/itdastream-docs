# Flexible Configuration System

ItdaStream provides a layered configuration system with multiple override levels for deployment flexibility.

- **Priority** (highest → lowest): System properties (`-Ditdastream.*`) → Properties file (`itdastream.properties`) → Hardcoded defaults
- **Placeholder support**: Recursive substitution (e.g., `${itdastream.base.data.dir}/metadata-rocksdb`)
- **Categories**: Service ports, S3 storage, concurrency/threading, security (SASL/SSL/KMS), retention, ZooKeeper
