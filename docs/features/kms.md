# Envelope Encryption and Key Management Service (KMS)

ItdaStream includes a built-in distributed KMS that provides envelope encryption for data at rest and in transit between brokers.

- **Three-layer key hierarchy**: Master Key (PBKDF2-SHA256) → Key Encryption Key (AES-256, versioned) → Data Encryption Key (AES-256-GCM, per-operation)
- **Key lifecycle**: ACTIVE → RETIRED → REVOKED, with version-controlled rotation for backward-compatible decryption
- **Management**: Only the controller broker creates/rotates keys; changes are broadcast to all followers via internal NIO protocol
- **Storage**: RocksDB-backed persistent keystore, encrypted with master key
