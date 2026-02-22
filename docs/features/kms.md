# Envelope Encryption and Key Management Service (KMS)

ItdaStream includes a built-in distributed Key Management Service that provides envelope encryption for data at rest and in transit between brokers.

## Three-layer key hierarchy

1. Master Key: Derived from environment variable using PBKDF2-SHA256 (200,000 iterations, 32-byte salt). Protects the keystore itself
2. Key Encryption Key (KEK): AES-256, versioned, leader-managed. Used to wrap/unwrap Data Encryption Keys. Supports rotation with backward compatibility
3. Data Encryption Key (DEK): AES-256-GCM, generated per encryption operation. Provides actual data encryption

## Encryption format

[KEK_Version(4B)][KEK_IV(12B)][WrappedDEK_Len(4B)][WrappedDEK]
[DEK_IV(12B)][EncryptedData]

## Key lifecycle management
- Key states: ACTIVE → RETIRED → REVOKED
- Version-controlled rotation: new versions can encrypt while old versions remain available for decryption
- Only the controller broker can create or rotate keys
- Keystore changes are automatically broadcast to all followers via internal NIO protocol
- RocksDB-backed persistent storage, encrypted with master key