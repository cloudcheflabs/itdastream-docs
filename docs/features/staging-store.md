# Crash Recovery with Staging Store

ItdaStream's staging store provides write durability guarantees even before data is uploaded to S3, enabling automatic recovery from broker crashes.

## RocksDB Staging Store

- All records are persisted to local RocksDB before S3 upload
- Key format: {topic}-{partition}:{offset}
- Envelope encryption (AES-256-GCM) when staging.encrypt=true
- Encryption format: [WrappedDEK_Len][WrappedDEK][IV][EncryptedPayload]

## Recovery process (on broker startup)

1. Open staging store database
2. Iterate all unflushed records via recover(BiConsumer) callback
3. Decrypt each record using KMS (DEK unwrapped with KEK)
4. Re-upload to S3 and register in metadata store
5. Clean up staging entries after successful upload

Async cleanup: After successful S3 upload, staging entries are removed asynchronously via a dedicated executor, avoiding I/O blocking on the produce path.

