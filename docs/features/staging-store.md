# Crash Recovery with Staging Store

ItdaStream's staging store provides write durability guarantees before S3 upload, enabling automatic recovery from broker crashes.

- **Staging**: All records are persisted to local RocksDB before S3 upload, with optional envelope encryption (AES-256-GCM)
- **Recovery**: On broker restart, unflushed records are decrypted, re-uploaded to S3, and registered in metadata store
- **Async cleanup**: Staging entries are removed asynchronously after successful S3 upload, avoiding I/O blocking on the produce path
