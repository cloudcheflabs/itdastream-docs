# Crash Recovery with Staging Store

A produce request is acknowledged as soon as the record is in memory and in the local staging store. It has not reached object storage yet — that happens in the background, in batches, because uploading one object per record would make the write path as slow as the object store.

The staging store is what makes that acknowledgement honest. It is a local RocksDB write-ahead log holding every record that has been acked but not yet flushed, and on restart the broker replays it.

```text
   produce ──► WriteBuffer (memory) ──────────────┐
          └──► staging store (RocksDB)            │  background flush
                      ▲                           ▼
                      │                    ┌─────────────┐
                 replayed on restart       │ object store│
                      │                    └─────────────┘
                      └── entry deleted after the flush succeeds
```

## The write path

1. The record is appended to the partition's in-memory `WriteBuffer` and written to the staging store under `<topic>-<partition>/<offset>`.
2. The produce request is acknowledged.
3. A background worker flushes the buffer to object storage when it reaches a size or time threshold.
4. Staging entries for the flushed range are deleted **asynchronously**, so cleanup I/O never blocks a producer.

Records are enveloped with AES-256-GCM before they land in RocksDB when encryption is enabled, using the same DEK machinery as the object store — see [KMS](kms.md). The staging store is on the broker's local disk, so leaving records in the clear there would undo the encryption everywhere else.

## Recovery on restart

When the log store starts, it replays the staging store before accepting traffic:

- Entries are grouped by partition and replayed **in numeric offset order**. RocksDB sorts keys as bytes, which would otherwise give offsets 1, 10, 100, 2 — and replaying out of order would corrupt the partition.
- Each record's **original offset** is preserved. Re-appending at a fresh offset would renumber records a producer already holds acknowledgements for.
- Records at or below the partition's current `nextOffset` are **skipped** — they made it to object storage before the crash, and their staging entries were simply not cleaned up yet.
- Every partition touched by the replay is flushed synchronously before startup continues, so the recovered data is durable before the broker takes a single request.

Nothing about this depends on which broker crashed. The staging store is local, but the log it feeds is shared: a replayed record lands in the same object store every other broker reads from.

!!! warning "The staging directory is not disposable"
    `itdastream.storage.s3.staging.path` holds the only copy of records that have been acknowledged but not yet flushed. Wiping it between restarts — a fresh volume, a cleared container, a `rm -rf` during troubleshooting — discards exactly the records recovery exists to save. Give it a persistent volume.

## Shutdown

A clean shutdown flushes rather than relying on replay: it drains in-flight requests, then flushes every write buffer to object storage. Replay is the backstop for the unclean case — a kill, a crash, a lost node. See [Broker Drain](broker-drain.md) for the shutdown sequence.

## See also

- [Broker Drain](broker-drain.md) — the clean path, where nothing needs replaying.
- [Envelope Encryption and KMS](kms.md) — how staged records are enveloped.
- [Disaggregated Compute-Storage Architecture](disaggre-architecture.md) — why the log store is shared and the staging store is not.
