# Durability: what an Acknowledgement Means

A produce is acknowledged at the point the producer's `acks` asks for. What survives what follows
from that.

| `acks` | Acknowledged after | Survives |
|---|---|---|
| `0` | nothing — no response is sent | nothing is promised |
| `1` | the batch is in the broker's [staging store](staging-store.md) and its WAL is fsynced | a broker crash, a power loss, a kernel panic (the broker replays it on restart) |
| `all` (`-1`) | the batch is in object storage and registered in the segment list | anything short of losing the bucket: the broker, its disk, the whole node |

Kafka clients default to `acks=all` (and `enable.idempotence=true`) since 3.0.

## acks=all

An acks=all batch waits for its partition to be flushed to object storage. The broker does not
wait for the size or time threshold: a waiting produce triggers a flush after
`itdastream.produce.acks.all.linger.ms` (default 50 ms), which collects every batch that arrives
for the partition meanwhile into one upload. The handler thread is released while the upload runs;
the response is sent when it completes, in request order on the connection.

If the upload fails, it is retried (the same segment, under the same object keys) until it succeeds
or the producer's request timeout passes — then the producer gets `REQUEST_TIMED_OUT` and retries.

Measured on a 3-broker cluster on ShannonStore, 500 records/s: acks=1 p50 6 ms / p99 33 ms,
acks=all p50 60 ms / p99 150 ms.

## acks=1

The batch is staged under the offsets the broker assigns it, and the staging store's WAL is fsynced
before the acknowledgement. Concurrent producers share one fsync (group commit). A staging failure
is answered `KAFKA_STORAGE_ERROR` — the producer retries — rather than acknowledging a batch that is
only in memory.

The batch reaches object storage at the next flush: when the segment reaches
`itdastream.storage.segment.max.size` (16 MiB), or when its oldest record is
`itdastream.storage.s3.flush.timeout.ms` old (default 5 minutes). Until then it is on this broker's
disk only. Lose the disk, and it is lost.

| Setting | Default | |
|---|---|---|
| `itdastream.storage.s3.staging.sync` | `true` | fsync the staging WAL before an acks=1 acknowledgement. `false` survives a process crash but not a power loss |
| `itdastream.storage.s3.flush.timeout.ms` | `300000` | maximum age of an unflushed record — the acks=1 exposure window |
| `itdastream.produce.acks.all.linger.ms` | `50` | how long an acks=all flush waits to collect more batches |

## When partition leadership moves

A partition is led by the broker at its position in the serving-broker list, so leadership moves
when a broker joins, leaves or starts draining. Three things keep that from losing data:

- **Produce goes to the leader only.** Any other broker answers `NOT_LEADER_OR_FOLLOWER`, and the
  client refreshes its metadata. A broker that is still starting up is left out of the metadata.
- **Registration is a compare-and-set.** If the old and the new leader both hold records for the
  same offsets, the first to register its segment wins; the other moves its records past the
  winner's end (rewriting their offsets) and registers them there. Nothing is overwritten. The
  acks=all response carries the final offsets.
- **The old leader flushes at once.** When the serving brokers change, every broker flushes what it
  holds instead of waiting for the flush timeout.

A broker that comes back after a crash replays its staging store. Records another broker has since
written over (it led the partition meanwhile) are appended again after that broker's records, with
new offsets, instead of being dropped.

The consequence for acks=1: an acknowledged record can end up at a different offset than the one
it was acknowledged with, if leadership moved before it was flushed. With acks=all the offset in the
acknowledgement is final.

## Verified

`tests/durability-e2e.sh`: every produced value is unique, every acknowledgement is recorded, and
the topic is read back and compared exactly.

| Scenario | Result |
|---|---|
| acks=all, a broker's container and volume removed mid-stream (300 records/s) | 16,456 acknowledged, 0 missing, every offset as acknowledged |
| acks=1, all three brokers killed -9 | 3,000 acknowledged, 0 missing (replayed from staging) |
| acks=1, one broker killed holding unflushed records; others write the same offsets; it returns | 5,000 acknowledged, 0 missing (981 moved past the other writes) |
| acks=0 | completes; the connection is not blocked |
| ZooKeeper lost entirely and recreated empty | topics, data and group offsets back from the bucket |

## See also

- [Crash Recovery with Staging Store](staging-store.md)
- [Disaster Recovery](disaster-recovery.md)
