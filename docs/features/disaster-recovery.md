# Disaster Recovery

ItdaStream keeps the log **and everything needed to read it** in the bucket. Disaster recovery is
therefore replicating the bucket with the object store's own replication, and pointing a second
cluster at the replica. There is no MirrorMaker, no Connect cluster to run, and no offset
translation: offsets are the same numbers on both sides.

```
site A   primary cluster ──► bucket ──(ShannonStore bucket replication / S3 CRR / …)──► replica bucket
site B                                                                                     ▲
         standby cluster (its own ZooKeeper, cluster.mode=standby) ────────── reads ────────┘
```

## What is in the bucket

Under `<prefix>/_meta/`, next to the segments:

| Object | Holds |
|---|---|
| `segments/<topic>/<partition>/<base>.<last>.<size>.<ts>.json` | one registered segment (a listing alone rebuilds the segment list) |
| `topics/<topic>.json` | partition count and configs |
| `offsets/<group>.json` | every committed offset of a consumer group |
| `schemas/<id>.json` | schema registry entries |
| `streaming/jobs/<jobId>.json` | streaming job definitions |

ZooKeeper holds coordination only: broker registration, controller election, per-partition write
fences, streaming job assignment, group membership, transactions. Losing all of ZooKeeper loses no
data and no configuration — a cluster started against an empty ZooKeeper restores it from the
bucket.

!!! note "Upgrading from an earlier version"
    On the first start of this version one broker moves the segment list, committed offsets,
    topics, schemas and job definitions from ZooKeeper into the bucket, then removes ZooKeeper's
    segment and offset subtrees. Upgrade all brokers together; there is no rollback to a version
    that reads them from ZooKeeper. `itdastream.metadata.storage=zookeeper` keeps the old layout.

## Standby cluster

Start the recovery cluster on the replica bucket, with its own ZooKeeper, and:

```properties
itdastream.cluster.mode=standby
itdastream.standby.refresh.interval.ms=5000
```

A standby:

- loads topics, segment lists, schemas and job definitions from the replica, and re-reads it every
  refresh interval as replication brings more;
- serves reads — consumers and the console see the replicated data;
- changes nothing in the replica: produce is answered `POLICY_VIOLATION`; topic creation, deletion
  and partition changes, and offset commits are refused; retention and compaction do not run;
  streaming jobs are restored but not run (they would write their sinks a second time).

## Failover

1. **Stop replication into the replica** (in a real disaster the primary is gone anyway).
2. **Restore IAM and KMS** on the standby from the primary's [encrypted backup](backup.md) — they
   hold secrets and are not in the bucket — or create the users there.
3. **Promote**:

```bash
curl -X POST http://<standby-broker>:8082/admin/api/cluster/promote -H "Authorization: Bearer <admin token>"
curl         http://<standby-broker>:8082/admin/api/cluster/mode    -H "Authorization: Bearer <token>"
```

Promotion needs `kafka:PromoteCluster` (administrators). Every broker reads the replica one last
time and starts accepting writes; the write fences start after the replica's last segment, so no
offset is handed out twice. Streaming jobs start.

4. Point producers and consumers at the promoted cluster. Consumer groups resume from their
   committed offsets.

## What can be lost

| | Lost at failover |
|---|---|
| acks=all writes | what replication had not copied yet |
| acks=1 writes | the above, plus what the primary's brokers had not flushed ([Durability](durability.md)) |
| committed offsets | the above. An offset that replicated ahead of its data is answered as the log end, so the consumer resumes there rather than getting `OFFSET_OUT_OF_RANGE` |

Object-store replication copies objects independently and in no particular order. The standby lists
a segment only once its commit object has arrived, and a fetch that reaches a segment whose data has
not arrived yet stops there and re-reads the list, rather than skipping ahead.

## Verified

`tests/dr-e2e.sh`: a 3-broker primary on bucket `itdastream-data`, ShannonStore bucket replication
to `itdastream-dr`, a 3-broker standby on the replica.

| Step | Result |
|---|---|
| primary: 3,000 records (acks=all), a group commits after 1,000 | — |
| standby reads the replica | 3,000 of 3,000 |
| standby produce | refused (`PolicyViolationException`) |
| primary stopped, standby promoted | all three brokers active |
| group offsets on the promoted cluster | identical to the primary's |
| 1,000 more records on the promoted cluster, then read everything | 4,000 records, every one at its original offset, no offset reused |
| the group resumes | reads exactly the 3,000 it had not committed |
