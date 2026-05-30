# Backup & Restore

ItdaStream can periodically ship its irreplaceable cluster state — the **KMS keystore** and the **IAM** snapshot — to an S3-compatible target. Backups are leader-only, incremental, opt-in, and run on either a fixed interval, a 5-field UNIX cron expression, or both at once.

The Kafka log data itself is **not** in scope here: it already lives in the configured S3 backing store (whichever bucket the broker uses for segments), so it is a target for the same `aws s3 sync`-style data export tools you would use against any object store. What this feature backs up is the *control-plane* state without which a restored broker has no users, no encryption keys, and no audit history.

## Configuration

The `Backup & Restore` page in the admin UI exposes the full configuration:

- **Enabled** — master switch (default: off).
- **S3 Endpoint, Region, Bucket, Prefix** — destination object store.
- **Access Key / Secret Key** — long-lived static credentials for the destination.
- **Path-style addressing** — required for ShannonStore / MinIO destinations.
- **Interval (minutes)** — how often the scheduled backup runs.
- **Cron** — optional 5-field UNIX cron expression (e.g. `0 2 * * *` for daily at 02:00). Coexists with the interval rule — both can be set, whichever comes due first fires the backup.
- **Retention (days)** — how long the visible history is kept.

The same page has a **Backup Now** button (triggers an immediate run) and a **Restore** button on each row of the available-backups list.

### Cron vs. Interval

The two automatic rules are independent and share the same tick loop on the leader broker:

- **Interval** is a relative "at least N minutes since the last successful backup" timer.
- **Cron** is wall-clock based, evaluated in the leader's local time zone.

The first sighting after the cron is set, or after a leader handoff, **arms from "now"** — ItdaStream does not back-fire missed cron times on startup. An invalid cron is rejected with HTTP 400 at config-save time.

## Incremental by Design

Each backup writes a `manifest.json` under `s3://<bucket>/<prefix>/<backupId>/`, plus a content-addressed blob per snapshotted store. SHA-256 of the bytes is the key: if a store's hash matches the previous run, the manifest entry points at the prior backup id and no PUT happens for that store. A re-run with no cluster activity uploads only the new manifest.

A backup id is a millisecond timestamp + short UUID — so the visible list sorts newest-first by string compare.

## Restore

The available-backups list shows every backup id present at the configured prefix, newest first. **Restore** is destructive — it overwrites the live KMS and IAM stores with whichever snapshot the chosen backup id points to (possibly an older backup id, per the incremental layout). The admin UI confirms before proceeding.

Restore is leader-only — followers see the broadcast KMS/IAM state through the existing sync protocols.

## REST Endpoints

| Method | Path | Purpose |
|---|---|---|
| `GET`  | `/admin/api/backup/config`   | Read current config (secret never echoed) |
| `POST` | `/admin/api/backup/config`   | Update; sticky secret (only overwrites on non-empty) |
| `POST` | `/admin/api/backup/run`      | Trigger an immediate backup |
| `GET`  | `/admin/api/backup/history`  | Last 100 history entries |
| `GET`  | `/admin/api/backup/list`     | Backup ids in S3, newest first |
| `POST` | `/admin/api/backup/restore`  | Restore from `{backupId}` |

All mutating routes (`POST` config / run / restore) are leader-only and return 401/403 without an admin token.

You can send them to **any** broker, though: a mutating backup request that lands on a follower is transparently proxied to the current leader (controller), executed there, and the leader's response is relayed back. So an admin UI pointed at a follower still triggers the backup on the leader rather than failing. Read routes (`GET` config / history / list) are served locally on whichever broker receives them.
