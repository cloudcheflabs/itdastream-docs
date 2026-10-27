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

## Backups Are Encrypted

The IAM snapshot holds every access key's secret key and `ITOK` token, the password hashes and the SSO secrets (OIDC client secret, LDAP bind password, SAML SP private key). It is therefore never written in the clear:

| Object | Contents | Protection |
|---|---|---|
| `kms.bin` | KMS keystore | Wrapped with the cluster master key (`ITDASTREAM_MASTER_KEY`) |
| `iam.bin` | IAM snapshot (users, groups, policies, access keys, SSO settings) | KMS envelope encryption with the default KMS key (`itdastream.kms.default.key.id`); the blob starts with the marker `ITDSBAK1` and the manifest entry says `"format": "kms-envelope-v1"` |
| `manifest.json` | Backup id, store hashes, which backup id holds each blob | No secrets |

The **backup configuration is not part of the backup** — it holds the backup bucket's own secret key, which must never be written into the bucket it protects.

A backup needs an initialized KMS: without one the run is refused (status `ERROR`, "KMS is not initialized") instead of writing the IAM state unencrypted. To restore a backup on a rebuilt cluster you need the same master key the backed-up cluster used, because `kms.bin` — and through it `iam.bin` — can only be opened with it.

## Incremental by Design

Each backup writes a `manifest.json` under `s3://<bucket>/<prefix>/<backupId>/`, plus a content-addressed blob per snapshotted store. SHA-256 of the store's plaintext is the key (the encrypted bytes differ on every run): if a store's hash matches the previous run, the manifest entry points at the prior backup id and no PUT happens for that store. A re-run with no cluster activity uploads only the new manifest.

A backup id is a millisecond timestamp + short UUID — so the visible list sorts newest-first by string compare.

## Restore

The available-backups list shows every backup id present at the configured prefix, newest first. **Restore** is destructive — it overwrites the live KMS and IAM stores with whichever snapshot the chosen backup id points to (possibly an older backup id, per the incremental layout). The admin UI confirms before proceeding.

The KMS keystore is restored first, then the IAM snapshot is decrypted with it. Restore is leader-only: the leader imports the snapshot, persists it and broadcasts it, so every follower serves the restored KMS keys and IAM state right away (and the leader keeps it across a restart). The backup configuration in force at restore time is kept — restoring an old snapshot does not point future backups at an old bucket or old credentials.

### Backups from older versions

Before this change `iam.bin` was written as **plaintext JSON**, and it also carried the backup configuration. Restore still accepts such a backup (bytes starting with `{`) and logs a warning, so existing backups keep working. But anyone who could read the backup bucket could read those objects, so if you have backups taken by an older version:

1. **Rotate the access keys and `ITOK` tokens** of every IAM user (create a new key, switch clients over, delete the old one).
2. **Rotate the SSO secrets** — the OIDC client secret at the identity provider, the LDAP bind password, and the SAML SP key pair — and save the new values in the Admin UI.
3. **Rotate the backup bucket's access key / secret key** at the object store and save the new pair on the Backup & Restore page.
4. Users' passwords were only stored as PBKDF2 hashes, but consider asking administrators to change theirs.
5. Take a new backup (**Backup Now**), check that the new `iam.bin` starts with `ITDSBAK1`, then **delete the old `iam.bin` objects** (and the backup ids that point at them) from the bucket.

## Where the configuration lives

The configuration (including the S3 secret) is stored in the replicated IAM snapshot, which is encrypted at rest with the master key and reaches every broker. Any broker therefore shows the same configuration, and a newly elected leader keeps backing up after a failover. A configuration saved by an older version in the per-broker plaintext `backup-config.json` file is moved into the replicated store by the leader and the file is deleted; once the replicated configuration exists, every broker deletes its own leftover copy within a minute. The file is never written again.

## REST Endpoints

| Method | Path | Purpose |
|---|---|---|
| `GET`  | `/admin/api/backup/config`   | Read current config (secret never echoed) |
| `POST` | `/admin/api/backup/config`   | Update; sticky secret (only overwrites on non-empty) |
| `POST` | `/admin/api/backup/run`      | Trigger an immediate backup |
| `GET`  | `/admin/api/backup/history`  | Last 100 history entries |
| `GET`  | `/admin/api/backup/list`     | Backup ids in S3, newest first |
| `POST` | `/admin/api/backup/restore`  | Restore from `{backupId}` |

Every route needs `kafka:ManageBackup` on `arn:stream:kafka:cluster:<cluster-id>` (administrators); without it the API answers `403`. The run history (including the error of a failed run, shown under its status in the Admin UI) is kept on the leader.

You can send them to **any** broker, though: a mutating backup request that lands on a follower is transparently proxied to the current leader (controller), executed there, and the leader's response is relayed back. So an admin UI pointed at a follower still triggers the backup on the leader rather than failing. Read routes (`GET` config / history / list) are forwarded to the leader as well, so every broker shows the same configuration, history and backup list.
