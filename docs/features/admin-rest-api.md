# Admin REST API

Every broker serves the admin API on its admin port (`itdastream.admin.ui.port`, default `8080`) under `/admin/`. The Admin UI uses exactly this API, so anything the console does can be scripted. Send requests to **any** broker: writes that must run on the leader are forwarded to it, and the broker you called catches up with the leader before it answers (read-your-writes).

## Authentication

| Header | Credential | Obtained from |
|---|---|---|
| `Authorization: Bearer <jwt>` | Console session (24 h by default) | `POST /admin/login` |
| `Authorization: Token <ITOK...>` | Long-lived user token | *Generate Key* on the Identity Control page, or `POST /admin/iam/keys` |

- `POST /admin/login` `{"username","password"}` → `{"status":"success","userId","token","isAdmin","requirePasswordChange"}`. While an account still has its initial password the response has `requirePasswordChange: true` and **no token**; change it with `POST /admin/change-password` `{"username","oldPassword","newPassword"}` (authenticated by the old password) and log in again.
- `POST /admin/logout` revokes the bearer token on every broker (the revocation rides the replicated IAM snapshot until the token would have expired).
- `GET /admin/me` → `{"userId","isAdmin"}`.
- A token stops working when its user is deleted; an ITOK token stops working when its key is revoked or expires.
- Until a user has changed their initial password, every API call (including with their ITOK token) answers `403` with `requirePasswordChange: true`.

Public endpoints (no token): `POST /admin/login`, `POST /admin/change-password`, `/admin/auth/sso/*`, `GET /admin/health`, `GET /metrics` (Prometheus).

## Authorization

Every call is checked against the caller's IAM policies (`authorize(user, action, resource)`; see the [IAM Policy Reference](iam-policy.md)). The console hides buttons from non-administrators, but the check is on the server: a missing permission answers `403 {"status":"error","message":"Forbidden: <action> is not allowed for <user>"}`. `CLUSTER` below stands for `arn:stream:kafka:cluster:<cluster-id>`.

Malformed requests (invalid JSON, a missing or mistyped field, an invalid name, a duplicate) answer `400` with the reason instead of `500`.

## Endpoints

### Cluster and monitoring

| Method | Path | Permission | Notes |
|---|---|---|---|
| GET | `/admin/nodes` | signed in | Brokers with host, ports, draining flag |
| GET | `/admin/zookeeper` | signed in | Ensemble status and this broker's session |
| GET | `/admin/monitoring/metrics?startTs&endTs` | signed in | Per-broker metric samples (served by the leader) |
| GET | `/admin/cluster/metrics`, `/admin/metrics` | signed in | Current metrics |
| GET | `/admin/logs/tail?lines&offset&targetHost&targetPort` | `kafka:ReadBrokerLogs` on CLUSTER | `targetHost`/`targetPort` must be a registered broker's admin endpoint |
| GET / POST | `/admin/maintenance/drain` | POST: `kafka:Drain` | `{"brokerId","draining"}` |
| POST | `/admin/maintenance/logstore/rebalance` | `kafka:Rebalance` | |
| POST | `/admin/maintenance/cleanup/trigger` | `kafka:Cleanup` | |

### Topics

| Method | Path | Permission | Notes |
|---|---|---|---|
| GET | `/admin/topics`, `/admin/topics/sizes`, `/admin/topics/details/{topic}` | signed in | |
| POST | `/admin/topics` | `kafka:CreateTopic` | `{"topic","numPartitions"}`; name `[a-zA-Z0-9._-]{1,249}`, partitions ≥ 1; existing name → 400 |
| POST | `/admin/topics/partitions` | `kafka:UpdateTopic` | `{"topic","numPartitions"}`; increase only |
| DELETE | `/admin/topics/{topic}` | `kafka:DeleteTopic` | Unknown topic → 404 |

### Log storage

| Method | Path | Permission |
|---|---|---|
| GET | `/admin/browser/buckets` | `kafka:BrowseStorage` on CLUSTER |
| GET | `/admin/browser/objects?prefix=` | `kafka:BrowseStorage` on CLUSTER |

### Connections and streaming

| Method | Path | Permission | Notes |
|---|---|---|---|
| GET | `/admin/connections` | signed in | Secret-looking properties are returned as `***` |
| POST / PUT / DELETE | `/admin/connections[/{id}]`, `POST /admin/connections/test` | `kafka:ManageConnections` | PUT keeps a property sent as `***` |
| GET | `/admin/streaming/jobs[/{id}]` | signed in | Inline secrets in a job spec are returned as `***` |
| POST | `/admin/streaming/jobs` | `kafka:SubmitStreamingJob` | Also used by `bin/submit.sh` with an ITOK token |
| DELETE | `/admin/streaming/jobs/{id}` | `kafka:KillStreamingJob` | Returns `{"status":"ok","stopped":true}` once every broker stopped the job (waits up to `itdastream.admin.streaming.kill.wait.ms`) |
| GET | `/admin/streaming/jobs/{id}/log`, `/admin/streaming/joblogs` | signed in | |
| GET / POST | `/admin/deps` | POST: `kafka:UploadDependency` | Job dependency JARs |
| POST | `/admin/iceberg/publish` | `kafka:PublishIceberg` | WAP fast-forward / cherry-pick |
| GET | `/admin/schemas/subjects`, `.../{subject}/latest`, `/admin/schemas/ids/{id}` | signed in | |
| POST | `/admin/schemas/subjects/{subject}/versions` | `kafka:RegisterSchema` | |

### IAM

| Method | Path | Permission | Notes |
|---|---|---|---|
| GET | `/admin/iam/users`, `/groups`, `/policies` | signed in | Secret keys masked |
| POST | `/admin/iam/users` | `kafka:CreateUser` | Existing id → 400 (never overwritten); password ≥ 6 |
| DELETE | `/admin/iam/users/{id}` | `kafka:DeleteUser` | Cannot delete yourself; invalidates the user's tokens |
| DELETE | `/admin/iam/users/{id}/keys/{accessKey}` | `kafka:DeleteUser` | Revokes the key and its token |
| POST | `/admin/iam/groups` / DELETE `/admin/iam/groups/{name}` | `kafka:CreateGroup` / `kafka:DeleteGroup` | Existing group → 400 |
| POST | `/admin/iam/policies` / DELETE `/admin/iam/policies/{name}` | `kafka:CreatePolicy` / `kafka:DeletePolicy` | Invalid document → 400; `AdministratorAccess` cannot be deleted |
| POST | `/admin/iam/add-user-to-group`, `remove-user-from-group`, `attach-group-policy`, `detach-group-policy` | `kafka:UpdateIAM` | Unknown user / group / policy → 400 |
| POST | `/admin/iam/keys` | `kafka:CreateKey` | Returns access key, secret key and ITOK token once |
| GET | `/admin/iam/download-key/{user}/{accessKey}` | the key's owner, or `kafka:CreateKey` | CSV; a key under the wrong owner → 404 |

### KMS, SSO, backup

| Method | Path | Permission |
|---|---|---|
| GET | `/admin/kms/list`, `/admin/kms/status/{keyId}` | `kafka:ListKms` on `arn:stream:kafka:kms:*` |
| POST | `/admin/kms/create`, `/admin/kms/rotate/{keyId}` | `kafka:CreateKey` / `kafka:RotateKey` |
| GET | `/admin/sso/settings`, `/admin/sso/saml/metadata` | signed in (secrets reported only as set / not set) |
| PUT / POST | `/admin/sso/settings`, `/admin/sso/test`, `/admin/sso/saml/keypair`, `/admin/sso/saml/idp-metadata` | `kafka:ManageSso` on `arn:stream:kafka:iam:*` |
| GET / POST | `/admin/api/backup/config`, `/run`, `/history`, `/list`, `/restore` | `kafka:ManageBackup` on CLUSTER |

## Static files

The console bundle is served from `itdastream.admin.ui.static.path`. Paths are resolved and must stay inside that directory; anything else answers `404`. A browser navigation to a console route (for example a reload of `/admin/topics`) is answered with the console itself, while API calls on the same path return JSON.
