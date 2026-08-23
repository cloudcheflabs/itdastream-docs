# Iceberg Catalog Sessions and OAuth2 Token Renewal

Every Iceberg sink and every Iceberg read in ItdaStream talks to an **Iceberg REST catalog** —
Apache Polaris, a vanilla `iceberg-rest`/Nessie server, or the AWS Glue Iceberg REST endpoint.
That connection is long-lived: a streaming job stays up for weeks, while the OAuth2 token it
authenticates with typically lasts an hour. This page explains how the session authenticates,
how the token is renewed, what happens when a renewal fails, and how ItdaStream recovers.

It matters because the failure it prevents is silent and total: a catalog session that loses
its ability to renew keeps working until the current token expires, then fails **every**
catalog call from that moment on.

---

## How the session authenticates

The read path (`IcebergConnector`) and the write path (`IcebergConnectorSink`) build their
catalog properties from one shared builder, so the two can never drift apart — a mismatch
between them shows up as `createTable`/append hitting credential-vending 403s that a read-only
query never surfaces.

Two authentication flavors are supported, selected by `catalog.rest.flavor` on the connection:

=== "Polaris / OAuth2 (default)"

    The connection's client id and secret are combined into Iceberg's `credential` property and
    exchanged for a bearer token at the catalog's token endpoint using the OAuth2
    **client-credentials** grant:

    | Connection key | Iceberg property | Notes |
    |---|---|---|
    | `catalog.rest.client_id` + `catalog.rest.client_secret` | `credential` | Combined as `id:secret`. `catalog.rest.credential` also accepted. |
    | `catalog.rest.token` | `token` | A static, pre-issued token instead of a credential — see the warning below. |
    | `catalog.rest.scope` | `scope` | Defaults to `itdastream.iceberg.polaris.default.scope` (`PRINCIPAL_ROLE:ALL`) for the `polaris` flavor. |
    | `catalog.rest.oauth2.server.uri` | `oauth2-server-uri` | The token endpoint. Set it explicitly; the client's automatic fallback is deprecated upstream. |
    | `catalog.rest.prefix` | `prefix` | Defaults to the catalog name for `polaris`. Not sent for the `rest` flavor. |

=== "AWS Glue / SigV4"

    No OAuth2 at all: every catalog request is signed with AWS SigV4 through Iceberg's
    `RESTSigV4Signer`. Nothing on this page about token renewal applies — there is no token.

    | Connection key | Iceberg property |
    |---|---|
    | `catalog.rest.signing.name` | `rest.signing-name` (default `glue`) |
    | `catalog.rest.signing.region` | `rest.signing-region` |
    | `catalog.rest.aws.accessKey` / `.secretKey` / `.sessionToken` | `rest.access-key-id` / `rest.secret-access-key` / `rest.session-token` |

    When the AWS keys are omitted they fall back to the connection's `s3.*` keys, and when those
    are absent too, Iceberg uses the default AWS credentials provider chain (instance profile,
    environment, SSO) — the usual case under an IAM role.

---

## How the token is renewed

The Iceberg client schedules a background refresh at roughly 90% of the token's lifetime, and
renews with one of two OAuth2 grants:

| Grant | When it is used | What it needs |
|---|---|---|
| RFC 8693 **token exchange** | The default (`token-exchange-enabled=true`) | The current token as the subject token |
| **client_credentials** | When the exchange is disabled, and as the fallback for an already-expired token | The configured `credential` |

Both are exposed as switches, cluster-wide in `conf/itdastream.properties` and per connection:

| Cluster-wide | Per connection | Default |
|---|---|---|
| `itdastream.iceberg.catalog.oauth2.token.refresh.enabled` | `catalog.rest.oauth2.token-refresh-enabled` | `true` |
| `itdastream.iceberg.catalog.oauth2.token.exchange.enabled` | `catalog.rest.oauth2.token-exchange-enabled` | `true` |

Trino separates the same two switches, and for the same reason: RFC 8693 is optional, and an
identity provider that does not implement it fails every exchange while `client_credentials`
would have worked. **Turning the exchange off is the fix for that case, and only that case.**

!!! info "Measured against Apache Polaris 1.4.1"
    Polaris implements the token-exchange grant. It accepts the exchange with either Bearer or
    Basic authentication, accepts it even when the subject token has already expired, and
    issuing a new token does not invalidate the old one. Both switches therefore keep the
    Iceberg client's own defaults on ItdaStream — there is nothing to gain by deviating.
    Do not change them expecting to cure a token-expiry problem; the cause is below.

!!! warning "A static token with no credential cannot be recovered"
    A connection configured with `catalog.rest.token` but no client id/secret has nothing to
    re-authenticate with: Iceberg's expired-token path gives up immediately without a
    credential, and neither can ItdaStream sign in again. Once that token expires, every
    catalog call fails until the broker is restarted with a fresh one. The broker logs a
    warning about this at connection setup rather than letting you discover it an hour later.
    Configure `catalog.rest.client_id` / `catalog.rest.client_secret` instead.

---

## Why a single failed renewal used to be fatal

The Iceberg client re-arms the background refresh **only after a successful one**: its refresh
returns nothing on failure, and the scheduler re-schedules only on a non-empty result.

The consequence is out of proportion to the cause. One transient failure — the identity
provider restarting, a single 5xx, a network blip at the exact moment the refresh fires — ends
renewal permanently. Nothing retries it. The session keeps working with the token it already
holds, so nothing looks wrong, and then that token expires and every catalog call fails with
*"Not authorized"* until the process is restarted. No configuration prevents this, and no
choice of grant avoids it.

## How ItdaStream recovers

Catalog calls go through a session wrapper that treats an unauthorized response as a signal to
**sign in again and retry the call once**:

1. A catalog call comes back unauthorized.
2. The session re-authenticates — one token request against the catalog's token endpoint, using
   the connection's client credential.
3. Anything bound to the dead session is dropped, and the old session is closed.
4. The original call is retried exactly once against the new session.

The properties are what the session is rebuilt from, so re-authentication costs a single token
request and rebuilds nothing else: the S3 FileIO, open Parquet writers and buffered rows are
untouched, and an in-flight streaming checkpoint is not disturbed. Concurrent callers share one
sign-in rather than stampeding the token endpoint — the first thread to notice performs it and
the rest observe that it has already happened. If the freshly authenticated session is rejected
too, the rejection is real and propagates rather than looping.

Recovery is applied at the funnels every operation passes through — loading a table, checking
existence, listing tables and listing namespaces — which covers scans, DML (`UPDATE`, `DELETE`,
`MERGE`), table maintenance, WAP publishes and streaming commits alike, since all of them load
the table first.

You will see this in the broker log as:

```text
WARN  IcebergCatalogSession - Iceberg catalog session was rejected while loading db.events
      (Not authorized); signing in again and retrying
INFO  IcebergCatalogSession - Iceberg catalog 'prod-iceberg' session re-authenticated (generation 1)
```

A rising `generation` on a healthy cluster is worth investigating — it means renewals are
failing repeatedly at the identity provider — but the job keeps running either way.

---

## Related

- [Connection Registry](connections.md) — where the catalog credentials are stored
- [Iceberg Write-Audit-Publish](iceberg-wap.md) — branch-staged writes and publishing
- [Flexible Configuration System](configuration.md#iceberg-connector-settings) — the cluster-wide defaults
- [Streaming Operations & Tuning](streaming-operations.md)
