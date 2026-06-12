# AWS IAM-Compatible Access Control

ItdaStream implements an AWS IAM-compatible policy-based authorization system with users, groups, and JSON policies for fine-grained access control.

- **Policy format**: Standard AWS IAM JSON syntax with Effect/Action/Resource statements
- **Supported actions**: Data (Produce, Fetch), Topic management (Create, Delete), IAM operations, KMS operations
- **Policy evaluation**: Explicit Deny > Explicit Allow > Implicit Deny (default block); wildcard support
- **User model**: Users → Groups → Policies, with access key pairs and optional expiration
- **Credentials**: each access key carries three values — an **access key**, a **secret key**,
  and a long-lived **user token** (`ITOK...`) — generated together

For the full policy JSON schema, ARN format, action catalog, and worked examples, see the [IAM Policy Reference](iam-policy.md).

## Programmatic credentials (access key / secret key / user token)

Creating an access key for a user (Admin UI → IAM, or `POST /admin/iam/keys`) returns three
values at once:

```json
{ "accessKey": "AKIA...", "secretKey": "...", "token": "ITOK..." }
```

| Credential | Used for |
|---|---|
| access key + secret key | Kafka **SASL/PLAIN** authentication (producers/consumers) |
| user token (`ITOK...`) | programmatic admin/SDK auth via the `Authorization: Token <token>` header |

The secret key and user token are shown **once** (a one-time reveal in the UI, and in the
downloadable credentials CSV) — store them securely. The user token is the recommended
credential for the [Streaming SDK](streaming-sdk.md) and CI:

```java
ItdaStreamSession.builder().adminUrl("http://broker:8082").userToken("ITOK...").build();
```

Tokens inherit the user's group policies and honor the access key's `Active` status and optional
expiration; deleting the access key revokes the token.
