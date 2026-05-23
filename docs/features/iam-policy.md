# IAM Policy Reference

ItdaStream's authorization layer accepts AWS IAM-style JSON policy documents. This page is the reference for writing those documents: the JSON schema, the resource ARN format, every supported action, and the evaluation rules.

For a high-level overview of the IAM subsystem (users, groups, access keys) see [AWS IAM-Compatible Access Control](iam.md).

---

## Policy Document Structure

A policy is a JSON object with a `Version` and a list of `Statement` objects.

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "AllowProduceToOrders",
      "Effect": "Allow",
      "Action": ["kafka:Produce", "kafka:Metadata"],
      "Resource": "arn:stream:kafka:topic:orders-*"
    }
  ]
}
```

| Field | Required | Description |
|-------|----------|-------------|
| `Version` | Yes | Policy language version. Use `"2012-10-17"`. |
| `Statement` | Yes | One or more statements. Each is evaluated independently and the overall decision is combined per the rules below. |
| `Statement[].Sid` | No | Free-form identifier for the statement. Useful for audit and diff. |
| `Statement[].Effect` | Yes | `"Allow"` or `"Deny"`. Case-insensitive. |
| `Statement[].Action` | Yes | A single action string or a list of action strings. Wildcards (`*`, `?`) supported. |
| `Statement[].Resource` | Yes | A single ARN/string or list. Wildcards (`*`, `?`) supported. |
| `Statement[].Condition` | No | Reserved. The field is accepted by the parser but **not evaluated** in the current release. |

---

## Resource ARN Format

Control-plane (Admin HTTP API) calls authorize against ARN-shaped resource strings:

```
arn:stream:kafka:<resource-type>:<id>
```

| Resource type | Example ARN | Guards |
|---------------|-------------|--------|
| `topic` | `arn:stream:kafka:topic:orders` | Topic-scoped admin operations |
| `cluster` | `arn:stream:kafka:cluster:cluster-1` | Cluster-wide operations (CreateTopic, DeleteTopic, UpdateTopic, Rebalance, Cleanup) |
| `iam` | `arn:stream:kafka:iam:*` | User/group/policy/password management |
| `kms` | `arn:stream:kafka:kms:*` | KMS key listing, creation, rotation |

The cluster id segment is the `cluster.id` configured on the broker.

### Data-plane resources are bare topic names

The Kafka wire-protocol handlers (Produce, Fetch, CreateTopics, DeleteTopics on the Kafka port) authorize against the **bare topic name**, not an ARN. This is intentional — the wire protocol carries topic names, and translating to an ARN at request time would force every client to know the cluster id.

| Action | Resource string actually checked |
|--------|----------------------------------|
| `kafka:Produce` | `orders` (the topic name) |
| `kafka:Fetch` | `orders` |
| `kafka:CreateTopic` (wire protocol) | `orders` |
| `kafka:DeleteTopic` (wire protocol) | `orders` |

A policy intended to cover both data-plane and control-plane access for a topic must therefore include **both** forms in `Resource`, or use a wildcard:

```json
{
  "Effect": "Allow",
  "Action": ["kafka:Produce", "kafka:Fetch"],
  "Resource": ["orders-*", "arn:stream:kafka:topic:orders-*"]
}
```

Or, the simplest catch-all:

```json
{ "Effect": "Allow", "Action": "kafka:Produce", "Resource": "*" }
```

---

## Action Catalog

All actions live under the `kafka:` namespace. Wildcards work at any depth: `kafka:*`, `kafka:Create*`, `kafka:?etch`.

### Data plane (Kafka wire protocol, port 9092)

| Action | Resource form | Enforced at |
|--------|---------------|-------------|
| `kafka:Produce` | bare topic name | `ProduceHandler` |
| `kafka:Fetch` | bare topic name | `FetchHandler` |
| `kafka:CreateTopic` | bare topic name | `CreateTopicsHandler` |
| `kafka:DeleteTopic` | bare topic name | `DeleteTopicsHandler` |

### Control plane — Topics (Admin HTTP API, port 8080)

| Action | Resource form |
|--------|---------------|
| `kafka:CreateTopic` | `arn:stream:kafka:cluster:<cluster-id>` |
| `kafka:DeleteTopic` | `arn:stream:kafka:cluster:<cluster-id>` |
| `kafka:UpdateTopic` | `arn:stream:kafka:cluster:<cluster-id>` |
| `kafka:Rebalance` | `arn:stream:kafka:cluster:<cluster-id>` |
| `kafka:Cleanup` | `arn:stream:kafka:cluster:<cluster-id>` |

### Control plane — IAM

| Action | Resource form |
|--------|---------------|
| `kafka:CreateUser` | `arn:stream:kafka:iam:*` |
| `kafka:DeleteUser` | `arn:stream:kafka:iam:*` |
| `kafka:UpdatePassword` | `arn:stream:kafka:iam:*` |
| `kafka:CreateGroup` | `arn:stream:kafka:iam:*` |
| `kafka:DeleteGroup` | `arn:stream:kafka:iam:*` |
| `kafka:CreatePolicy` | `arn:stream:kafka:iam:*` |
| `kafka:DeletePolicy` | `arn:stream:kafka:iam:*` |
| `kafka:UpdateIAM` | `arn:stream:kafka:iam:*` |

### Control plane — KMS

| Action | Resource form |
|--------|---------------|
| `kafka:ListKms` | `arn:stream:kafka:kms:*` |
| `kafka:CreateKey` | `arn:stream:kafka:kms:*` |
| `kafka:RotateKey` | `arn:stream:kafka:kms:*` |

### Defined but not yet enforced

These action names appear in policy templates and are reserved for future enforcement. They are not currently checked at request time, so policies using them are accepted but have no runtime effect: `kafka:Metadata`, `kafka:InitProducerId`, `kafka:JoinGroup`, `kafka:SyncGroup`, `kafka:Heartbeat`, `kafka:LeaveGroup`, `kafka:OffsetCommit`, `kafka:OffsetFetch`.

---

## Evaluation Rules

For each request, the broker collects every policy attached to every group the user belongs to, then iterates statements:

1. A statement applies only if **both** its `Action` and `Resource` patterns match the requested action and resource.
2. If any matching statement has `Effect: "Deny"`, the decision is **immediately DENY**.
3. Otherwise, if any matching statement has `Effect: "Allow"`, the decision is **ALLOW**.
4. If no statement matches, the decision is **ABSTAIN**, which the authorizer treats as **deny** (default-deny).

In short: **explicit Deny > explicit Allow > implicit Deny**.

### Wildcard matching

- `*` matches any sequence of characters (including empty).
- `?` matches exactly one character.
- Without `*` or `?`, the comparison is a case-sensitive exact match.
- Patterns are compiled once per process and cached (up to 10,000 entries).

---

## Defaults

On first startup of a fresh cluster the leader broker seeds:

| Object | Value |
|--------|-------|
| Policy | `AdministratorAccess` — `Action: "*"`, `Resource: "*"` |
| Group | `admin-group` — bound to `AdministratorAccess` |
| User | `admin` (password `admin`, must change on first login) — member of `admin-group` |

This is the only way to bootstrap; rotate the password immediately after first login.

---

## Worked Examples

### Full producer access to a single topic

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "OrdersProducer",
      "Effect": "Allow",
      "Action": ["kafka:Produce", "kafka:Metadata", "kafka:InitProducerId"],
      "Resource": ["orders", "arn:stream:kafka:topic:orders"]
    }
  ]
}
```

### Full consumer access to a topic family

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "OrdersConsumer",
      "Effect": "Allow",
      "Action": [
        "kafka:Fetch",
        "kafka:Metadata",
        "kafka:JoinGroup",
        "kafka:SyncGroup",
        "kafka:Heartbeat",
        "kafka:LeaveGroup",
        "kafka:OffsetCommit",
        "kafka:OffsetFetch"
      ],
      "Resource": ["orders-*", "arn:stream:kafka:topic:orders-*"]
    }
  ]
}
```

### Read-only admin operator

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "ReadOnlyAdmin",
      "Effect": "Allow",
      "Action": ["kafka:ListKms", "kafka:Metadata"],
      "Resource": "*"
    },
    {
      "Sid": "BlockMutations",
      "Effect": "Deny",
      "Action": [
        "kafka:CreateTopic", "kafka:DeleteTopic", "kafka:UpdateTopic",
        "kafka:CreateUser", "kafka:DeleteUser", "kafka:UpdatePassword",
        "kafka:CreateGroup", "kafka:DeleteGroup",
        "kafka:CreatePolicy", "kafka:DeletePolicy", "kafka:UpdateIAM",
        "kafka:CreateKey", "kafka:RotateKey",
        "kafka:Rebalance", "kafka:Cleanup"
      ],
      "Resource": "*"
    }
  ]
}
```

### Allow producing everywhere, deny one sensitive topic

Because explicit Deny wins, a broad Allow paired with a narrow Deny is the idiomatic way to carve out exceptions.

```json
{
  "Version": "2012-10-17",
  "Statement": [
    { "Sid": "ProduceAnywhere", "Effect": "Allow", "Action": "kafka:Produce", "Resource": "*" },
    { "Sid": "BlockPii", "Effect": "Deny", "Action": "kafka:Produce", "Resource": ["pii-*", "arn:stream:kafka:topic:pii-*"] }
  ]
}
```

---

## Limitations

- **`Condition` is parsed but not evaluated.** Authoring conditions today does nothing; the field exists for forward compatibility.
- **Anonymous requests** bypass policy evaluation only when authentication is disabled at the listener level; once SASL is enabled, every request is mapped to a user before authorization runs.
- **Resource form is asymmetric** between data plane (bare names) and control plane (ARNs). Use both forms in `Resource`, or use `"*"`, when a policy spans both surfaces.
