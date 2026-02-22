# AWS IAM-Compatible Access Control

ItdaStream implements an AWS IAM-compatible policy-based authorization system with users, groups, and JSON policies. This provides fine-grained access control for all Kafka operations and
administrative actions.

## Policy format (AWS IAM JSON syntax)

```
{
"Version": "2012-10-17",
"Statement": [{
"Effect": "Allow",
"Action": ["kafka:Produce", "kafka:Fetch"],
"Resource": "arn:itdastream:kafka:*:topic/orders-*"
}]
}
```


## Supported actions

- Data operations: kafka:Produce, kafka:Fetch
- Topic management: kafka:CreateTopic, kafka:DeleteTopic
- IAM operations: kafka:CreateUser, kafka:DeleteUser, kafka:CreateGroup, kafka:DeleteGroup, kafka:CreatePolicy, kafka:DeletePolicy, kafka:UpdateIAM
- KMS operations: kafka:CreateKey, kafka:RotateKey, kafka:ListKms

## Policy evaluation

- Explicit Deny takes highest priority
- Explicit Allow grants access if action and resource match
- Implicit Deny (no matching statement) blocks access by default
- Wildcard support: * matches all, kafka:* matches all Kafka actions, arn:itdastream:kafka:*:* matches all resources

## User management

- Users belong to groups; groups have attached policies
- Access key pairs (16-char access key + 32-char secret key) with optional expiration
- Default setup: "admin" user in "admin-group" with "AdministratorAccess" policy (Allow *)