# AWS IAM-Compatible Access Control

ItdaStream implements an AWS IAM-compatible policy-based authorization system with users, groups, and JSON policies for fine-grained access control.

- **Policy format**: Standard AWS IAM JSON syntax with Effect/Action/Resource statements
- **Supported actions**: Data (Produce, Fetch), Topic management (Create, Delete), IAM operations, KMS operations
- **Policy evaluation**: Explicit Deny > Explicit Allow > Implicit Deny (default block); wildcard support
- **User model**: Users → Groups → Policies, with access key pairs and optional expiration

For the full policy JSON schema, ARN format, action catalog, and worked examples, see the [IAM Policy Reference](iam-policy.md).
