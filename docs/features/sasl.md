# SASL Authentication and TLS Encryption

ItdaStream authenticates Kafka clients over SASL and encrypts the listener with
TLS, so the security protocols every Kafka client already knows —
`SASL_PLAINTEXT`, `SSL`, `SASL_SSL` — all work unchanged.

## Mechanisms

| Mechanism | Credential | Notes |
| --- | --- | --- |
| **SASL/PLAIN** | Username = Access Key, Password = Secret Key | The IAM credential pair. `POST /admin/iam/keys` issues one. |
| **SASL/PLAIN** | Username = login name, Password = directory password or OIDC ID token | Single sign-on for clients that speak only PLAIN. See [Single Sign-On](sso.md). |
| **SASL/OAUTHBEARER** | An OIDC ID token, supplied through the client's standard token callback | Single sign-on the way Kafka clients already do it. Advertised only while an identity provider is configured. |

Enable it with `itdastream.sasl.enabled=true`. With SASL off, the listener is
unauthenticated.

```properties
# Access key and secret key — the credential this cluster issues
security.protocol=SASL_PLAINTEXT
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required \
    username="AKIA…" password="…";
```

The handshake is the standard Kafka one: `SaslHandshake` names the mechanism, then
`SaslAuthenticate` carries the exchange. Nothing about the client is special, and
which mechanisms a broker will accept can be read from its handshake response.

!!! note "Why OAUTHBEARER appears and disappears"
    A Kafka client that has chosen a mechanism does not fall back, so advertising
    OAUTHBEARER with no identity provider behind it would have clients select it
    and then fail at the exchange — with nothing to say the mechanism was never
    usable there. The broker therefore offers it only while OIDC, SAML or LDAP is
    enabled.

## Authorization is separate

Authenticating says who the caller is; the IAM policies attached to their groups
decide what they may do. That is the same evaluation for an access key and for a
federated identity — see [AWS IAM-Compatible Access Control](iam.md) and
[IAM Policy Reference](iam-policy.md).

## TLS

- Non-blocking TLS through Java's `SSLEngine`, so an encrypted connection costs no
  thread of its own.
- `TLSv1.3` by default; `itdastream.ssl.protocol` selects another version.
- JKS keystore and truststore, the latter only for mutual TLS.
- A `TransportLayer` abstraction keeps it invisible above the socket:
  `PlaintextTransportLayer` does direct I/O and `SslTransportLayer` does the full
  handshake, and nothing upstream of them knows which is in use.

```properties
itdastream.ssl.enabled=true
itdastream.ssl.protocol=TLSv1.3
itdastream.ssl.keystore.path=/etc/itdastream/server.jks
itdastream.ssl.keystore.password=…
itdastream.ssl.key.password=…
# Only for mutual TLS
#itdastream.ssl.truststore.path=/etc/itdastream/clients.jks
#itdastream.ssl.truststore.password=…
```

Combine the two for `SASL_SSL`: `itdastream.sasl.enabled=true` plus
`itdastream.ssl.enabled=true`. A credential sent over a plaintext listener crosses
the network in the clear — including a directory password, which is a credential
for systems other than this one.

## See also

- [Single Sign-On](sso.md) — OIDC, SAML and LDAP, on this plane and in the console.
- [Configuration](configuration.md) — every setting, with its default.
