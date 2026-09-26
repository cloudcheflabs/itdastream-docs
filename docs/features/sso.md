# Single Sign-On (OIDC, SAML, LDAP)

ItdaStream can hand authentication to an external identity provider, so the
cluster stops being another place that holds passwords and becomes another thing
your directory governs. Three providers are supported — OpenID Connect, SAML 2.0,
and LDAP / Active Directory — and all three work on both surfaces.

## SSO means two different things here

This is the part most integrations get wrong, so it is worth being precise before
any configuration.

**The admin console is a browser.** It can be redirected, so it uses the flows
built for that: OIDC Authorization Code with PKCE, or SAML 2.0 Web Browser SSO.

**The Kafka protocol is not a browser — but it has SASL.** So SSO on the data
plane *is* SASL, and there are two mechanisms:

| Mechanism | Credential | When to use it |
| --- | --- | --- |
| **SASL/OAUTHBEARER** | An OIDC ID token, supplied by the application | The path to prefer. Every mainstream Kafka client already ships this mechanism for token authentication, so an SSO deployment needs configuration and no custom client code. |
| **SASL/PLAIN** | An ID token or a directory password in the password field | For clients that speak only PLAIN, which is most of the older ones. |

```properties
# OAUTHBEARER — the client asks the application for the token through the
# standard callback, which is the integration point an application with an
# identity provider already has.
security.protocol=SASL_PLAINTEXT
sasl.mechanism=OAUTHBEARER
sasl.jaas.config=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required;
sasl.login.callback.handler.class=com.example.MyTokenHandler

# PLAIN — the directory password, for a client that has no OAUTHBEARER support
security.protocol=SASL_PLAINTEXT
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required \
    username="alice" password="<directory password, or an ID token>";
```

!!! note "OAUTHBEARER is advertised only when a provider is configured"
    A Kafka client that has chosen a mechanism does not fall back, so a broker
    advertising OAUTHBEARER without an identity provider behind it would have
    clients select it and then fail at the exchange — with nothing to say the
    mechanism was never usable there. The SASL handshake therefore offers
    OAUTHBEARER only while OIDC, SAML or LDAP is enabled.

!!! warning "A SAML assertion is not a Kafka credential"
    It is refused on the Kafka plane, deliberately. An assertion may be used once
    — that is what makes replay protection possible — while a Kafka client
    authenticates on **every connection it opens**: one to bootstrap, and one to
    each broker it needs. So an assertion would work on the first connection and
    fail on the next, which is the worst of both. The broker says so and names
    what to use instead: OAUTHBEARER with an ID token, or directory credentials.
    The console, which authenticates once, accepts assertions normally.

## What decides permissions

Nothing about the provider does. A federated identity arrives carrying group
names; those are mapped to groups **on this cluster**, and the policies attached
to those groups authorize every produce and fetch. The evaluation is the same code
path as an access key — only where the group list came from differs.

No local user account is created. A federated caller has no password and nothing
to persist; your directory is the record. Creating an account for everyone who
ever connected would put them all in the IAM list and in every replicated
snapshot, and removing someone from the directory would leave the copy behind
still authorizing them.

### Where a federated session lives, and why it differs by plane

- **A Kafka session is not replicated between brokers.** Each connection
  authenticates against the broker it lands on, so that broker is also the one
  that authorizes it. Replicating one record per connection would put client churn
  into the IAM broadcast for no benefit.
- **A console session is replicated**, because an admin call may be proxied to the
  leader and the leader is what resolves it.

Both expire after `itdastream.sso.federated.session.seconds` (default 3600).

### Group mapping

```properties
itdastream.sso.group.mappings=db-admins:itda-admins,analysts:itda-readers
```

Left empty, provider group names are used as they are — the common case where the
directory already uses this product's group names. **Once set, the mapping is
exhaustive:** a group not named in it is dropped, so creating a group at the
provider cannot grant access here by itself.

An identity whose groups all map to nothing is refused, not admitted with no
groups. Such a session has no policies and is denied every action, so letting it
in produces a client that connects and then fails every produce and fetch — which
reads as a broker problem. Set `itdastream.sso.allow.unmapped.groups=true` if you
would rather allow it.

The directory login endpoint reports that case as **403**, separately from a wrong
password's 401 — otherwise an operator goes and resets a password that was right.

## Configuring it

Everything below can be set in the **admin console under Single Sign-On**, which
stores it with the IAM state and applies it on every broker — no file edits, no
restart. The properties file is still read for anything left unset, so a cluster
configured by file keeps working untouched.

Local passwords keep working while SSO is on. Enabling it cannot lock you out.

### OpenID Connect

```properties
itdastream.sso.oidc.enabled=true
itdastream.sso.oidc.issuer=https://keycloak.example.com/realms/company
itdastream.sso.oidc.client.id=itdastream-console
itdastream.sso.oidc.client.secret=…
itdastream.sso.oidc.redirect.uri=https://itda.example.com/admin/auth/sso/oidc/callback
itdastream.sso.oidc.groups.claim=groups
```

Endpoints are read from the issuer's discovery document, so they are not
configured individually. The ID token's signature is verified against the
provider's published key set, and its issuer, audience and expiry are all checked
— a token issued for a different application is refused even though it is genuine
and correctly signed.

!!! note "The `groups` scope"
    `itdastream.sso.oidc.scopes` deliberately does **not** include `groups`. It is
    not a standard scope, and a provider that does not define it rejects the whole
    authorization request with `invalid_scope` — so asking for it by default logs
    nobody in. Group membership comes from a claim the provider is configured to
    include.

### SAML 2.0

A SAML integration is an exchange of metadata, not a form-filling exercise.

1. **Download this cluster's metadata** from the console (or
   `GET /admin/sso/saml/metadata`) and give it to whoever administers your
   identity provider.
2. **Paste your provider's metadata** into the console. The entity ID, sign-on URL
   and signing certificate are read from it — which beats transcribing three
   fields by hand, where the typos are.

```properties
itdastream.sso.saml.enabled=true
itdastream.sso.saml.idp.entity.id=https://idp.example.com/realms/company
itdastream.sso.saml.idp.sso.url=https://idp.example.com/protocol/saml
itdastream.sso.saml.idp.certificate=MIIC…
itdastream.sso.saml.sp.entity.id=itdastream
itdastream.sso.saml.sp.acs.url=https://itda.example.com/admin/auth/sso/saml/acs
```

Every assertion is checked four ways, and each one is a real attack if skipped:

| Check | What it prevents |
| --- | --- |
| Signature, against the provider's certificate | An assertion the caller wrote |
| Audience | A genuine assertion for another service logging in here |
| Validity window | A captured assertion replayed forever |
| Issuer | Any provider the caller can reach being trusted |

On top of those, an assertion already used is refused. That record is kept **in
ZooKeeper, not in one broker's memory**: an assertion is a bearer document, and a
replay arrives at whichever broker the client happens to reach — usually not the
one that saw the original. It is also why only the leader writes IAM but any
broker can record a use.

**Encrypted assertions and signed requests.** Several providers encrypt assertions
or require the authentication request to be signed. Both need a service-provider
keypair — generate one from the console, then re-import the SP metadata at your
provider so it picks up the new certificate. An encrypted assertion must carry its
own signature: encryption proves who the assertion was *for*, never who wrote it.

**NameID format** is left empty by default, which omits the request entirely and
lets the provider issue whatever it is configured for. Naming one breaks more
integrations than it fixes.

### LDAP / Active Directory

```properties
itdastream.sso.ldap.enabled=true
itdastream.sso.ldap.url=ldaps://ad.example.com:636
itdastream.sso.ldap.bind.dn=cn=svc-itdastream,ou=service,dc=example,dc=com
itdastream.sso.ldap.bind.password=…
itdastream.sso.ldap.user.base.dn=ou=people,dc=example,dc=com
itdastream.sso.ldap.user.filter=(sAMAccountName={0})
itdastream.sso.ldap.group.base.dn=ou=groups,dc=example,dc=com
itdastream.sso.ldap.group.filter=(member={0})
```

Authentication is **search then bind**. A service account finds the user's entry —
their DN is something the product cannot construct, since Active Directory puts
people under `CN=John Doe,OU=Staff,…` where neither component is the login name —
and the password is then checked by binding as that DN.

That second bind *is* the authentication. Reading a password attribute and
comparing it would be wrong even where the directory allows it: only the server
knows how its own hashes are salted, and account lockout, expiry and disabled
flags are enforced on bind and nowhere else.

Group membership is read **both ways**: from the user's `memberOf` and from a
search of the group tree. Directories disagree about which side records it —
OpenLDAP usually keeps it on the group, Active Directory mirrors it onto the user
— and reading only one way silently returns no groups against half the servers in
the field.

This is also the provider that makes the Kafka plane simplest: the directory
password goes in the SASL/PLAIN password field, so an existing client works with
directory credentials and no token handling at all.

!!! warning "Use TLS"
    Without `ldaps://` or `itdastream.sso.ldap.starttls=true`, the bind password
    crosses the network in the clear.

## Behind a load balancer

Both browser flows work on any broker, regardless of which one started them. The
login state — including the PKCE verifier — is sealed with a key every broker
derives from `ITDASTREAM_MASTER_KEY` and carried in the `state` parameter itself
rather than held in memory on one broker. Unsealing it is also what proves this
cluster issued it, which is the login-CSRF check.

Without that, SSO works on a single broker and fails on roughly half of all
attempts in a cluster — and it fails looking like a problem at the identity
provider.

## Revocation

Disabling someone at the provider stops new logins immediately. Sessions already
issued keep working until they expire — this cluster is not told about the change.
`itdastream.sso.federated.session.seconds` bounds how long that gap lasts; shorter
is safer.

A federated session gets no refresh token for the same reason: renewing would keep
someone signed in after the directory disabled them.

## Password storage

Independent of SSO, local passwords are stored as PBKDF2-HMAC-SHA256 hashes. They
used to be kept as the plaintext itself; the IAM state is encrypted at rest, but
that only moved the problem — a backup, a snapshot in transit, or anyone who could
read the decrypted state held every password, and password reuse makes that a
credential for other systems too.

Stored plaintext still verifies and is rewritten on the owner's next successful
login, which is the only moment the plaintext is available to hash — so nobody is
locked out and no migration step is required. The iteration count travels with
each stored hash, so raising
`itdastream.auth.password.hash.iterations` (default 600000) does not invalidate
existing passwords.

## See also

- [SASL Authentication and TLS Encryption](sasl.md) — the mechanisms and the transport.
- [AWS IAM-Compatible Access Control](iam.md) — the groups and policies a federated identity maps onto.
- [Configuration](configuration.md) — every setting, with its default.
