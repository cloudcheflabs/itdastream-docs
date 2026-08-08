# Admin Password Recovery

itdastream ships with a built-in recovery channel that lets an operator
reset the `admin` user's password **without stopping the broker**, even
when no one remembers the current password.

Recovery uses a local Unix domain socket — there is no HTTP back-door, no
network endpoint, no recovery URL. Authentication is performed by the
operating system: only a process that already shares the broker's
filesystem identity can open the socket.

## When to use

- The admin password was forgotten or rotated out of the password manager.
- An automation script needs to provision a known admin password during
  first-time setup, without going through the web UI.
- A new operator is onboarded and you need to hand them an admin credential.

For day-to-day password changes (the user remembers the old password and
wants a new one), use the Admin UI's **Change Password** screen instead —
that path requires the old password and does not flag the user for forced
rotation.

## How it works

```
┌────────────────────┐   JSON over UDS    ┌────────────────────┐
│  itdastream-cli    │ ─────────────────▶ │  Broker (running)  │
│  iam:reset-password│                    │   AuthManager      │
└────────────────────┘                    │   .adminResetPwd() │
         ▲                                │                    │
         │ stdout: new password           │  saveAndBroadcast()│
         │ (one-time)                     │  cluster sync push │
                                          │  audit log append  │
                                          └────────────────────┘
```

| Property | Value |
|---|---|
| **Socket path** | `${itdastream.base.data.dir}/admin.sock` by default, mode `600` — the live value is published to `bin/broker.socket` |
| **Authentication** | OS file permission — same user as the broker process |
| **Socket path marker** | `bin/broker.socket` — written when the socket binds, removed on shutdown |
| **Network surface** | none — Unix domain socket only |
| **Downtime** | none — applied in-process on the live broker |
| **Cluster sync** | automatic — IAM state fans out to peer brokers via the existing sync path |
| **Audit log** | `data/iam-audit/reset.log` (mode `600`, append-only) |
| **Post-reset state** | `requirePasswordChange = true` (forced rotation on next login) |

## Quick start

The simplest invocation lets the broker generate a strong 20-character
password and print it to stdout. The new password must be changed on the
admin's next login (the `requirePasswordChange` flag is set automatically).

```bash
# Inside the broker host or container:
bin/itdastream-cli.sh iam:reset-password
```

## Input modes

| Mode | Command | When to use |
|---|---|---|
| **Broker-generated** | `itdastream-cli.sh iam:reset-password` | Default. Strong random password printed once on stdout. |
| **Explicit** | `itdastream-cli.sh iam:reset-password --new-password 'My!Pass'` | Automation that knows the desired value. Beware: argv may show up in `ps`. |
| **Stdin** | `echo 'My!Pass' \| itdastream-cli.sh iam:reset-password --new-password -` | Automation that wants to avoid argv exposure. |
| **Interactive** | `itdastream-cli.sh iam:reset-password --interactive` | Operator at a TTY. Prompts for password twice with no echo. |

Resetting a different user is also supported:

```bash
bin/itdastream-cli.sh iam:reset-password --user some-user --new-password 'NewPass123'
```

## Configuration

The recovery socket is enabled by default. Every key below lives in
`conf/itdastream.properties` and is read at broker startup:

```properties
# conf/itdastream.properties
# Set false to remove the local recovery path entirely.
itdastream.admin.socket.enabled      = true
itdastream.admin.socket.path         = ${itdastream.base.data.dir}/admin.sock
# Name of the file under <itdastream.home>/bin that receives the socket path the
# broker actually bound to (see "How the CLI finds the socket" below).
itdastream.admin.socket.marker.file  = broker.socket
# Append-only audit trail of socket operations.
itdastream.iam.audit.dir = ${itdastream.base.data.dir}/iam-audit
```

The socket follows `itdastream.base.data.dir`. If you launch the broker with
`-Ditdastream.base.data.dir=/var/lib/itdastream` the socket moves to
`/var/lib/itdastream/admin.sock` — you do not have to restate it.

### How the CLI finds the socket

Re-deriving the socket path from `conf/itdastream.properties` is not reliable on its own:
`itdastream.base.data.dir` can be overridden with `-D` at launch or edited after
startup, and the file does not record which value the live process used. So the
broker **publishes the path it actually bound to** into
`<install dir>/bin/broker.socket` when the socket comes up, and removes that file on
shutdown. `bin/itdastream-cli.sh` prefers it.

Full resolution order, highest priority first:

1. `--socket /path/to/admin.sock` — read by the Java CLI, always wins.
2. `$ITDASTREAM_ADMIN_SOCKET` — if already exported in the caller's shell.
3. `<install dir>/bin/broker.socket` — the path published by the running broker.
   Used only when the file exists *and* the path in it is a live socket.
4. `itdastream.admin.socket.path` from `conf/itdastream.properties`, with
   `${itdastream.base.data.dir}` expanded. A value that still contains a
   `${...}` placeholder is rejected rather than used literally.
5. `<install dir>/data/admin.sock`, then `/data/admin.sock`.

Step 3 is what makes a moved data dir work: with the socket at
`/data/admin.sock` and the properties file still saying `./data`, only the marker
knows where to connect.

To rename the marker, change one key — both ends read it:

```bash
# conf/itdastream.properties
itdastream.admin.socket.marker.file = itdastream-recovery.socket
```

Restart the broker; it publishes `bin/itdastream-recovery.socket`, and the CLI
picks the new name up from the same properties file.

### The master key is for the broker, not the CLI

`ITDASTREAM_MASTER_KEY` must be exported for the broker process. `bin/start-broker.sh` checks it up front and refuses to start when it is unset or shorter than 32 characters.
The variable name itself is configurable — `itdastream.kms.master.key.env` in
`conf/itdastream.properties` names the variable the broker reads:

```bash
export ITDASTREAM_MASTER_KEY='replace-with-a-32-char-or-longer-secret'
bin/start-broker.sh
```

`bin/itdastream-cli.sh` does **not** need it. The CLI only opens the Unix socket and
hands the request to the running broker, which already holds the unsealed key,
so this works with the variable unset:

```bash
unset ITDASTREAM_MASTER_KEY
bin/itdastream-cli.sh ping
# pong
```

If a CLI invocation complains about the key rather than the socket, you are
running a start script, not the CLI.

### Worked examples

```bash
# 1. On the host, as the same OS user that runs the broker:
cd /opt/itdastream
bin/itdastream-cli.sh ping
bin/itdastream-cli.sh iam:reset-password

# 2. The broker runs as a service account and you are root:
sudo -u itdastream /opt/itdastream/bin/itdastream-cli.sh iam:reset-password

# 3. Inside a container:
docker exec -it itdastream-broker-1 /app/bin/itdastream-cli.sh iam:reset-password

# 4. Data dir was relocated at launch — no extra flags needed, the CLI
#    reads the published marker:
cat /opt/itdastream/bin/broker.socket
# /var/lib/itdastream/admin.sock
bin/itdastream-cli.sh ping

# 5. Socket in a non-standard place and no marker (the broker is stopped,
#    or you are on a host where the marker was cleaned up):
bin/itdastream-cli.sh --socket /var/lib/itdastream/admin.sock iam:reset-password

# 6. Non-interactive automation, password from stdin so it never reaches argv:
echo 'S0me!Strong!Pass' | bin/itdastream-cli.sh iam:reset-password --new-password -
```

## Security model

**1. The socket is OS-gated.**
At startup the broker creates `data/admin.sock` with mode `600` (owner
read/write only). Even other unprivileged users on the same host cannot
connect. There is no token, no shared secret, no network listener.

**2. The audit log records every reset.**
Every successful reset appends a JSON line to `data/iam-audit/reset.log`
(mode `600`). The plaintext password is **never** logged — only the first
8 characters of its hash, the user, whether it was broker-generated, and
the OS user that invoked the CLI.

```json
{"ts":"2026-05-23T16:00:46.922Z","event":"iam.reset-password","user":"admin","generated":true,"hashFp":"OYV/Ojf/","invokedAs":"root"}
```

**3. The new password is exposed exactly once.**
For broker-generated passwords, the plaintext is returned only on the
single CLI invocation that triggered the reset. It is not retransmitted.
Treat scrollback and shell history accordingly — or use stdin input mode
to avoid argv exposure entirely.

**4. Forced rotation on next login.**
After reset, the user is flagged `requirePasswordChange = true`. The next
successful login forces the user through the change-password flow, so a
temporary password used by the operator is immediately replaced by a
password only the user knows.

**5. The broker must be running.**
Because the recovery channel is in-process, the broker must be alive for
the CLI to connect. This is intentional: RocksDB requires an exclusive
lock, so an offline edit would either conflict with a running broker or
need a complex stale-lock recovery. With this design, the only way to
reset is to be on the host *and* have the broker running *and* share its
filesystem identity.

## Limitations

- **Broker must be running.** If the broker is down, this CLI cannot help.
  Bring the broker back up first, then run the CLI.
- **No knowledge factor.** Any process that shares the broker's filesystem
  identity can invoke the CLI. In multi-tenant or shared-shell
  environments, restrict shell access to the broker accordingly. A future
  enhancement may add an opt-in "recovery key" requirement for an
  additional knowledge factor.

## Related

- [AWS IAM-Compatible Access Control](iam.md)
- [IAM Policy Reference](iam-policy.md)
- [Disaggregated Architecture](disaggre-architecture.md)
