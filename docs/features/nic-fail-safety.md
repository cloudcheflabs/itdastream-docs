# NIC Silent-Fail Safety

ItdaStream's internal NIO plane survives the trickiest failure mode in distributed systems: **a peer whose NIC dies after the socket was opened, but ZooKeeper hasn't expired the session yet**. From ZooKeeper's POV the node is alive. From every neighbour's POV its TCP connection is also alive — the kernel does not surface a half-open peer for tens of seconds, sometimes minutes, while it exhausts its retransmit budget. RPCs that route to that peer hang. Membership-driven failover doesn't fire because membership is still healthy.

The 1.0.0 release closes this gap on **both ends** of every internal RPC — `InternalNioClient` (used by every broker → broker call — controller polls, KMS/IAM sync, metrics collection, internal data forwarding) and `InternalNioServer` (the response side).

## Why the legacy NIO loop is dangerous

A blocking `SocketChannel`-based loop in three places hangs on a silently-dead peer:

1. **`connect()`** in blocking mode parks the caller inside the TCP three-way handshake until the OS retransmit budget drains. On Linux this is dictated by `tcp_syn_retries` and runs ~60–130 seconds by default.
2. **`channel.write()`** parks the caller the instant the kernel send buffer fills. For a peer that stopped draining, the buffer never frees up until the OS gives up — which is the *retransmit* budget, not the SYN budget. Linux `tcp_retries2` ranges from 5 to 15 minutes depending on tuning.
3. **`channel.read()`** in blocking mode is similar — no application-visible deadline.

A single hung writer takes its calling thread out of service. If that thread is the master's heartbeat-sweep loop or a worker's coordinator-pull, the entire cluster appears wedged from outside even though only one peer is sick.

## What changed in the client

`com.cloudcheflabs.itdastream.broker.network.internal.InternalNioClient` opens one short-lived
`SocketChannel` per request on a daemon thread pool (`internal-nio-client`) and closes it when
the response has been read. Every one of the three phases is bounded:

- **Bounded connect.** `socket().connect(address, connectTimeoutMs)` fails at the deadline
  instead of parking inside the TCP three-way handshake for the OS SYN budget. Default 5s,
  from `itdastream.internal.nio.connect.timeout.ms`.
- **Bounded write.** This is the phase the OS gives you no help with. `writeBounded()` flips
  the channel to non-blocking, registers `OP_WRITE` on a per-call `Selector`, and loops with a
  deadline: on a peer that has stopped draining, `write()` returns 0 and `select(remaining)`
  returns at the deadline, at which point a `SocketTimeoutException` is raised and the calling
  thread is freed. A plain blocking `write()` would instead have parked for the retransmit
  budget. The deadline is a fixed **10 seconds**.
- **Bounded read.** `setSoTimeout(readTimeoutMs)` gives the response read an
  application-visible deadline. Default 30s, from `itdastream.internal.nio.read.timeout.ms`.

Frames larger than `itdastream.internal.nio.max.frame.bytes` (10 MiB) are rejected outright, so
a corrupt length prefix cannot make the reader wait for data that will never arrive.

## What changed in the server

`com.cloudcheflabs.itdastream.broker.network.internal.InternalNioServer`:

- The accept path already left client channels non-blocking — the server uses a Selector reactor.
- The **response write** path previously did a bare `while (buf.hasRemaining()) channel.write(buf);` under a `synchronized(channel)` block. In non-blocking mode `channel.write()` returns 0 the moment the send buffer is full — the old loop **busy-spun forever** on a wedged peer because there was no `Selector` to wait on writability.
- The new path switches the channel to non-blocking, opens a per-call `Selector` registered for
  `OP_WRITE`, and bounds the loop with a deadline. On timeout it throws `IOException`, the
  worker thread frees up, and the connection is closed in the `finally` block.

The server's deadline is `itdastream.internal.nio.socket.timeout.ms` (default 30s) — the same
value it uses for `setSoTimeout` on the request read. Worker-pool size is
`itdastream.internal.nio.worker.threads` (default 32).

## Failure-handling invariants

| Scenario | Old behaviour | 1.0.0 behaviour |
|---|---|---|
| Peer NIC dies mid-handshake | `connect()` hangs for OS SYN budget (~60–130s) | `SocketTimeoutException` at the connect deadline (default 5s) |
| Peer NIC dies after socket established, mid-write | `channel.write()` hangs for OS retransmit budget (5–15 min) | `SocketTimeoutException` at the 10s write deadline; caller sees a fast failure and can fall back |
| Peer process paused (kernel still ACKs but app doesn't drain) | Same as above — kernel send buffer fills, then writer blocks indefinitely | `Selector.select()` wakes at the write deadline; writer fails out cleanly |
| Response never arrives from a wedged peer | Blocking `read()` with no application deadline | `SO_TIMEOUT` fires at `itdastream.internal.nio.read.timeout.ms` (default 30s) |

## Verifying it on your stack

`tests/test-nic-failure-channel-health.sh` (shipped with the 1.0.0 release) drives the failure on a 2-node compose:

```bash
docker compose -f tests/docker-compose-itdastream.yml up -d
./tests/test-nic-failure-channel-health.sh    # expect PASS
```

The script does the pausing itself: it `docker pause`s `itda-broker-2` to simulate a silent NIC
death, hammers the surviving `itda-broker-1` admin endpoint while the peer is frozen, then
unpauses it and checks that it recovers (an `EXIT` trap unpauses the peer even on failure). With
the fix, broker-1 answers `200 OK` for every call throughout; without it, it would block — often
for minutes — on the dead peer's internal RPC.

## Operational notes

- **The deadlines are conservative**, and three of the four are tunable — see
  [Configuration → Internal NIO](configuration.md#internal-nio-broker-to-broker):

    | Phase | Property | Default |
    |---|---|---|
    | Connect | `itdastream.internal.nio.connect.timeout.ms` | `5000` |
    | Response read (client) | `itdastream.internal.nio.read.timeout.ms` | `30000` |
    | Request read + response write (server) | `itdastream.internal.nio.socket.timeout.ms` | `30000` |
    | Request write (client) | *fixed in code* | 10s |

    Each is far below any realistic peer-to-peer round trip on a healthy cluster but above the
    worst-case TCP retransmit on a busy network.
- **No backwards-compatible setting reverts to the old blocking behaviour**. There is no safe value here — the old behaviour was always a hang.
- The fix changes only the *internal* broker-to-broker RPC plane (`itdastream.nio.port`). External-facing transports — the Kafka wire-protocol listener and the admin HTTP server — keep their existing transport-specific timeouts, as does the S3 object-storage client.
