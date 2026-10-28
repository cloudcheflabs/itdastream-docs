# Cluster Topology

The admin console's **Cluster Topology** page shows every process the cluster depends on: the
brokers that serve Kafka clients, and the ZooKeeper ensemble the brokers coordinate through.

ZooKeeper is not a side detail. Every broker registers itself there, the controller is elected there,
drain flags, KMS and IAM sync signals and streaming assignments all live there. A broker cluster
whose ensemble has lost quorum cannot elect a controller or register a restarted broker, even though
every broker still looks healthy. The page therefore lists the ensemble next to the brokers, with
the role and health of each server.

## Brokers

One row per broker registered under `/brokers/ids` in ZooKeeper:

| Column | Meaning |
|---|---|
| Broker ID | The broker's id (stateless by default: derived from its address and port) |
| Host / IP | Advertised host and resolved address |
| Kafka Port / Admin Port | The Kafka listener and the console / REST port |
| Status | **Serving**, or **Draining** while the broker is being taken out of rotation — see [Broker Drain](broker-drain.md) |

Each row has **Drain / Resume** and **Log View**. The header counts the brokers that are serving.

A broker can stall longer than its ZooKeeper session timeout, for example during a long GC pause, a frozen VM or a network partition. It then loses its registration, and with it its row in this table and its place in the metadata that clients receive. When it reconnects, it registers again and keeps its drain state. A follower also pulls KMS, IAM and connections from the leader to catch up. Its log records `Re-registered … the previous session expired`. A leader that stalls this long hands leadership to another broker and returns as a follower. The new leader re-creates the cluster-ready marker, so brokers restarted afterwards become healthy right away and do not wait out the 120-second ready timeout.

A password change sent to a follower broker is answered with success only once that broker accepts the new password. Otherwise the answer is `502`.

## ZooKeeper ensemble

One row per server of `itdastream.zookeeper.server.list`, in the order it is configured:

| Column | Meaning |
|---|---|
| Server | `host:port` from the connect string |
| Server id | The server's own id (`myid`) as it reports it to `conf`, or `server id unknown (...)` with the reason — see [Server ids](#server-ids-myid) |
| Role | `leader`, `follower`, `observer`, or `standalone` for a single-server ZooKeeper |
| Status | **Up**, or **Down** with the reason (connection refused, timed out, `srvr` not whitelisted, …) |
| Version | The ZooKeeper server version |
| Connections | Client connections open on that server |
| Latency | Average and maximum request latency (ms) since the server started or its stats were reset |
| Zxid | The last transaction id the server has applied — followers lagging the leader show a lower zxid |
| Znodes | The number of znodes in the server's data tree |

The header shows how many servers are up and whether they still form a **quorum** (more than half of
the configured servers). With no quorum it turns red: the ensemble cannot accept writes, so brokers
cannot elect a controller or register.

Below the table, **Broker _n_ session** shows the ZooKeeper session of the broker that answered the
page: its state (`CONNECTED`, `CONNECTING`, `CLOSED`, …), the server it is connected through, the
session id and the negotiated session timeout. This is the broker's own view, so it shows a broker
that has lost its session even while the ensemble itself answers.

The page refreshes every 5 seconds.

### How the servers are probed

Each broker asks every server directly with ZooKeeper's `srvr` four-letter command over a plain TCP
connection — no client session, no extra library. `srvr` is the one command in ZooKeeper's default
`4lw.commands.whitelist`, so this works against any ensemble out of the box.

!!! warning "Keep `srvr` in the whitelist"
    If your ensemble narrows `4lw.commands.whitelist` and leaves `srvr` out, every server is shown
    **Down** with the reason *srvr is not in this server's 4lw.commands.whitelist*. The brokers
    themselves are not affected — only the status display.

A server that does not answer within `itdastream.zookeeper.status.probe.timeout.ms` (default 2000 ms)
is shown Down. The servers are probed in parallel, so one dead server costs one timeout, not one per
server.

### Server ids (myid)

Every server of an ensemble needs its own id (`myid`). Two servers with the same id — typically
left behind by a scale-out or a rebuild that reused an instance number — or servers that do not
agree on who is in the ensemble break it **silently**: ZooKeeper raises no error, and clients just
see an unstable or split service. The page therefore also asks each server that answers with the
`conf` four-letter word, which reports the server's own `serverId` and the ensemble membership it
was configured with (its `server.N=` lines). The **Server id** column shows the id, or
`server id unknown (...)` with the reason, and a red line above the table names each problem:

| Warning | Meaning |
|---|---|
| `Duplicate ZooKeeper server id 2 on zk2:2181, zk3:2181 — the ensemble is misconfigured; give every server a unique myid` | two or more configured servers report the same id |
| `ZooKeeper server zk4:2181 reports server id 4, which is not in its own ensemble membership (1, 2, 3) — check its myid` | a server's id is not among the `server.N` entries it was configured with |
| `ZooKeeper servers disagree about the ensemble membership: zk1:2181, zk2:2181 list server ids 1, 2, 3; zk3:2181 lists server ids 2 — every server must be configured with the same server.N list` | the servers were configured with different ensembles |
| `ZooKeeper server id unknown on zk1:2181, zk2:2181, zk3:2181 (conf not in 4lw.commands.whitelist; add conf there and restart that ZooKeeper server) — a duplicate myid cannot be ruled out` | the id could not be read (here `conf` is not whitelisted; also `server is down`, `server is not serving requests`) — the check is never reported as passing for those servers |
| `ZooKeeper servers zk1:2181, zk2:2181 run standalone — a standalone server is a separate single-server ZooKeeper, not part of one ensemble with the other configured servers` | the connect string lists several servers but some of them run standalone |

A healthy ensemble shows each server's id and no warning; a single standalone ZooKeeper reports id
`0` and is not a warning. To fix a duplicate or a mismatch, correct the `myid` file in the server's
data directory (`ZOO_MY_ID` for the `zookeeper` Docker image) and its `server.N=` lines so that
every server has a unique id and all of them list the same ensemble, then restart that server.

`conf` is **not** on ZooKeeper's default whitelist. Add it next to `srvr`, for example
`4lw.commands.whitelist=srvr,conf` (`ZOO_4LW_COMMANDS_WHITELIST=srvr,conf` for the Docker image),
and restart that ZooKeeper server: ZooKeeper reads the whitelist only at startup.
Without it every server's id reads `server id unknown (conf not in 4lw.commands.whitelist; add conf there and restart that ZooKeeper server)`.
The ZooKeeper bundled with itdastream (`conf/zk/zoo.cfg`) ships with `4lw.commands.whitelist=srvr,conf,ruok,stat,mntr`.

`tests/e2e-zk-myid.sh` verifies all of this on a real three-server ensemble, on the API and on the
Cluster Topology page from a non-secure origin (`tests/ui-e2e/45-zk-myid.mjs`).

## REST API

Both endpoints need an authenticated session (`Authorization: Bearer <token>`) and are read-only. Any
broker answers them; there is no forwarding to the controller.

### `GET /admin/nodes`

The registered brokers:

```json
[
  { "id": "1630712349", "host": "broker-1", "ip": "10.0.0.11", "port": 9092, "adminPort": 8082,
    "internalNioPort": 9000, "rack": null, "draining": false }
]
```

### `GET /admin/zookeeper`

The ensemble and the answering broker's session:

```json
{
  "connectString": "zk1:2181,zk2:2181,zk3:2181",
  "rootPath": "/itdastream",
  "servers": [
    { "host": "zk1", "port": 2181, "version": "3.9.1", "latencyMin": 0, "latencyAvg": 0.44, "latencyMax": 12,
      "received": 120, "sent": 119, "connections": 3, "outstanding": 0, "zxid": "0x200000004",
      "mode": "leader", "nodeCount": 41, "status": "UP", "serverId": 1, "serverIdNote": null,
      "membership": { "1": "zk1:2888:3888:participant;0.0.0.0:2181", "2": "zk2:2888:3888:participant;0.0.0.0:2181",
                      "3": "zk3:2888:3888:participant;0.0.0.0:2181" } },
    { "host": "zk2", "port": 2181, "status": "DOWN", "error": "Connection refused",
      "serverId": null, "serverIdNote": "server id unknown (server is down)", "membership": {} }
  ],
  "warnings": [ "ZooKeeper server id unknown on zk2:2181 (server is down) — a duplicate myid cannot be ruled out" ],
  "client": { "state": "CONNECTED", "sessionId": "0x10000a3b2c40001", "sessionTimeoutMs": 30000,
              "connectedServer": "zk1/10.0.0.21:2181", "brokerId": 1630712349 }
}
```

A chroot in the connect string (`zk1:2181,zk2:2181/itdastream`) is ignored for probing; a server
without a port is probed on 2181. `warnings` is empty when every server reported a unique id and all
of them agree on the membership.

## Configuration

| Key | Default | Description |
|---|---|---|
| `itdastream.zookeeper.server.list` | `localhost:2181` | The ensemble the brokers use — and the servers the page probes |
| `itdastream.zookeeper.status.probe.timeout.ms` | `2000` | Timeout (ms) for one server's `srvr` (and `conf`) probe; a server slower than this is shown Down |

See [Flexible Configuration System](configuration.md#cluster-coordination-settings-zookeeper) for the
other ZooKeeper settings.
