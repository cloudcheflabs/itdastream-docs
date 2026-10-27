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
      "mode": "leader", "nodeCount": 41, "status": "UP" },
    { "host": "zk2", "port": 2181, "status": "DOWN", "error": "Connection refused" }
  ],
  "client": { "state": "CONNECTED", "sessionId": "0x10000a3b2c40001", "sessionTimeoutMs": 30000,
              "connectedServer": "zk1/10.0.0.21:2181", "brokerId": 1630712349 }
}
```

A chroot in the connect string (`zk1:2181,zk2:2181/itdastream`) is ignored for probing; a server
without a port is probed on 2181.

## Configuration

| Key | Default | Description |
|---|---|---|
| `itdastream.zookeeper.server.list` | `localhost:2181` | The ensemble the brokers use — and the servers the page probes |
| `itdastream.zookeeper.status.probe.timeout.ms` | `2000` | Timeout (ms) for one server's `srvr` probe; a server slower than this is shown Down |

See [Flexible Configuration System](configuration.md#cluster-coordination-settings-zookeeper) for the
other ZooKeeper settings.
