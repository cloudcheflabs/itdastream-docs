# Broker Drain

Draining takes a broker out of the picture clients are given, **before** it stops — so traffic moves off it while it is still able to answer, instead of failing against a socket that has already closed.

It exists because of what ItdaStream is. The log lives in object storage and brokers are stateless, so a broker holds nothing that has to be handed over. The only thing tying a client to a particular broker is the metadata response that named it. Draining edits exactly that, and nothing else.

```text
   ┌──────────┐  metadata: brokers = [1, 2, 3]      ┌────────────────┐
   │  client  │────────────────────────────────────►│ broker 1  (3)  │
   └──────────┘                                     └────────────────┘

        drain broker 2 ──────────────────────────────────────────────

   ┌──────────┐  metadata: brokers = [1, 3]         ┌────────────────┐
   │  client  │────────────────────────────────────►│ broker 1  (3)  │
   └──────────┘                                     └────────────────┘
                                                    ┌────────────────┐
              requests already in flight ──────────►│ broker 2       │
                        are still answered          │  (draining)    │
                                                    └────────────────┘
```

## What draining does, exactly

A draining broker:

- **is left out of every broker's metadata response** — including its own. Clients pick this up at their next metadata refresh and route elsewhere.
- **is not given partition leadership.** Leadership is assigned by position in the serving-broker list, so dropping out of that list moves every partition it was fronting onto the brokers that remain.
- **keeps serving every request it receives.** This is the part that is easy to get wrong. Refusing requests during a drain would produce exactly the client errors the drain exists to avoid, and would be refusing them for no reason — no broker is the wrong broker to write through when the log is in object storage.

So a drain is a **routing change with a waiting period**, not a shutdown and not a maintenance mode. Nothing is refused, nothing is paused, and background work carries on.

## Where the flag lives

In the broker's own ephemeral registration under `/brokers/ids/<id>` in ZooKeeper — not in RocksDB, which is where cluster **settings** live.

That split is deliberate and runs across every Cloud Chef Labs product: **ZooKeeper holds node state, RocksDB holds settings.** "This broker is on its way out" is node state — it describes one process, is only true while that process is running, and must not survive it.

Being ephemeral gives that last part for free. A broker that dies mid-drain leaves nothing behind: its registration vanishes with its session, and the flag with it. A restarted broker comes back **serving**, because the flag described a shutdown that has now happened.

!!! note "A drain does not survive a restart — on purpose"
    If you restart a drained broker and want it kept out of rotation, drain it again once it is back. A flag that outlived the process it described would be a broker that is quietly missing from the cluster with nothing on the node explaining why.

## Draining a broker

From the Admin UI: **Cluster Topology → Drain** on the broker's row. Its status changes to *Draining*, a banner appears while any broker is draining, and the button becomes **Resume**. The last serving broker's Drain button is disabled.

Or over REST, against **any** broker's admin port:

```bash
TOKEN=$(curl -sf -X POST http://localhost:8082/admin/login \
    -H 'Content-Type: application/json' \
    -d '{"username":"admin","password":"…"}' | jq -r .token)

# who is serving?
curl -sf http://localhost:8082/admin/maintenance/drain \
    -H "Authorization: Bearer $TOKEN"

# drain broker 42
curl -sf -X POST http://localhost:8082/admin/maintenance/drain \
    -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
    -d '{"brokerId":42,"draining":true}'

# put it back in rotation
curl -sf -X POST http://localhost:8082/admin/maintenance/drain \
    -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
    -d '{"brokerId":42,"draining":false}'
```

`brokerId` is optional and defaults to the broker that received the call. The request does **not** have to go to the broker being drained — the flag lives in ZooKeeper and every broker reads the same tree. Routing it to the target would only add a hop that fails when the target is exactly the node you are trying to take out.

`GET` returns one entry per registered broker:

```json
[ {"id":41,"host":"broker-1","port":9092,"draining":false,"self":true},
  {"id":42,"host":"broker-2","port":9092,"draining":true, "self":false} ]
```

`POST` requires the `kafka:Drain` action on `arn:stream:kafka:cluster:<clusterId>`. `GET` needs only a valid admin session — it restates what any client already learns from a metadata request, and an operator watching a drain has to be able to poll it freely.

### The last serving broker is protected

Draining the only remaining serving broker returns **409 Conflict**. Draining is a routing change, and with nothing left to route to, clients would be handed an empty broker list and stop producing entirely — an outage wearing the costume of a controlled evacuation. To stop the whole cluster, stop the brokers.

## Shutdown drains automatically

You do not have to remember to drain before a restart. `shutdown()` does it first, in this order:

1. **Announce the drain.** Every other broker stops advertising this one.
2. **Pause** for `itdastream.broker.drain.pause.ms` (default 15s), still serving normally. Without this the announcement is pointless: a client that fetched metadata a second earlier still believes this broker is serving.
3. **Wait for in-flight requests** for up to `itdastream.broker.shutdown.grace.ms` (default 30s), then interrupt whatever is left.
4. **Flush** the write buffers to object storage and close the stores.

Step 3 matters more than it looks. Interrupting a handler mid-produce can abort a batch after it reached the staging store but before the write buffer took it — leaving a producer holding an acknowledgement for a record the log does not have. The wait is bounded so a wedged handler cannot stop the broker from stopping.

If ZooKeeper is unreachable at step 1, shutdown logs it and carries on. A broker that cannot reach ZooKeeper still has to be able to stop, and the data safety of a shutdown comes from step 4, not step 1.

```properties
# How long a broker keeps serving after announcing that it is draining.
# Set it to at least the clients' metadata refresh interval; too short and they
# keep arriving at a broker that is about to stop.
itdastream.broker.drain.pause.ms=15000

# How long shutdown waits for requests already being handled to finish before
# interrupting the handler threads.
itdastream.broker.shutdown.grace.ms=30000
```

## When to drain by hand

| Drain first | No need |
| --- | --- |
| Taking a broker out for hardware or OS work | Adding a broker (it registers and starts taking work) |
| Shrinking the cluster | A rolling restart (shutdown drains on its own) |
| Moving a broker to another host | A broker that has already crashed (its registration is gone) |
| Watching one broker misbehave before you decide | Anything shorter than a client's metadata refresh |

The mental model: drain when you want traffic to leave a broker **while it is still healthy**. If it is already gone, there is nothing to drain — the ephemeral registration disappeared with it and clients re-route on the next refresh anyway.

## Verifying it

`tests/broker-drain-e2e.sh` in the product repository runs the whole cycle against a three-broker compose cluster backed by ShannonStore: baseline produce/consume, drain broker 2 and confirm it leaves **every** other broker's metadata while still answering itself, produce with broker 2 still named in the bootstrap list, stop it, produce again, restart it, and produce again — checking after every phase that the consumer sees the full id range with no gaps.

Checking by **id** rather than by count is the point. A count alone passes on a run that lost one message and duplicated another.

## See also

- [Disaggregated Compute-Storage Architecture](disaggre-architecture.md) — why a broker owns nothing that needs handing over.
- [Crash Recovery with Staging Store](staging-store.md) — what happens to records staged but not yet flushed when a broker stops.
- [IAM Policy Reference](iam-policy.md) — the `kafka:Drain` action.
