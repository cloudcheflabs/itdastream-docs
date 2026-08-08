# Streaming SDK

The `itdastream-sdk` is a small Java client for submitting streaming jobs programmatically. It
builds the same job specification the Admin UI produces and POSTs it to the admin API — there is
no Arrow or broker dependency on the client side, only the JDK HTTP client and Jackson. The SDK
jar ships under `sdk/` in the distribution.

```xml
<!-- or depend on the published jar -->
itdastream-sdk-1.0.0.jar
```

!!! tip "Want the end-to-end recipe?"
    For a step-by-step walkthrough — create an `ITOK` token, write the job, build an **uber-JAR**, run it as `java -jar`, and upload a custom transform with the job — see [Tutorial: Submit a Job (Token + Uber-JAR)](streaming-job-tutorial.md). This page is the API reference.

---

## Authentication

Prefer a long-lived **IAM user token** (see [IAM](iam.md)) for SDK / CI usage:

```java
ItdaStreamSession session = ItdaStreamSession.builder()
    .adminUrl("http://broker:8080")
    .userToken("ITOK...")          // Authorization: Token ITOK...
    .build();
```

Alternatives: `.token(jwt)` for a pre-obtained JWT, or `.credentials("admin", "password")` to log
in (not recommended for automation).

---

## Programming model

`Source → transforms → Sink`, then `start()`:

```java
String jobId = session.streamSource(Source.kafka("events").format("json"))
    .filter("event_type = 'purchase'")     // simple predicate
    .select("id", "name", "amount")        // projection
    .map("com.example.EnrichFn")           // user MapFunction (1 -> 1)
    .sink(Sink.iceberg("prod-iceberg", "analytics.purchases").upsertKeys("id"))
    .name("purchases-to-iceberg")
    .parallelism(4)
    .commitInterval(5_000)
    .jars("build/libs/my-transforms-all.jar")   // upload the JAR with EnrichFn (omit if no .map/.flatMap)
    .start();
```

### Sources

`Source.kafka(topic).format("json"|"avro").groupId(...).property(k, v)` — the source is always the
ItdaStream cluster itself, so only the topic and format are required.

### Transforms

| Method | Operation | Notes |
|---|---|---|
| `.filter("amount > 100")` | FILTER | simple `=`, `<>`, `>`, `>=`, `<`, `<=` predicates |
| `.select("a","b")` | SELECT | projection |
| `.map("fqcn")` | MAP | user `MapFunction` (1 row → 1 row) |
| `.flatMap("fqcn")` | FLATMAP | user `FlatMapFunction` (1 row → N rows) |

`map` / `flatMap` reference a class by fully-qualified name that implements
`com.cloudcheflabs.itdastream.streaming.udf.MapFunction` /
`...udf.FlatMapFunction`, with a public no-arg constructor:

```java
public class EnrichFn implements MapFunction {
    public Map<String,Object> apply(Map<String,Object> row) {
        row.put("amount_usd", ((Number) row.get("amount")).doubleValue() * 1.08);
        return row;
    }
}
```

You ship the class **with the job**: list its JAR(s) via `.jars("path/to/transforms.jar", …)`. On `start()` the SDK uploads them to the cluster dependency store (`POST /admin/deps`, deduped by name), and each broker loads the class from a **per-job `URLClassLoader`** over those JARs — no copy to `lib/`, no restart. Mark `itdastream-streaming` `compileOnly` when building the JAR (it is provided by the broker). The full packaging recipe is in [Tutorial: Submit a Job (Token + Uber-JAR)](streaming-job-tutorial.md#advanced-custom-transforms-uploaded-with-the-job).

!!! tip "No transforms? No JARs."
    A job with only `.filter(...)` / `.select(...)` (or none) needs no `.jars(...)` — it is the [no-code auto-sink](streaming-no-code.md) path and runs entirely from the declarative spec.

### Sinks

```java
Sink.iceberg("conn", "ns.table").upsertKeys("id")   // exactly-once
Sink.jdbc("conn", "table")                            // exactly-once
Sink.kafka("conn", "topic").keyField("id")            // at-least-once
Sink.elasticsearch("conn", "index").idField("id")     // at-least-once
Sink.http("https://example.com/hook")                 // at-least-once
Sink.console()                                         // debug
Sink.neorunBase("conn", "table")                      // at-least-once
```

Credentials come from the named connection (`conn`); see [Connections](connections.md).

---

## Lifecycle

```java
JobHandle handle = session.streamSource(Source.kafka("events"))
    .sink(Sink.iceberg("prod-iceberg", "analytics.events"))
    .parallelism(2).submit();   // returns a handle instead of just the id

String status = handle.status();   // per-broker liveness + last completed checkpoint
handle.stop();                     // stop and delete the job
```

---

## Complete example

A full, runnable program. It authenticates with an IAM user token, submits an exactly-once
Kafka → Iceberg upsert pipeline with a user transform, prints the job status, and (optionally)
stops it.

```java
package com.example;

import com.cloudcheflabs.itdastream.sdk.ItdaStreamSession;
import com.cloudcheflabs.itdastream.sdk.JobHandle;
import com.cloudcheflabs.itdastream.sdk.Sink;
import com.cloudcheflabs.itdastream.sdk.Source;

public class PurchasesPipeline {
    public static void main(String[] args) throws Exception {
        // args: <adminUrl> <userToken>
        String adminUrl  = args.length > 0 ? args[0] : "http://localhost:8080";
        String userToken = args.length > 1 ? args[1] : System.getenv("ITDASTREAM_USER_TOKEN");

        // 1) Open a session. Prefer a long-lived IAM user token for SDK / CI.
        ItdaStreamSession session = ItdaStreamSession.builder()
                .adminUrl(adminUrl)
                .userToken(userToken)         // Authorization: Token ITOK...
                .build();

        // 2) Build and submit the job: events topic (JSON) -> filter/select/map -> Iceberg upsert.
        JobHandle job = session.streamSource(
                        Source.kafka("events")
                              .format("json")
                              .property("auto.offset.reset", "earliest"))
                .filter("event_type = 'purchase'")          // keep purchases only
                .select("id", "user_name", "amount", "ts")  // project columns
                .map("com.example.EnrichFn")                // 1->1 user transform (uploaded with the job)
                .sink(Sink.iceberg("prod-iceberg", "analytics.purchases")
                          .upsertKeys("id"))                // idempotent upsert by id
                .name("purchases-to-iceberg")
                .parallelism(4)                             // 4 consumer threads across the cluster
                .commitInterval(5_000)                      // exactly-once checkpoint cadence (ms)
                .jars("build/libs/my-transforms-all.jar")   // ship EnrichFn to the cluster
                .submit();                                  // returns a JobHandle

        System.out.println("submitted jobId=" + job.jobId());

        // 3) Inspect status (per-broker liveness + last completed checkpoint).
        System.out.println("status=" + job.status());

        // 4) Stop + delete the job when done (omit for a long-running pipeline).
        // job.stop();
    }
}
```

The `map` step references a user class that the SDK uploads with the job (via `.jars(...)`) and the broker loads per-job:

```java
package com.example;

import com.cloudcheflabs.itdastream.streaming.udf.MapFunction;
import java.util.Map;

public class EnrichFn implements MapFunction {
    @Override
    public Map<String, Object> apply(Map<String, Object> row) {
        double amount = ((Number) row.getOrDefault("amount", 0)).doubleValue();
        row.put("amount_usd", amount * 1.08);            // add a derived column
        row.put("ingested_at", System.currentTimeMillis());
        return row;
    }
}
```

Compile and run against the SDK jar (bundled under `sdk/` in the distribution):

```bash
javac -cp "itdastream-sdk-1.0.0.jar" -d out com/example/PurchasesPipeline.java
java  -cp "out:itdastream-sdk-1.0.0.jar:jackson-databind.jar:jackson-core.jar:jackson-annotations.jar" \
      com.example.PurchasesPipeline http://broker:8080 "$ITDASTREAM_USER_TOKEN"
```

### Variations by sink

The only change between targets is the `Sink` and the connection it references:

```java
// Kafka -> another topic (at-least-once)
.sink(Sink.kafka("prod-kafka", "purchases-out").keyField("id"))

// JDBC upsert target (exactly-once, transactional)
.sink(Sink.jdbc("prod-pg", "purchases"))

// Elasticsearch index (at-least-once)
.sink(Sink.elasticsearch("prod-es", "purchases").idField("id"))

// HTTP webhook (at-least-once)
.sink(Sink.http("https://example.com/ingest"))

// NeorunBase (REST or JDBC mode, per the connection)
.sink(Sink.neorunBase("prod-nrb", "purchases"))

// Console, for debugging a pipeline locally
.sink(Sink.console())
```

### Submitting many jobs

`ItdaStreamSession` is reusable and thread-safe for submission; build it once and submit many
jobs:

```java
ItdaStreamSession session = ItdaStreamSession.builder()
        .adminUrl("http://broker:8080").userToken(token).build();

for (String topic : List.of("events", "clicks", "orders")) {
    String id = session.streamSource(Source.kafka(topic).format("json"))
            .sink(Sink.iceberg("prod-iceberg", "raw." + topic))
            .name(topic + "-raw")
            .parallelism(2)
            .commitInterval(10_000)
            .start();
    System.out.println(topic + " -> jobId " + id);
}
```

### Error handling

`build()` throws if the admin URL is unreachable or auth fails (e.g. an account that still
requires a password change). `start()` / `submit()` throw a `RuntimeException` carrying the
HTTP status and body if the spec is rejected (unknown connection, bad sink, etc.):

```java
try {
    String id = session.streamSource(Source.kafka("events"))
            .sink(Sink.iceberg("missing-conn", "ns.t"))
            .start();
} catch (RuntimeException e) {
    System.err.println("submit failed: " + e.getMessage());  // e.g. 400 Connection not found
}
```
