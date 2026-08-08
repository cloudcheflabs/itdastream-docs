# Tutorial: Submit a Streaming Job (User Token + Uber-JAR)

This tutorial walks the full path of running a real streaming job on itdastream:

1. Get a long-lived **IAM user token** (`ITOK…`).
2. Write a job with the **Streaming SDK** (Kafka → Iceberg, exactly-once).
3. Package your submitter as an **uber-JAR** and run it as `java -jar …`.
4. Verify and operate the job.
5. (Advanced) Package a **custom transform** as a JAR and **upload it with the job** — no broker restart.

It complements the API reference in [Streaming SDK](streaming-sdk.md) and the [No-Code Kafka to Iceberg](streaming-no-code.md) path — use the SDK when you want the job in version control / CI, or when you need a custom transform.

## How submission actually works

The SDK builds a **declarative job spec** (source → operations → sink) and `POST`s it as JSON to `/admin/streaming/jobs` with your token. The cluster schedules and runs it. The spec itself carries no code — but a job *can* reference **custom-transform classes**, and those are uploaded alongside it. There are two kinds of JAR, with two destinations:

- The program that *defines and submits* the spec is an ordinary client — package **it** as an uber-JAR for convenient `java -jar` / CI runs. (Recipe in steps 2–4.)
- A **custom transform** (`.map(...)` / `.flatMap(...)`) names a class the broker loads at run time. You **upload** that transform JAR to the cluster's dependency store (`POST /admin/deps`); the broker pulls it from shared object storage and loads the class with a **per-job `URLClassLoader`** — no broker restart, no copying to `lib/`. The SDK's `.jars(...)` and the `bin/submit.sh --jars` CLI do the upload for you. (Recipe in the last section.)

Keep these two JARs separate in your head — they have different contents, but **both are delivered from your machine at submit time**. There's a summary table at the end.

## Prerequisites

- A running itdastream cluster — admin API reachable (e.g. `http://localhost:8080`).
- A source topic (e.g. `events`).
- An **ICEBERG connection** registered in the connection registry (Admin UI → Connections, or the REST API) — the sink references it by id (e.g. `prod-iceberg`). See [Connections](connections.md).
- JDK 17+ and Gradle on the machine that submits the job.
- The `itdastream-sdk-1.0.0.jar` from the distribution's `sdk/` directory (and, only for custom transforms, `itdastream-streaming-1.0.0.jar` from `lib/` to compile against).
- For the `bin/submit.sh` route: `bash`, `curl`, and `jq` on the submitting machine.

## Step 1 — Create an IAM user token (`ITOK…`)

The user token is the recommended SDK / CI credential — it is long-lived and carries the user's group policies. Create an access key for a user (Admin UI → IAM, or the REST API); the response returns three values **once**:

```bash
curl -s -X POST http://localhost:8080/admin/iam/keys \
  -H "Authorization: Bearer $ADMIN_JWT" \
  -H 'Content-Type: application/json' \
  -d '{"username": "pipeline-bot"}'
# → { "accessKey": "AKIA...", "secretKey": "...", "token": "ITOK..." }
```

Keep the `token` (`ITOK…`) safe — it is shown only once. It authenticates programmatic calls via the `Authorization: Token <token>` header, which the SDK sets for you. See [IAM](iam.md) for the credential model and how policies / expiration apply.

```bash
export ITDASTREAM_USER_TOKEN="ITOK..."
```

## Step 2 — Set up the submitter project

Create a Gradle project that depends on the SDK. The SDK ships in the itdastream distribution's `lib/`; copy `itdastream-sdk-1.0.0.jar` into a `libs/` folder in your project (or resolve it from your internal artifact repository).

`build.gradle`:

```groovy
plugins {
    id 'java'
    id 'application'
    // Uber-JAR plugin (Recipe A — the submitter fat jar)
    id 'com.gradleup.shadow' version '8.3.5'
}

repositories { mavenCentral() }

dependencies {
    // The itdastream SDK (from the distribution lib/, or your artifact repo).
    implementation files('libs/itdastream-sdk-1.0.0.jar')
    // The SDK serializes the spec with Jackson.
    implementation 'com.fasterxml.jackson.core:jackson-databind:2.17.2'
}

java { toolchain { languageVersion = JavaLanguageVersion.of(17) } }

application {
    mainClass = 'com.example.PurchasesToIceberg'
}
```

## Step 3 — Write the job

The SDK is a fluent builder: `streamSource(...)` → transforms → `.sink(...)` → `.start()`. Authenticate with the user token from the environment.

`src/main/java/com/example/PurchasesToIceberg.java`:

```java
package com.example;

import com.cloudcheflabs.itdastream.sdk.ItdaStreamSession;
import com.cloudcheflabs.itdastream.sdk.Sink;
import com.cloudcheflabs.itdastream.sdk.Source;

public class PurchasesToIceberg {
    public static void main(String[] args) {
        // args: <adminUrl> [userToken]   (token also read from ITDASTREAM_USER_TOKEN)
        String adminUrl   = args.length > 0 ? args[0] : "http://localhost:8080";
        String userToken  = args.length > 1 ? args[1] : System.getenv("ITDASTREAM_USER_TOKEN");

        ItdaStreamSession session = ItdaStreamSession.builder()
                .adminUrl(adminUrl)
                .userToken(userToken)          // → Authorization: Token ITOK...
                .build();

        String jobId = session
                .streamSource(Source.kafka("events").format("json")
                        .property("auto.offset.reset", "earliest"))
                .filter("event_type = 'purchase'")          // declarative predicate
                .select("id", "name", "amount")             // project columns
                .sink(Sink.iceberg("prod-iceberg", "analytics.purchases")
                        .upsertKeys("id"))                  // exactly-once upsert by key
                .name("purchases-to-iceberg")
                .parallelism(4)
                .commitInterval(5_000)                      // checkpoint + Iceberg commit cadence (ms)
                .start();                                   // POST /admin/streaming/jobs

        System.out.println("Submitted streaming job: " + jobId);
    }
}
```

`filter` and `select` are declarative and run on the cluster with no extra code. For richer logic, see *Custom transforms* below.

## Step 4 — Build the uber-JAR and submit

Build a single self-contained jar (your `main` + the SDK + Jackson), then run it:

```bash
./gradlew shadowJar
# → build/libs/<project>-all.jar

java -jar build/libs/purchases-to-iceberg-all.jar \
     http://localhost:8080 "$ITDASTREAM_USER_TOKEN"
# → Submitted streaming job: job-3f2a...
```

That single command authenticates, builds the spec, and submits it. The same jar drops straight into a CI step or a scheduler — the token is the only secret it needs.

## Step 5 — Verify and operate

Check status, list jobs, or cancel — with the same token:

```bash
# status of one job
curl -s http://localhost:8080/admin/streaming/jobs/<jobId> \
  -H "Authorization: Token $ITDASTREAM_USER_TOKEN" | jq

# cancel
curl -s -X DELETE http://localhost:8080/admin/streaming/jobs/<jobId> \
  -H "Authorization: Token $ITDASTREAM_USER_TOKEN"
```

Or use the **SDK handle** instead of plain REST:

```java
var handle = session.streamSource(...). ... .submit();  // returns a handle
System.out.println(handle.status());
handle.stop();   // stop and delete the job (DELETE /admin/streaming/jobs/{id})
```

The job also appears in the Admin UI's streaming view, and the Iceberg table `analytics.purchases` starts receiving rows on the first commit (every `commitInterval`). Query it from any Iceberg-compatible engine (Trino, Spark, …) pointed at the same catalog.

## Advanced — custom transforms (uploaded with the job)

`filter` / `select` cover projection and filtering. For arbitrary per-record logic, implement a **MapFunction** (or **FlatMapFunction**) from `itdastream-streaming` and reference it by class name.

`src/main/java/com/example/UpperName.java`:

```java
package com.example;

import com.cloudcheflabs.itdastream.streaming.udf.MapFunction;
import java.util.Map;

public class UpperName implements MapFunction {           // public no-arg ctor required
    @Override
    public Map<String, Object> apply(Map<String, Object> row) {
        Object name = row.get("name");
        if (name != null) row.put("name", name.toString().toUpperCase());
        return row;
    }
}
```

Reference it in the job, and hand the SDK the JAR(s) that contain it:

```java
String jobId = session
        .streamSource(Source.kafka("events").format("json"))
        .map("com.example.UpperName")               // broker loads this class at run time
        .sink(Sink.iceberg("prod-iceberg", "analytics.events"))
        .name("events-upper")
        .parallelism(4)
        .jars("build/libs/my-transforms-all.jar")   // ← uploaded to the cluster, loaded per-job
        .start();
```

**How the class reaches the broker.** There is no copy-to-`lib/` and no restart. On `start()` the SDK:

1. `GET /admin/deps` to see which JARs the cluster already has (dedup by file name + content), then
2. `POST /admin/deps` (header `X-File-Name`, raw jar body) for each missing JAR — the broker stores it in the shared object store under `deps/`, and
3. submits the spec with a `deps: ["my-transforms-all.jar"]` list.

When the controller starts the job, each broker downloads the job's deps from the object store into `data/deps/<jobId>/` and builds a **per-job `URLClassLoader`** over them. `.map("com.example.UpperName")` is then resolved with `Class.forName(name, true, thatLoader)`. The JAR lives with the job: no broker-wide classpath change, no restart, and two jobs can use different versions of the same class without conflict.

**Build the transform JAR** — itdastream is *provided* (already on the broker), so bundle only your own third-party dependencies:

```groovy
// build.gradle for the transform JAR
plugins { id 'java'; id 'com.gradleup.shadow' version '8.3.5' }
dependencies {
    compileOnly files('libs/itdastream-streaming-1.0.0.jar')   // provided by the broker
    // implementation 'com.some:helper:1.2.3'                  // your own deps get shaded in
}
```

```bash
./gradlew shadowJar
# → build/libs/my-transforms-all.jar   (passed to .jars(...) or --jars below)
```

!!! warning "itdastream classes are `compileOnly` for the transform JAR"
    Do **not** bundle `itdastream-streaming` (or its dependencies) into the transform JAR — those classes already exist on the broker classpath, and the per-job loader delegates to it as its parent. Mark them `compileOnly` and shade only your own third-party libraries.

## Submitting from the shell — `bin/submit.sh`

The distribution ships `bin/submit.sh`, a `curl`/`jq` CLI that does the same upload-then-submit flow without writing a Java submitter. Hand it a spec JSON and any transform JARs:

```bash
bin/submit.sh \
  --master localhost:8080 \
  --spec   job.json \
  --jars   build/libs/my-transforms-all.jar \
  --token  "$ITDASTREAM_USER_TOKEN" \
  --wait
```

`job.json` is the declarative spec — exactly what the SDK would `POST`, minus `deps` (submit.sh injects it):

```json
{
  "name": "events-upper",
  "parallelism": 4,
  "kafka": { "topic": "events", "format": "json" },
  "operations": [ { "type": "MAP", "value": "com.example.UpperName" } ],
  "sink": { "type": "table", "connectionId": "prod-iceberg", "table": "analytics.events" },
  "commitIntervalMs": 5000, "checkpointIntervalMs": 5000
}
```

What it does, step by step:

1. Collects JARs from `--jars a.jar,b.jar` and/or `--jar-dirs dir1,dir2`.
2. `GET /admin/deps` and uploads only the JARs the cluster is missing (dedup by name).
3. Injects the uploaded names into the spec's `deps` array.
4. `POST /admin/streaming/jobs` and prints the `jobId`.
5. With `--wait`, polls `GET /admin/streaming/jobs/{id}` until it stops (Ctrl-C just stops watching; the job keeps running).

Auth is the same user token as the SDK — pass `--token ITOK…` or export `ITDASTREAM_USER_TOKEN`. A spec with no `operations` (or only `FILTER`/`SELECT`) needs no `--jars` at all — that is the [no-code auto-sink](streaming-no-code.md) path, which also adds the automatic [`_ingest_ts` hourly partition](streaming-no-code.md#automatic-time-partitioning-_ingest_ts).

## The two JARs at a glance

| | Submitter app (Recipe A) | Custom transform (Advanced) |
| --- | --- | --- |
| **Purpose** | Run the client that builds + `POST`s the job spec | Provide the `MAP` / `FLATMAP` classes the broker loads |
| **Contents** | your `main` + `itdastream-sdk` + Jackson (+ your deps) | your `MapFunction` classes + your third-party deps |
| **itdastream deps** | `implementation` (bundled) | `compileOnly` / provided (already on broker) |
| **Where it runs** | your machine / CI | every broker, in a per-job `URLClassLoader` |
| **How it's delivered** | `java -jar …-all.jar` | uploaded to `/admin/deps` via `.jars(...)` or `submit.sh --jars` (no restart) |
| **Needed when** | always (to submit) | only for `.map(...)` / `.flatMap(...)` |

## See also

- [Streaming SDK](streaming-sdk.md) — full API reference (sources, transforms, sinks, lifecycle).
- [No-Code Kafka to Iceberg](streaming-no-code.md) — submit the same kind of job from the Admin UI, no code.
- [IAM](iam.md) — access keys, user tokens, and policies.
- [Connections](connections.md) — registering the Iceberg / Kafka / JDBC connections sinks reference by id.
