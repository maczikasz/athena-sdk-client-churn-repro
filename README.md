# Athena JDBC driver 3.8.0: SDK client construction churn under HikariCP

This project reproduces a defect in the AWS Athena JDBC driver, version 3.8.0.
It needs no AWS account and makes no network call to AWS. It runs offline.

## The bug

`com.amazon.athena.jdbc.configuration.ConnectionConfiguration#setApiRequestTimeout(Duration)`
holds five AWS SDK v2 client instances as lazily built fields: `athenaClient`,
`athenaSdkClient`, `athenaStreamingClient`, `s3SdkClient`, and `glueSdkClient`.
When the method runs, it does one of two things:

- If the new `Duration` equals the current one, it returns immediately and keeps every client.
- Otherwise, it sets all five client fields to `null`. It does **not** close the old clients
  first. The next call that needs a client rebuilds it from scratch.

`AthenaConnection#setNetworkTimeout(Executor, int)` calls this method on every invocation.
A positive millisecond value becomes `Duration.ofMillis(millis)`. A value of `0` becomes a
constant the driver calls `PRACTICALLY_INFINITE_DURATION`, and `getNetworkTimeout()` reports
that constant back as `0`. So a driver connection with no `NetworkTimeoutMillis` property set
always reports its network timeout as `0`.

HikariCP calls `Connection#setNetworkTimeout` in the normal course of managing a pool, not only
when a caller asks for it:

- `PoolBase#isConnectionDead` sets the connection's network timeout to the pool's
  `validationTimeout` before calling `isValid()`, then restores the connection's original
  network timeout afterward. That is two calls to `setNetworkTimeout` per validation.
- `PoolBase#quietlyCloseConnection` sets the network timeout to a hardcoded 15 seconds before
  closing a retired connection. That is one more call.

With the driver's default network timeout reporting as `0`, and any `validationTimeout` other
than exactly `15000`, these three values — `0`, the pool's `validationTimeout`, and `15000` —
are pairwise different. Every one of HikariCP's `setNetworkTimeout` calls carries a value that
differs from the value `ConnectionConfiguration` currently holds, so the equals check never
short-circuits, and the driver drops all five SDK clients. A single pool borrow that runs
validation triggers this about three times; every connection HikariCP retires adds one more.

The clients that get discarded are never closed. In this driver's earlier releases (before
3.8.0), each dropped client left its executor threads running forever: an unbounded thread
leak. Release 3.8.0 fixed that specific symptom by giving the internal executors
`allowCoreThreadTimeOut(true)`, so idle core threads now time out and the executor becomes
eligible for garbage collection once nothing references it. Constructing a client with no
outstanding request never starts a thread in the first place — a thread only appears once a
request is submitted to the client's executor — which is why this reproduction, which builds
clients but never issues a query, shows zero matching threads throughout.

What is still true in the shipped 3.8.0 jar is the **construction churn**: every pool borrow
that runs validation rebuilds up to five new SDK client objects, none of which existed a moment
before and none of which get closed. Under real traffic, where borrows happen continuously and
some of them do issue queries, this means the driver is repeatedly paying the cost of
constructing new `AthenaAsyncClient`, `S3AsyncClient`, `GlueAsyncClient`, and streaming client
instances — each of which builds its own Netty event loop group and connection pool — instead
of reusing the ones it already has. Under load, this construction work competes with the pool
for CPU and can slow down connection creation across the board.

The Athena JDBC driver 3.8.0 release notes state that the driver "properly shuts down internal
thread pools when the connection is closed or when the network timeout is changed." The first
half of that claim holds. The second half does not: changing the network timeout still
discards the SDK clients without closing them, and — separately from thread pool shutdown —
the discarded clients are rebuilt from scratch on the next use instead of being reused.

This is a follow-up to an earlier thread-leak report for this driver, discussed with the
Athena driver team in January 2026 and reproduced at
<https://github.com/maczikasz/athena-thread-leak-repro>. That report covered the pre-3.8
thread leak. This project demonstrates what remains after the 3.8.0 fix: the clients no longer
leak threads, but they are still discarded and rebuilt without being closed, so the driver
pays a real construction cost it does not need to pay.

## How the reproduction works, offline

No AWS credentials and no network access to AWS are needed:

- The JDBC URL is `jdbc:athena://` with `Region=eu-central-1`, `Workgroup=primary`,
  `Catalog=AwsDataCatalog`, `Database=default`, `AccessKeyId=dummy`, `SecretAccessKey=dummy`,
  and — critically — `ConnectionTest=false`. That last property stops the driver from running
  its own connect-time `select 1`, which would otherwise be the only AWS call in this program.
- `AthenaConnection#isValid` is a plain field read, not a network call, so HikariCP's
  validation step passes without contacting AWS.
- Constructing an AWS SDK v2 async client resolves no credentials by itself. Credential
  resolution happens when a request is signed, which never happens here, so the dummy
  credentials above are never used to sign anything.

The program borrows a connection from a single-connection HikariCP pool, unwraps it down to
the driver's own `com.amazon.athena.jdbc.AthenaConnection`, and reads the private
`configuration` field (a `ConnectionConfiguration`) by reflection. It calls
`getAthenaClient()` to force lazy construction — the same thing issuing a query would trigger
— then reads all five client fields by reflection and records
`System.identityHashCode(...)` for each into a per-field set. It repeats this for 40 borrows,
sleeping 650ms between borrows so HikariCP's 500ms `aliveBypassWindow` elapses and
`isConnectionDead` actually runs its validation path on every borrow.

Two scenarios run back to back:

- **Scenario A (bug present)**: default HikariCP settings, no `NetworkTimeoutMillis` property.
  HikariCP's `validationTimeout` (5000ms here) never matches the driver's reported `0`, or
  HikariCP's hardcoded 15000ms close-time value, so every validated borrow rebuilds all the
  clients.
- **Scenario B (workaround)**: the data source property `NetworkTimeoutMillis=15000` is set,
  matching HikariCP's `validationTimeout`, which is also set to 15000ms in this scenario. Now
  every `setNetworkTimeout` call HikariCP makes carries a value the driver's configuration
  already holds, the equals early-return fires every time, and the clients are never discarded.

## How to run it

```bash
export JAVA_HOME=/path/to/a/jdk-17-or-newer
./gradlew run
```

The `run` task depends on `downloadAthenaJdbc`, which fetches the driver
(`com.amazonaws:athena-jdbc:3.8.0`) from its public AWS download URL — it is not published to
Maven Central — and unpacks it into `libs/`, which is not committed to this repository:

```
https://downloads.athena.us-east-1.amazonaws.com/drivers/JDBC/3.8.0/athena-jdbc-3.8.0-lean-jar-and-separate-dependencies-jars.zip
```

The download task prints the SHA-256 of the downloaded zip. Any Gradle 8 or newer can run this
project; the checked-in wrapper uses Gradle 8.10.2.

## Expected output

This is the actual output from a verified run (log lines from HikariCP and the driver's own
`WARN` about an unsupported `Executor` parameter are omitted for readability):

```
Athena JDBC driver 3.8.0 - SDK client construction churn reproduction
========================================================================

Scenario A: default Hikari validation timeout, driver default network timeout (BUG)
--------------------------------------------------------------------------------
borrows    | athenaClient     | athenaSdkClient  | s3SdkClient      | athenaStreamingClient | glueSdkClient
5          | 5                | 5                | 5                | 5                | 0
20         | 20               | 20               | 20               | 20               | 0
40         | 40               | 40               | 40               | 40               | 0
Threads matching 'athena-jdbc'/'aws-java-sdk-NettyEventLoop' right after scenario A: 0

Scenario B: NetworkTimeoutMillis pinned to Hikari's validationTimeout (WORKAROUND)
--------------------------------------------------------------------------------
borrows    | athenaClient     | athenaSdkClient  | s3SdkClient      | athenaStreamingClient | glueSdkClient
5          | 1                | 1                | 1                | 1                | 0
20         | 1                | 1                | 1                | 1                | 0
40         | 1                | 1                | 1                | 1                | 0

Waiting 12 seconds before the final thread count, to show thread counts stay bounded
(3.8.0 fixed the pre-3.8 thread leak with allowCoreThreadTimeOut; this repro is about
client CONSTRUCTION churn, a separate, still-open defect).
Threads matching 'athena-jdbc'/'aws-java-sdk-NettyEventLoop' - start: 0, end: 0

SUMMARY
-------
BUG REPRODUCED: 40 borrows created 40 distinct AthenaAsyncClient instances; with NetworkTimeoutMillis pinned: 1
```

`glueSdkClient` stays at `0` in both scenarios: this program only forces construction through
`getAthenaClient()`, which does not touch the Glue client. The other four fields — the two
Athena clients, the S3 client, and the streaming client — all show the same pattern: one new
instance per borrow in Scenario A, growing linearly and without bound as borrows continue; a
constant `1` instance in Scenario B, for the entire run.

The thread counts stay at `0` throughout, in both scenarios. This program never issues a
query, and this driver's clients only start executor threads when a request is submitted to
them — not at construction time — so no thread activity is expected here even in the buggy
scenario. The `0` is consistent with the 3.8.0 fix for the earlier thread-leak defect: what
this reproduction demonstrates is a different, still-open defect — construction churn, not a
thread leak.

## Files

- `src/main/java/com/example/athenachurn/AthenaClientChurnRepro.java` — the reproduction
- `build.gradle` — the `downloadAthenaJdbc` task and dependencies
- `libs/` — where the downloaded driver lands; not committed
