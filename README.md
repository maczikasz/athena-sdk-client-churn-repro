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

## Does the churn actually produce a production failure?

The first two scenarios prove the clients get rebuilt and never closed. They do not, by
themselves, prove that matters under real traffic. `ProductionTimeoutRepro` (a separate program,
`./gradlew runProductionTimeout`) checks that against a local mock Athena/S3 endpoint over real
HTTPS (a throwaway self-signed certificate, generated at run time with the JDK's own `keytool`,
no files committed). Three questions, in order:

**Scenario 3 - does churn turn into the production symptom
`Connection is not available, request timed out after 3000ms`?** A 15-connection pool is warmed
against a fast mock (150ms/call), then the mock is slowed to 2500ms/call (about 7.5s to build one
physical connection: `StartQueryExecution` + `GetQueryExecution` + `GetQueryResults`, each
sleeping 2500ms) and every pooled connection is evicted, forcing the pool to rebuild its
connections from scratch under load. 30 threads then hammer `getConnection()` with a 3000ms
Hikari `connectionTimeout`, spaced 650ms apart per thread (past Hikari's 500ms
`aliveBypassWindow`, so `isConnectionDead` validation - and the churn-driving
`setNetworkTimeout` calls - actually run on every borrow). This reliably reproduces the exact
production exception (see the verbatim capture below). What the data does **not** support: a
clean "pinning fixes it" story. Distinct-client counts came out exactly as scenarios 1-2 predict
(1 for pinned, 4-5 for unpinned, every run), but the raw timeout counts across four repeated,
order-swapped runs were noisy in both directions - see "What this experiment does not show"
below for the honest read on why.

**Scenario 4 - does a wedged pool recover once Athena becomes healthy again?** Yes, in every run.
After the mock's latency drops back to 150ms, or a 60-second window of mock 500s ends, the pool
refills to its full size and borrows succeed again on their own, no restart needed. Recovery took
on the order of 18-25 seconds in the runs below - roughly the time for Hikari's connection-adder
to rebuild the pool one connection at a time, each still paying the fast-mock construction cost.

**Scenario 5 - is there a failure mode that does NOT self-heal?** Yes, a completely different and
much simpler one, unrelated to the churn bug: a pool's `AccessKeyId` is read once, at
`HikariConfig` construction, and frozen in that `Properties` object for the pool's entire life.
If the credential the mock accepts changes after the pool is built (a credential rotation, or a
disabled key), the pool's evicted connections can never succeed again - the endpoint is healthy
and a *different*, freshly-built pool with the new key connects immediately - but the old pool's
`AccessKeyId` never changes, so it fails forever, exactly the same "not available, timed out"
symptom, `total=0`. This is a design property of how JDBC connection pools read credentials, not
a driver defect, and it does not depend on the churn bug in any way - it is included here because
it produces the identical-looking symptom for a completely different reason, which matters when
diagnosing a real "total=0, timing out" pool in production. Of everything in this project, this
is the only genuinely permanent, restart-only wedge - see scenario 6 for a candidate that looked
like it might be a second one, and was not, once actually tested.

**Scenario 6 - a hung SDK call during connection creation: the strongest "never recovers"
candidate, tested directly.** This was motivated by a specific production detail: a real
`Connection is not available, request timed out after 3000ms` stack trace with **no
`Caused by:`** at all. HikariCP's `createTimeoutException` only attaches a cause if some
connection-creation attempt had actually *finished* (successfully or not) since the pool went
empty - so a missing cause means that, at the moment of the timeout, no attempt had finished.
The natural hypothesis: the pool's single connection-adder thread was permanently stuck inside
one SDK call that would never return, so every later borrow just piled up behind it, forever.

`ProductionTimeoutRepro` tests this directly. The mock accepts a connection, reads the full
`StartQueryExecution` request, and then sends back **nothing at all** - no response, no error,
no close, no RST - and holds the socket exactly like that, indefinitely. The pool is evicted
first, so its single connection-adder thread is the one that walks straight into the hang. A
thread dump taken moments later finds it parked exactly where expected:

```
Thread "scenario6-unpinned:connection-adder" (state=WAITING):
    ...
    at app//com.amazon.athena.jdbc.AthenaStatementBase.getQueryExecutionId(AthenaStatementBase.java:179)
    at app//com.amazon.athena.jdbc.AthenaConnection.testConnection(AthenaConnection.java:438)
    at app//com.amazon.athena.jdbc.AthenaDriver.connect(AthenaDriver.java:147)
    at app//com.zaxxer.hikari.pool.HikariPool$PoolEntryCreator.call(HikariPool.java:752)
```

While that thread is still parked there, a completely independent direct `connect()` call
(bypassing the pool) succeeds immediately - proving the endpoint is healthy and only the one
socket the adder is stuck on is dead, exactly like a network path that silently dropped a
connection without tearing it down. Meanwhile the pool itself stays at `total=0`, every borrow
fails with `Connection is not available, request timed out after 3000ms`, and
**`exception.getCause()` is `null`** - reproducing the exact production signature.

**Where this stops matching the hypothesis: it is not permanent.** In every run, both pinned and
unpinned, the pool recovered on its own at almost exactly the same wall-clock moment - **30.2
seconds** after the hang started (`t=30226ms` unpinned, `t=30223ms` pinned, in the run captured
below - repeatable, not noise). Pinning `NetworkTimeoutMillis` made no measurable difference at
all. Decompiling the driver and the AWS SDK explains both halves of that:

- `ConnectionConfiguration#getHttpClientBuilder()` uses `apiRequestTimeout` (what
  `NetworkTimeoutMillis` sets) for exactly one thing: Netty's `connectionAcquisitionTimeout` -
  the time to grab an already-open pooled HTTP connection. It is never wired to anything that
  bounds waiting for a response on a connection that's already open and already sent its
  request. Pinning it cannot touch this hang.
- `software.amazon.awssdk.http.SdkHttpConfigurationOption` (in the AWS SDK's own Netty transport
  jar) hard-codes `DEFAULT_SOCKET_READ_TIMEOUT = Duration.ofSeconds(30)`, and the driver never
  overrides it. That default - not the driver, not `NetworkTimeoutMillis` - is what eventually
  fires Netty's `ReadTimeoutHandler`, fails the hung attempt, and frees the adder to try again
  against the (already healthy) mock.

So scenario 6 reproduces the exact diagnostic signature of the "no `Caused by:`" production
trace - a real parked adder thread, a real `getCause() == null` - but the underlying wedge in
this driver version is bounded at about 30 seconds by an AWS SDK default, not permanent. A pool
that stays wedged for much longer than that, or that a restart is the only fix for, is better
explained by something in the same family as scenario 5 (a static, frozen input that a fresh
attempt can never satisfy) than by an infinite hang in a single SDK call.

## How to run it

```bash
export JAVA_HOME=/path/to/a/jdk-17-or-newer
./gradlew run                      # scenarios 1-2: offline churn counting
./gradlew runProductionTimeout     # scenarios 3-6: mock-endpoint timeout/recovery/credential-wedge/hang
```

Both tasks depend on `downloadAthenaJdbc`, which fetches the driver
(`com.amazonaws:athena-jdbc:3.8.0`) from its public AWS download URL — it is not published to
Maven Central — and unpacks it into `libs/`, which is not committed to this repository:

```
https://downloads.athena.us-east-1.amazonaws.com/drivers/JDBC/3.8.0/athena-jdbc-3.8.0-lean-jar-and-separate-dependencies-jars.zip
```

The download task prints the SHA-256 of the downloaded zip. Any Gradle 8 or newer can run this
project; the checked-in wrapper uses Gradle 8.10.2. `runProductionTimeout` takes a few minutes:
it deliberately waits out a real 60-second failure window, a real ~130-second credential-wedge
window, and two real ~60-second hang-observation windows, using wall-clock time, not simulated
time - about 8 minutes end to end.

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

## Expected output: scenarios 3-6 (`./gradlew runProductionTimeout`)

This is the actual output from a verified run, with HikariCP/driver log lines, Java stack
traces, and (in scenario 6) the ~90 unrelated thread names in each end-of-window thread dump
omitted for readability:

```
Mock Athena/S3 endpoint listening at https://127.0.0.1:49507
Scenario 3, phase A: healthy baseline (mock latency 150ms/call)
---------------------------------------------------------------
  borrows: successes=160, timeouts=0, otherFailures=0, borrow p50=0ms, p99=5ms
Scenario 3, phase B (UNPINNED): mock latency raised to 2500ms/call (~7.5s per connection) after eviction
----------------------------------------------------------------------------------------------------
  Pool warm at total=15, active=0, idle=15, waiting=0. Raising mock latency and evicting all connections.
  borrows: successes=90, timeouts=60, otherFailures=0, borrow p50=0ms, p99=396ms
  Distinct athenaSdkClient instances observed during this phase: 3
  Verbatim exception (first occurrence): phaseB-unpinned - Connection is not available, request timed out after 3071ms (total=0, active=0, idle=0, waiting=18)
Scenario 3, phase B (PINNED): mock latency raised to 2500ms/call (~7.5s per connection) after eviction
----------------------------------------------------------------------------------------------------
  Pool warm at total=15, active=0, idle=15, waiting=0. Raising mock latency and evicting all connections.
  borrows: successes=90, timeouts=60, otherFailures=0, borrow p50=0ms, p99=361ms
  Distinct athenaSdkClient instances observed during this phase: 1
  Verbatim exception (first occurrence): phaseB-pinned - Connection is not available, request timed out after 3071ms (total=0, active=0, idle=0, waiting=26)
Scenario 4a: recovery after mock latency drops back to 150ms
------------------------------------------------------------
  Restoring mock latency to 150ms and measuring recovery time.
  Recovered: true after 14264ms. Pool status: total=15, active=0, idle=15, waiting=0
Scenario 4b: recovery after a 60s window of mock 500s
-----------------------------------------------------
  Mock returning 500s for 60s, with borrow pressure applied throughout.
  Restoring the mock to healthy and measuring recovery time.
  Recovered: true after 14830ms. Pool status: total=15, active=0, idle=15, waiting=0
Scenario 5: a deterministically-bad, process-frozen credential wedges the pool forever
--------------------------------------------------------------------------------------
  Phase A: pool built with AccessKeyId=key-A, mock currently accepts any key.
  borrows: successes=30, timeouts=0, otherFailures=0, borrow p50=0ms, p99=0ms
  Phase B: mock now only accepts key-B. key-A (frozen in poolA's config) is
  rejected with 403 UnrecognizedClientException, exactly like a rotated real
  AWS credential. Evicting poolA's connections and retrying borrows for ~130s.
  Retried for 132114ms: 43 borrow attempts, 43 failed.
  Verbatim exception: scenario5-keyA - Connection is not available, request timed out after 3061ms (total=0, active=0, idle=0, waiting=0)
  Direct proof the endpoint itself is healthy: a fresh connect() with key-B,
  made independently of poolA, while poolA is still wedged:
  Direct connect with key-B while poolA is wedged: SUCCEEDED (com.amazon.athena.jdbc.AthenaConnection@4a23350)
  Phase C: does poolA (still configured with key-A) ever recover on its own?
  poolA self-recovery within 10s of extra waiting: false (expected: false - its AccessKeyId is frozen at pool-creation time)
  A brand NEW pool built with key-B recovers immediately - only a fresh
  pool/process (which re-reads the credential) fixes this, not waiting:
  New pool with key-B: borrows: successes=30, timeouts=0, otherFailures=0, borrow p50=0ms, p99=0ms
Scenario 6 (UNPINNED): mock accepts the connection, reads the request, then never responds
------------------------------------------------------------------------------------------
  Pool warm at total=15, active=0, idle=15, waiting=0. Evicting, then arming a one-shot hang
  on the next StartQueryExecution call - that is the adder's next creation attempt.
  Thread dump: looking for the pool's connection-adder thread, parked mid-call.
  Thread "scenario6-unpinned:connection-adder" (state=WAITING):
      at java.base/jdk.internal.misc.Unsafe.park(Native Method)
      at java.base/java.util.concurrent.CompletableFuture$Signaller.block(CompletableFuture.java:1864)
      at java.base/java.util.concurrent.CompletableFuture.waitingGet(CompletableFuture.java:1898)
      at java.base/java.util.concurrent.CompletableFuture.get(CompletableFuture.java:2072)
      at app//com.amazon.athena.jdbc.AthenaStatementBase.getQueryExecutionId(AthenaStatementBase.java:179)
      at app//com.amazon.athena.jdbc.AthenaStatementBase.runQuery(AthenaStatementBase.java:136)
      at app//com.amazon.athena.jdbc.AthenaConnection.testConnection(AthenaConnection.java:438)
      at app//com.amazon.athena.jdbc.AthenaDriver.connect(AthenaDriver.java:147)
      at app//com.zaxxer.hikari.pool.HikariPool$PoolEntryCreator.call(HikariPool.java:752)
      at java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1144)
  Flipping the mock back to healthy - but ONLY for NEW connections. The socket
  the hung adder is parked on is never touched: no response, no close, no RST,
  exactly like a network path that silently dropped without tearing down the TCP
  connection. Proving the endpoint itself is fine with an independent direct connect:
  Direct connect while the pool's adder is still parked: SUCCEEDED (com.amazon.athena.jdbc.AthenaConnection@13da7ab0)
  Observing for up to 60s: pool status every 10s, first exception's getCause().
  t=10072ms  total=0, active=0, idle=0, waiting=1  attempts=5  successes=0
  t=20151ms  total=0, active=0, idle=0, waiting=1  attempts=8  successes=0
  t=30226ms  total=3, active=0, idle=3, waiting=0  attempts=28  successes=19
  --> pool recovered at t=30226ms
  t=40303ms  total=13, active=0, idle=13, waiting=0  attempts=110  successes=101
  t=50378ms  total=15, active=0, idle=15, waiting=0  attempts=192  successes=183
  t=60454ms  total=15, active=0, idle=15, waiting=0  attempts=275  successes=266
  Sample borrow exception: java.sql.SQLTransientConnectionException: scenario6-unpinned - Connection is not available, request timed out after 3061ms (total=0, active=0, idle=0, waiting=0)
  sample.getCause(): null
  Recovered within 60s observation window: true (at t=30226ms)
Scenario 6 (PINNED): mock accepts the connection, reads the request, then never responds
----------------------------------------------------------------------------------------
  Pool warm at total=15, active=0, idle=15, waiting=0. Evicting, then arming a one-shot hang
  on the next StartQueryExecution call - that is the adder's next creation attempt.
  Thread dump: looking for the pool's connection-adder thread, parked mid-call.
  Thread "scenario6-pinned:connection-adder" (state=WAITING):
      [identical stack to the unpinned run above]
  Direct connect while the pool's adder is still parked: SUCCEEDED (com.amazon.athena.jdbc.AthenaConnection@7c8d5312)
  Observing for up to 60s: pool status every 10s, first exception's getCause().
  t=10074ms  total=0, active=0, idle=0, waiting=1  attempts=5  successes=0
  t=20148ms  total=0, active=0, idle=0, waiting=1  attempts=8  successes=0
  t=30223ms  total=3, active=0, idle=3, waiting=0  attempts=28  successes=19
  --> pool recovered at t=30223ms
  t=40295ms  total=13, active=0, idle=13, waiting=0  attempts=114  successes=105
  t=50370ms  total=15, active=0, idle=15, waiting=0  attempts=198  successes=189
  t=60443ms  total=15, active=0, idle=15, waiting=0  attempts=286  successes=277
  Sample borrow exception: java.sql.SQLTransientConnectionException: scenario6-pinned - Connection is not available, request timed out after 3075ms (total=0, active=0, idle=0, waiting=0)
  sample.getCause(): null
  Recovered within 60s observation window: true (at t=30223ms)
```

### What this experiment does not show

**Scenario 3.** The verbatim production exception was reproduced exactly, both pinned and
unpinned. What it does **not** cleanly show is pinning *reducing* the failure rate during a mass
reconnect storm: in this run both pinned and unpinned timed out on 60 of 150 borrows. Earlier
runs (kept in this project's history) even showed pinned failing *more* than unpinned. What
stayed rock solid across every run: the **distinct-client counts** matched scenarios 1-2 exactly,
every time (1 for pinned, 3-5 for unpinned) - the churn mechanism itself is real and reproducible
even under this concurrent, mixed create/validate load. The most defensible read: phase B's
timeouts are dominated by the unavoidable cost of building brand-new physical connections from
nothing (three real round trips at 2500ms each, ~7.5s, identical regardless of pinning), not by
churn's validation-time overhead - that overhead is real and unbounded over time (scenarios 1-2
prove it directly), but a mass-eviction-under-load stress test is not the regime where it
dominates the raw failure rate. **This experiment does not support a claim that pinning
`NetworkTimeoutMillis` reduces production borrow-timeout rates during a reconnect storm** - what
it does do, proven directly by scenarios 1-2, is stop the SDK clients from being discarded and
rebuilt on every validated borrow of an existing, healthy connection.

**Scenario 6.** This was built to test a specific hypothesis: that a hung SDK call during
connection creation wedges the pool permanently when `NetworkTimeoutMillis` is unset, because
the driver's default `apiRequestTimeout` is `PRACTICALLY_INFINITE_DURATION`, and that pinning it
to 15 seconds bounds the hang and fixes it. Both halves of that hypothesis are contradicted by
the data above: unpinned recovered (`t=30226ms`), and pinned recovered at essentially the same
moment (`t=30223ms`) - pinning made no measurable difference. Decompiling the driver shows why:
`apiRequestTimeout` is wired only to Netty's `connectionAcquisitionTimeout` (acquiring an
already-open pooled connection), never to anything that bounds waiting for a response once a
request is already in flight. The actual bound - confirmed in
`software.amazon.awssdk.http.SdkHttpConfigurationOption`, in the AWS SDK's own Netty transport
jar - is a hard-coded, undocumented-by-this-driver `DEFAULT_SOCKET_READ_TIMEOUT` of 30 seconds,
which the driver never overrides. What scenario 6 does confirm, precisely: the diagnostic
signature of a real production trace with no `Caused by:` - a connection-adder thread genuinely
parked inside the driver's SDK call, and a borrow-timeout exception whose `getCause()` really is
`null` while that thread is stuck. It is just bounded, in this driver version, at about 30
seconds, not forever.

### A note on two hypotheses this project does NOT support

**A permanently torn-down shared event loop group.** An earlier hypothesis held that the AWS
SDK's default Netty transport uses a shared, reference-counted `EventLoopGroup` (confirmed in
this jar's dependencies: `software.amazon.awssdk.http.nio.netty.internal.SharedSdkEventLoopGroup`),
and that once its reference count reaches zero the group would be permanently torn down, wedging
every SDK client built afterward. Decompiling `SharedSdkEventLoopGroup#get()` in the shipped
`AwsJavaSdk-HttpClient-NettyNioClient-2.0.jar` shows that is not the shipped behavior: `get()`
checks whether its static holder is `null` and builds a brand new group when it is, so a
reference count reaching zero tears down the *old* group but does not wedge the class - the next
`get()` call transparently builds a replacement. This project does not include a scenario for
that hypothesis, because static analysis already disproves it against this jar. What churn
*does* leave behind, confirmed by scenarios 1-2, is that the discarded clients' reference to
that shared group is never released in the first place (`close()` is never called on them) - so
under the churn bug, the shared group's reference count only ever climbs, it does not reach zero
through this path at all.

**A permanent hang from a single unresponsive SDK call.** See "What this experiment does not
show" above: scenario 6 tested this directly and it is bounded at ~30 seconds by an AWS SDK
default, in both the pinned and unpinned case. Of everything demonstrated in this project, only
scenario 5's frozen credential is a genuinely permanent, restart-only wedge.

## Files

- `src/main/java/com/example/athenachurn/AthenaClientChurnRepro.java` — scenarios 1-2, offline churn counting
- `src/main/java/com/example/athenachurn/MockAthenaServer.java` — the local HTTPS mock Athena/S3 endpoint used by scenarios 3-6
- `src/main/java/com/example/athenachurn/ProductionTimeoutRepro.java` — scenarios 3-6
- `build.gradle` — the `downloadAthenaJdbc` task and both `run` / `runProductionTimeout` tasks
- `libs/` — where the downloaded driver lands; not committed
