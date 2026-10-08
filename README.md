# Athena JDBC driver 3.8.0: an event-loop wedge, reproduced in one test

A process using the driver's streaming result fetcher can lose a shared Netty event loop for good.
Every HikariCP pool in the process then drains to `total=0` and only a restart recovers it. This
repository reproduces that offline, on a real HikariCP pool, a real driver, real jOOQ and a real
Reactor timeout. The only fake is Athena.

    ./gradlew test --tests '*WedgeTest*'        # the wedge in five forced steps, deterministic
    ./gradlew test --tests '*SoakTest*'         # the same five steps happening on their own
    ./gradlew runChurn                           # Finding 2 on its own, with numbers

Needs Java 21 (jOOQ 3.21). The tests run with `-XX:ActiveProcessorCount=1` so the SDK's shared
event-loop group has two loops, like the affected production process.

## Project layout

```
src/main/java/com/example/athenachurn/
├── mock/   MockAthenaServer: HTTPS Athena that can hold a query RUNNING and hold a stream's headers
├── soak/   HealthCheckSoak + wiring: the application's health probe on the affected pool config
└── churn/  AthenaClientChurnRepro: Finding 2, measured
src/test/java/com/example/athenachurn/soak/
├── WedgeTest.java           read this first
└── HealthCheckSoakTest.java fails when the event loop wedges; prints HikariCP's close reason
```

## The wedge in five steps (`WedgeTest`)

1. **A caller-side timeout abandons a slow query.** The probe wraps a blocking jOOQ fetch in
   `Mono.timeout`. When it fires, Reactor interrupts the worker. The driver rethrows the interrupt
   as a `SQLException` with no SQLState, so HikariCP does not evict; the connection returns to the
   pool. But the statement's asynchronous poll → fetch chain keeps running with nobody waiting.
2. **A later borrow detaches the driver's clients.** HikariCP calls `setNetworkTimeout` around
   `isValid`. The driver's `setApiRequestTimeout` nulls its five SDK clients *without closing them*
   (Finding 2). The orphaned chain still holds the old streaming client; the driver does not.
3. **The query completes; the orphan fetches the stream** with the detached client. The request is
   in flight.
4. **The connection is physically closed.** `ConnectionConfiguration.close()` closes the clients it
   still references — not the detached one — and shuts the completion executor down.
5. **The response arrives.** `GetQueryResultsStreamQueryResultsFactory` attaches its blocking parse
   with `thenApply`, so it runs on whatever thread completes the future. The SDK's hop to the
   executor is rejected (`"Could not complete the service call future on the provided
   FUTURE_COMPLETION_EXECUTOR..."`), it completes synchronously on the Netty event loop, and the
   parse blocks there waiting for bytes only that loop can deliver:

       aws-java-sdk-NettyEventLoop-2-0 WAITING at GetQueryResultsStreamResponseParser.parse(GetQueryResultsStreamResponseParser.java:39)

That is the frame from the production thread dumps. Every client in the JVM shares that loop group
(the driver never sets its own), so one blocked loop starves every connection and every pool.

Remove any one step and it recovers. Each was checked: a close without the detachment aborts the
in-flight stream first (the client is still referenced, so `close()` closes it); the detachment
without a close leaves the executor alive (the configuration creates it once and every client
generation shares it); and a timeout alone does neither.

## The same five steps, unforced (`HealthCheckSoakTest`)

`HealthCheckSoak` runs the application's health probe (same shape, same queries) plus background
tool queries against a randomly slow mock Athena, on the pool configuration the affected
application shipped with before our fix: `maxLifetime` 30 min (scaled down), 3 s acquisition
timeout, `connectionInitSql("select 1")`, no `NetworkTimeoutMillis` pin, streaming fetcher.
HikariCP logs at DEBUG so every close names its reason. Roughly one two-minute run in two wedges.
The test fails on a wedge and prints the timeline and the HikariCP lines that led to it:

    15:54:21.273 [boundedElastic-1]   Query soak-query-50 is executing          <- probe borrows, slow query
    15:54:31.331 [connection-closer]  Closing connection ...@4156624f: (connection was evicted)
    15:54:31.331 [boundedElastic-1]   Operator called default onErrorDropped     <- the probe timeout; same ms
    15:54:32.974 [NettyEventLoop-2-1] Query execution soak-query-50 has state SUCCEEDED
    15:54:33.494 *** WEDGE *** NettyEventLoop-2-1 WAITING at GetQueryResultsStreamResponseParser.parse:39

Step 4 in production is `maxLifetime`: it expired during the probe's long borrow, so HikariCP closed
the connection on return, in the same millisecond as the timeout (`(connection was evicted)`); or a
few seconds after, while the orphan was still polling (`(connection has passed maxLifetime)`). Both
flavours appear in the soak. Production matched: in 5 of 6 probe timeouts the Athena query history
shows a new connection created within a second, and the abandoned probe query completed on the
Athena side 0.42 s after the timeout.

## Workaround in use

`ResultFetcher=GetQueryResults`. The buffered API never parses on the event loop, so the chain
breaks at step 5. That is what we shipped.

## Finding 2: SDK client construction churn on every pool borrow

### Mechanism

`ConnectionConfiguration#setApiRequestTimeout(Duration)` holds five lazily built AWS SDK v2
client instances as fields (`athenaClient`, `athenaSdkClient`, `athenaStreamingClient`,
`s3SdkClient`, `glueSdkClient`). If the new `Duration` differs from the one already held, it sets
all five fields to `null` without closing them. The next call that needs a client rebuilds it.

`AthenaConnection#setNetworkTimeout` routes straight into this method. HikariCP calls
`setNetworkTimeout` on every pool borrow that runs validation (once to raise it to the pool's
`validationTimeout`, once to restore it afterward) and once more, with a hardcoded 15 seconds,
when it retires a connection. Because the driver's default reported network timeout (`0`,
meaning "practically infinite") almost never equals HikariCP's `validationTimeout`, every one of
these calls discards and rebuilds all five clients.

### Reproduction and measured numbers

```bash
./gradlew runChurn
```

`AthenaClientChurnRepro` runs 40 pooled borrows under default settings, then 40 more with the
driver's `NetworkTimeoutMillis` connection property pinned to the pool's `validationTimeout` (so
the equals check short-circuits and the clients are never discarded). It counts distinct SDK
client instances by identity, via reflection into `ConnectionConfiguration`'s private fields.
Real output from this repro:

```
Scenario A: default Hikari validation timeout, driver default network timeout (BUG)
borrows    | athenaClient     | athenaSdkClient  | s3SdkClient      | athenaStreamingClient | glueSdkClient
5          | 5                | 5                | 5                | 5                | 0
20         | 20               | 20               | 20               | 20               | 0
40         | 40               | 40               | 40               | 40               | 0

Scenario B: NetworkTimeoutMillis pinned to Hikari's validationTimeout (WORKAROUND)
borrows    | athenaClient     | athenaSdkClient  | s3SdkClient      | athenaStreamingClient | glueSdkClient
5          | 1                | 1                | 1                | 1                | 0
20         | 1                | 1                | 1                | 1                | 0
40         | 1                | 1                | 1                | 1                | 0

SUMMARY
BUG REPRODUCED: 40 borrows created 40 distinct AthenaAsyncClient instances; with NetworkTimeoutMillis pinned: 1
```

40 borrows built 40 distinct `AthenaAsyncClient` (and `athenaClient`, `s3SdkClient`,
`athenaStreamingClient`) instances, one per borrow, every one abandoned without being closed.
Pinning `NetworkTimeoutMillis` to the pool's validation timeout holds that at 1 instance across
all 40 borrows. `glueSdkClient` stays at 0 throughout because nothing in this repro calls a Glue
operation, so it is never lazily created either way.

## Expected driver behavior

Three changes in the driver would each break the chain on their own; together they close it.

1. **Parse off the event loop.** `GetQueryResultsStreamQueryResultsFactory` attaches its blocking
   parse with `thenApply(...)`, so it runs on whichever thread completes the response future. It
   should use `thenApplyAsync(...)` on a live executor, with a safe fallback when that executor is
   gone: fail the future, never block the completing thread. `WedgeTest` step 5 is the target for this.
2. **Close what you detach.** `ConnectionConfiguration.setApiRequestTimeout` nulls the five SDK
   clients without closing them, so a request in flight on an old client outlives
   `ConnectionConfiguration.close()`, which only closes the clients it still references. Either
   close the old clients or keep referencing them until they are idle. `AthenaClientChurnRepro`
   measures this; `WedgeTest` step 2 is where it enters the chain.
3. **Give an abandoned statement a way to stop.** After the caller's thread is interrupted, the
   statement's poll → fetch chain keeps running and issues `GetQueryResultsStream` for a result
   nobody will read. `AsyncQueryResults` has no close or abort, and `Statement.close()` only
   reaches `StopQueryExecution` while the statement is still marked executing. Cancel the chain
   when the statement is closed or its thread interrupted, and close the streamed HTTP response
   instead of leaving it open.

