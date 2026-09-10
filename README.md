# Athena JDBC driver 3.8.0: an event-loop wedge reproduced offline, and its ingredients

This project reproduces two independent defects in the AWS Athena JDBC driver, version 3.8.0,
and demonstrates two code-level workarounds for the first one. Everything runs offline: a local
HTTPS mock stands in for the Athena service, so no AWS account and no network access is needed.

- **Finding 1**: streamed query results can permanently block the Netty event-loop thread that
  is completing the response, wedging every unrelated request that shares that thread.
- **Finding 2**: the driver rebuilds all five of its internal AWS SDK clients on almost every
  HikariCP pool borrow, because `setNetworkTimeout` calls a method that discards them.

## Project layout

```
com.example.athenachurn
├── mock/        MockAthenaServer — local HTTPS stand-in for the Athena endpoints
├── datasource/  DataSource adapters that wire the scenarios into real Hikari pools
├── streaming/   Finding 1: StreamingResponseStallRepro, TimeoutInterruptRepro
├── soak/        The production sequence end to end: HealthCheckSoak (read this one first)
└── churn/       Finding 2: AthenaClientChurnRepro
```

## Finding 1: streaming result parsing can starve a Netty event loop

### Mechanism

`GetQueryResultsStreamQueryResultsFactory` calls `getQueryResultsStream(request,
AsyncResponseTransformer.toBlockingInputStream())` and attaches its result parser with a plain
`CompletableFuture.thenApply(...)`, not `thenApplyAsync(...)`:

```
at app//com.amazon.athena.client.results.GetQueryResultsStreamQueryResultsFactory.lambda$create$1(GetQueryResultsStreamQueryResultsFactory.java:44)
at java.base/java.util.concurrent.CompletableFuture$UniApply.tryFire(CompletableFuture.java:646)
```

A plain `thenApply` runs on whichever thread completes the future. The attached continuation,
`GetQueryResultsStreamResponseParser.parse` at line 39, calls `BufferedReader.readLine()` — a
blocking read. If the completing thread is a Netty event-loop thread, the read waits for bytes
that can only be delivered by more `channelRead` callbacks on that same thread. The loop never
runs again: the read waits forever for data only it could deliver.

No stalled server is required. Even a complete, well-formed response wedges the loop: the parser
starts inside the `channelRead` that delivers the response headers, and the remaining body —
including the end-of-stream marker — can only be handed to it by later events on the same thread.
Netty's read timeout cannot rescue it either: that handler is scheduled on the blocked loop.

### How this happens in production

The driver does configure `SdkAdvancedAsyncClientOption.FUTURE_COMPLETION_EXECUTOR` on all its
SDK clients, pointing at its own `athena-jdbc-*` thread pool (a bounded `ThreadPoolExecutor`:
core `max(8, CPUs)`, max `max(64, 2×CPUs)`, queue capacity 1000, abort on rejection). So normally
the parse hops off the event loop. But the AWS SDK's `MakeAsyncHttpRequestStage` only *attempts*
that hop via `handleAsync`; if the executor rejects the task, the SDK logs a DEBUG message
("Could not complete the service call future on the provided FUTURE_COMPLETION_EXECUTOR...") and
falls back to completing the future synchronously — on the Netty event-loop thread.

Production thread dumps show the telltale signs of exactly this fallback: the parser stack
contains no `Completion.run`/`UnmanagedExecutor.execute` hop frames, and the JVM contains **zero**
`athena-jdbc-*` threads. (Zero threads alone is ambiguous — the pool allows core-thread timeout,
so an idle pool looks the same — but a live idle pool would have accepted the hop, and the missing
hop frames rule that out.) The driver shuts that pool down in
`ConnectionConfiguration.close()` (via `AthenaConnection.close()`), unconditionally, without
waiting for in-flight requests — and the driver offers no way to abort an in-flight
`GetQueryResultsStream` (`AsyncQueryResults` has no close/cancel; once requested, the HTTP
response is delivered to completion no matter what the JDBC consumer does).

So the real-world trigger is a close racing an in-flight stream. HikariCP never closes a borrowed
connection, but it does close one on return when `maxLifetime` expired during the borrow — and a
health probe that waits 30 s on a slow query is exactly such a borrow. The soak below shows the
full sequence on the real stack, and production data matched it: a probe times out, its thread is
interrupted, the connection comes back and HikariCP closes it in the same millisecond
(`Closing connection ...: (connection was evicted)`). That close shuts the completion executor,
while the abandoned statement's poll → fetch chain is still running on a client the driver had
already detached (Finding 2), so `close()` never reached it. When that chain's response arrives,
the completion is rejected by the shut-down pool, the SDK falls back to synchronous completion, and
the driver's blocking parse lands on the event loop — permanently.

The blast radius is process-wide because the event loops are shared. The driver never sets an
event-loop group on its `NettyNioAsyncHttpClient` builder, so every SDK client in the JVM draws
from the SDK's shared, reference-counted group (`SharedSdkEventLoopGroup`). One blocked loop
thread therefore starves requests from every connection and every pool, which is why fresh
connection attempts hang on the same wedged loop instead of escaping to a new one.

### Reproduction (mechanism level)

```bash
./gradlew run
```

This is the unit-level demonstration that the parser blocks the completing thread; it forces the
inline completion rather than reaching it. The production path is in the two sections after it.

`StreamingResponseStallRepro` builds a single-threaded Netty event-loop group, points an Athena
streaming client at a local HTTPS mock, and starts a
`GetQueryResultsStreamQueryResultsFactory.create(...)` call. The mock answers with a complete,
well-formed streamed response — metadata line plus one data row — and closes it immediately. No
artificial stall is needed: the parser blocks anyway, and the repro asserts it is *still* blocked
after the server has sent every byte and closed the body.

The client sets `FUTURE_COMPLETION_EXECUTOR` to `Runnable::run` to deterministically force the
same inline-on-the-event-loop completion that production reaches through the SDK's rejection
fallback described above.

A real Hikari pool, backed by an `AthenaAsyncClient` sharing the same wedged event loop, then
tries to borrow a connection. Real output from this repro:

```
Reproduced the production-facing exception:
java.sql.SQLTransientConnectionException: mcp-server-athena-write-pool-repro - Connection is not available, request timed out after 3004ms (total=0, active=0, idle=0, waiting=0)

Reproduced: GetQueryResultsStream parser blocks the sole Netty event loop.
The server sent the whole response and closed it normally, but the parser is
still blocked: only the wedged event loop could deliver those bytes.
Hikari remains at total=0 because its Athena connection request cannot complete.
Thread "aws-java-sdk-NettyEventLoop-repro-0" (state=WAITING):
    ...
    at java.base/java.io.BufferedReader.readLine(BufferedReader.java:329)
    at app//com.amazon.athena.client.results.parsing.GetQueryResultsStreamResponseParser.parse(GetQueryResultsStreamResponseParser.java:39)
    at app//com.amazon.athena.client.results.GetQueryResultsStreamQueryResultsFactory.parseResponse(GetQueryResultsStreamQueryResultsFactory.java:55)
    at app//com.amazon.athena.client.results.GetQueryResultsStreamQueryResultsFactory.lambda$create$1(GetQueryResultsStreamQueryResultsFactory.java:44)
```

This is the same failure shape production reported: `SQLTransientConnectionException` with
`total=0, active=0, idle=0, waiting=0`, and a Netty event-loop thread blocked in
`BufferedReader.readLine` inside the driver's own parser frame.

### Production-shaped reproduction: no hand-rolled `shutdown()` (`runTimeoutInterrupt`)

`StreamingResponseStallRepro` shows the parser blocks whichever thread completes the future. This
scenario shows how production gets that thread to be the event loop, on the real stack: HikariCP 7.1.0 pool of real driver connections, jOOQ 3.21.6 `fetch`, and
Reactor 3.8.7 `Mono.fromCallable(..).subscribeOn(boundedElastic()).timeout(T, fallback)` — the
exact shape of the Develocity mcp-server Athena health probe. The mock keeps the query RUNNING past
the timeout and flips it to SUCCEEDED on command.

    ./gradlew runTimeoutInterrupt                       # sweep + phases B, C, E, D, F in order
    ./gradlew runTimeoutInterrupt -Pphases=F -Ptrials=3 # the decisive phase, repeated
    ./gradlew runTimeoutInterrupt -Pphases=G -Ptrials=3 # phase F without the close: never wedges

What each phase established (all verified against driver 3.8.0, Hikari 7.1.0, Reactor 3.8.7 bytecode
and then observed at runtime):

1. **The timeout interrupts the worker, and nothing evicts the connection.** Reactor's
   `SchedulerTask.dispose()` calls `future.cancel(true)` from the timeout thread. The driver rethrows
   the `InterruptedException` as `new SQLException(message, cause)` — no SQLState. Hikari's
   `checkException` evicts only on SQLState `08*` or its fixed error lists, so the connection goes
   back to the pool alive, executor alive. The sweep (query completes 0.4 s or 5 s after the
   timeout) never wedges by itself.
2. **The driver leaves an ownerless async chain behind.** After the caller is gone, the statement's
   poll → fetch chain keeps running. When the poll sees SUCCEEDED it issues `GetQueryResultsStream`
   for a result nobody will read. There is no way to cancel it (`AsyncQueryResults` has no
   close/abort). Phase B shows a plain `Statement.close()` during execution does send
   `StopQueryExecution`, but the jOOQ/interrupt path does not reach it — and the mock answers it
   with AccessDenied, as a read-only role would, which the driver swallows silently.
3. **The real `close()` alone does not wedge (phases C, D, E).** `ConnectionConfiguration.close()`
   closes the five SDK clients *before* it shuts the executors. Each client owns its own Netty HTTP
   client, so closing it aborts the in-flight stream; the SDK fails the request fast on the loop
   instead of parsing it. Six isolated trials of C: zero wedges.
4. **Finding 2 supplies the missing step (phase F, deterministic).** Any borrow of that connection
   makes Hikari call `setNetworkTimeout` around `isValid`, and the driver's `setApiRequestTimeout`
   nulls its five client fields without closing them. The orphaned chain still holds the old
   streaming client; the configuration no longer does. When the connection is then physically
   closed (Hikari eviction here; `maxLifetime` rotation or any other physical close in production),
   `close()` skips the detached client and shuts the completion executor down. The detached
   client's stream response then completes, the hop is rejected, the SDK completes the future on
   the Netty event loop, and the blocking parse parks there:

       aws-java-sdk-NettyEventLoop-2-1 WAITING at GetQueryResultsStreamResponseParser.parse(GetQueryResultsStreamResponseParser.java:39)

   That is the exact frame from the production thread dumps. The drain check that follows shows the
   production dynamic: follow-up requests routed to that loop fail with
   `Acquire operation took longer than 15000 milliseconds` or never return, so pools drain over time.

5. **Detachment alone is not enough (phase G).** Phase F without the eviction recovers every time:
   the configuration creates its completion executor once, in its constructor, and every client
   generation shares it, so nulling the clients leaves the orphan's executor alive. A physical
   close of the connection is required. Finding 2 is what makes that close lethal; it does not
   replace it.

Production correlate (Develocity, apache instance, 2026-09-07): the probe timed out at 16:08:44.783 UTC;
136 ms later another borrow validated a connection (the detachment step); the query reached SUCCEEDED
at 16:08:45.202 and the pod never issued another Athena request. Which physical close ran on that
connection in production is not identified. The application has no eviction of its own, validation
never exceeded its 15 s timeout, and `maxLifetime` rotation (8 h across 30 connections, one close
per ~16 min) is far too rare to land inside sub-second windows three times in 25 timeouts. The
production thread dumps show the wedged loop frame-for-frame as phase F, with both pools'
connection-adder threads created 17-20 s after the onset — that is `validationTimeout` expiring on
the dead loop, a consequence of the wedge, not its cause.

Two conclusions for the driver, beyond Finding 1's parse-on-loop: the detach-without-close in
`setApiRequestTimeout` is what lets a request outlive its configuration's `close()`, and an abandoned
statement keeps issuing network requests with no way to stop it.

### Soak: the wedge from random Athena slowness alone (`runSoak`)

The scenarios above force each step. This one forces nothing. It runs the mcp-server health probe
(`AdaptiveHealthProbe` shape, probe queries verbatim) plus background tool queries against a mock
Athena that is randomly slow, on the pool configuration Develocity shipped **before 2026-08-31** —
the build the production incidents happened on:

- `maxLifetime` 30 min (scaled to 40 s here), `connectionTimeout` 3 s, `validationTimeout` 15 s,
- `connectionInitSql("select 1")` on top of the driver's ConnectionTest,
- no `NetworkTimeoutMillis` pin, so HikariCP's `setNetworkTimeout` detaches the driver's clients on
  every validation and every close,
- `ResultFetcher=GetQueryResultsStream`.

HikariCP logs at DEBUG, so every close carries its reason. `-XX:ActiveProcessorCount=1` gives the
production pod's two-loop SDK event-loop group.

    ./gradlew runSoak -Psoak.seconds=120 -Psoak.passIntervalSeconds=8 -Psoak.maxLifetimeSeconds=40

It wedged in 2 of 4 two-minute runs, and both logs show the same sequence to the millisecond:

    15:54:21.273 [boundedElastic-1]   Query soak-query-50 is executing          <- probe borrows, slow query
    15:54:31.331 [connection-closer]  Closing connection ...@4156624f: (connection was evicted)
    15:54:31.331 [boundedElastic-1]   Operator called default onErrorDropped     <- the probe timeout; same ms
    15:54:32.974 [NettyEventLoop-2-1] Query execution soak-query-50 has state SUCCEEDED
    15:54:33.494 *** WEDGE DETECTED *** NettyEventLoop-2-1 WAITING at GetQueryResultsStreamResponseParser.parse:39

Read in order: the probe holds a connection for its slow query; `maxLifetime` expires during that
borrow, and HikariCP marks the connection because it cannot close one in use; the probe times out,
the thread is interrupted and returns the connection, and HikariCP closes it on return —
`(connection was evicted)` — in the same millisecond as the timeout. That close is
`ConnectionConfiguration.close()`: it shuts the completion executor, but the abandoned query is
still polling on a client the driver had already detached, so `close()` never reached it. Two
seconds later that poll completes with no executor to hop to, so it runs on the Netty event loop
(`[NettyEventLoop-2-1] ... has state SUCCEEDED`), fetches the stream from there, and the blocking
parse parks the loop.

The close comes in two flavours, both `maxLifetime`. If the lifetime expired *during* the borrow,
HikariCP closes the connection on return, in the same millisecond as the timeout:
`(connection was evicted)`. If it expires a few seconds *after* the return, while the abandoned
chain is still polling, the housekeeper closes it idle: `(connection has passed maxLifetime)`. The
test has produced both. Either way the orphan's connection loses its executor while its request is
in flight. This is why production closes were within a second of a probe timeout (5 of 6 in the
Athena history): the close is the probe's connection reaching the end of its 30-minute life during,
or right after, the one borrow that lasted 30 seconds. A long borrow is exactly when a lifetime expiry gets
caught in use. The remaining open number is how often a 30-minute lifetime lands inside a 30-second
borrow at production traffic; HikariCP DEBUG on a production pod (`Closing connection ...:
(connection was evicted)` right after `probe exceeded PT30S`) settles it directly.

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

## Workaround and fix check

The workaround below addresses **Finding 1** from the caller's side. It is not a fix in the
driver; it is what a caller of the driver can do today, and it is what Develocity shipped.

### Workaround A: `ResultFetcher=GetQueryResults` (configuration only)

Decompiling the driver's `ConnectionConfiguration.getQueryResultsFactory(String)` shows the
connection property `ResultFetcher` (default `auto`) is matched case-insensitively against five
literal values, each building a different result-fetching class:

```
34: aload_1
35: ldc_w  #358   // String auto
38: ...     -> AutoQueryResultsFactory
73: ldc_w  #365   // String S3V2      -> S3StreamingQueryResultsFactory (v2 layout)
110: ldc_w #355   // String S3        -> S3StreamingQueryResultsFactory
146: ldc_w #369   // String GetQueryResults        -> GetQueryResultsQueryResultsFactory
177: ldc_w #372   // String GetQueryResultsStream   -> GetQueryResultsStreamQueryResultsFactory
208: ... else throw IllegalArgumentException("Invalid result fetcher: \"%s\"")
```

`GetQueryResults` (case-insensitive) selects `GetQueryResultsQueryResultsFactory`, which calls
the buffered, non-streaming `AmazonAthena.GetQueryResults` JSON API through the ordinary
`AthenaAsyncClient`, instead of the streaming `GetQueryResultsStream` API. That response is
unmarshalled by the AWS SDK's normal non-blocking, event-driven JSON parser. There is no
in-callback blocking read anywhere in that path, so there is nothing that can occupy a Netty
event-loop thread waiting for more bytes.

```bash
./gradlew runResultFetcherWorkaround
```

`ResultFetcherWorkaroundRepro` drives `GetQueryResultsQueryResultsFactory` directly (the same way
`StreamingResponseStallRepro` drives `GetQueryResultsStreamQueryResultsFactory` directly), against
a mock that mid-body stalls the buffered `GetQueryResults` response exactly like the streaming
scenario mid-body stalls the streamed one: it sends the JSON prefix (result set metadata and the
`Rows` array opener), flushes, then holds the connection open before sending the row and closing
braces. Real output:

```
GetQueryResults response is mid-body stalled (only the ResultSetMetadata prefix and the JSON array opener were sent; the row and closing braces are held back).
(a) A concurrent GetQueryExecution call completed on the SAME sole event loop in 28ms while GetQueryResults was still mid-body stalled. The event loop was never blocked.
(b) Hikari borrowed a real connection in 11ms while GetQueryResults was still mid-body stalled on the other client. The pool never reported total=0.

X-Amz-Target values the mock observed, in order:
  AmazonAthena.GetQueryResults
  AmazonAthena.GetQueryExecution
  AmazonAthena.StartQueryExecution

(c) The driver switched APIs: GetQueryResultsQueryResultsFactory called AmazonAthena.GetQueryResults, never GetQueryResultsStream.
```

All three of the task's proof points hold: the sole event loop stayed live for an unrelated
request while `GetQueryResults` was still mid-body stalled (a); a real Hikari-pooled connection
was still obtainable, not stuck at `total=0` (b); and the mock's request log shows the driver
actually called `AmazonAthena.GetQueryResults`, never `GetQueryResultsStream` (c). This is the
cheapest workaround: one connection property, no code changes, no dependency on driver
internals.

## Expected driver behavior

Three changes in the driver would each break the chain on their own; together they close it.

1. **Parse off the event loop.** `GetQueryResultsStreamQueryResultsFactory` attaches its blocking
   parse with `thenApply(...)`, so it runs on whichever thread completes the response future. It
   should use `thenApplyAsync(...)` on a live executor, with a safe fallback when that executor is
   gone: fail the future, never block the completing thread. `StreamingResponseStallRepro` is the
   unit-level target for this.
2. **Close what you detach.** `ConnectionConfiguration.setApiRequestTimeout` nulls the five SDK
   clients without closing them, so a request in flight on an old client outlives
   `ConnectionConfiguration.close()`, which only closes the clients it still references. Either
   close the old clients or keep referencing them until they are idle. `AthenaClientChurnRepro`
   measures this; phase G of `TimeoutInterruptRepro` shows the wedge needs it.
3. **Give an abandoned statement a way to stop.** After the caller's thread is interrupted, the
   statement's poll → fetch chain keeps running and issues `GetQueryResultsStream` for a result
   nobody will read. `AsyncQueryResults` has no close or abort, and `Statement.close()` only
   reaches `StopQueryExecution` while the statement is still marked executing. Cancel the chain
   when the statement is closed or its thread interrupted, and close the streamed HTTP response
   instead of leaving it open.

The production sequence that needs all three is in the soak section above: a caller-side timeout
interrupts the thread; the abandoned chain keeps polling on a client the driver has already
detached; HikariCP closes the connection on return because `maxLifetime` expired during the
borrow; the detached client's response then completes on the Netty event loop and the parse parks
it. `HealthCheckSoakTest` fails when that happens and prints the timeline and the HikariCP close lines that led to it.
`HealthCheckSoak` is the scenario in ~40 lines; the other classes in `soak/` are its wiring.
