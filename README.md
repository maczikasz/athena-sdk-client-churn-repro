# Athena JDBC driver 3.8.0: two offline reproductions and two workarounds

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
├── streaming/   Finding 1: StreamingResponseStallRepro, MidFlightCloseRepro (+ its DriverFaithfulAthenaSetup), both workaround repros
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
connection, but the JDBC-level lifecycle routinely ends before the HTTP stream does — a
response completes after the owning connection closed. Candidate sources include the paginator
prefetching a page the consumer never reads, an error path abandoning a statement mid-stream, and
a connection retired by max-lifetime immediately on return to the pool. (Plain early truncation
by the consumer is not enough on its own where the query layer materializes the full result set
before closing, as jOOQ's fetch() does.) When the late response bytes then arrive, the completion is rejected by
the shut-down pool, the SDK falls back to synchronous completion, and the driver's blocking parse
lands on the event loop — permanently.

The blast radius is process-wide because the event loops are shared. The driver never sets an
event-loop group on its `NettyNioAsyncHttpClient` builder, so every SDK client in the JVM draws
from the SDK's shared, reference-counted group (`SharedSdkEventLoopGroup`). One blocked loop
thread therefore starves requests from every connection and every pool, which is why fresh
connection attempts hang on the same wedged loop instead of escaping to a new one.

### Reproduction

```bash
./gradlew run
```

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

### End-to-end reproduction: the same wedge through the SDK's own rejection fallback

```bash
./gradlew runMidFlightClose
```

`MidFlightCloseRepro` removes the one shortcut in the scenario above. It does not inject
`Runnable::run`; it builds the exact completion executor the driver builds
(`ConnectionConfiguration#createExecutor`), registers it as `FUTURE_COMPLETION_EXECUTOR`, and
reaches the inline completion the way production does. It runs the same slow response twice:

- **Benign run (`slowResponseWithExecutorAliveRecovers`) — slow response, executor alive.** The mock holds the streaming response before the
  first header byte (a query that takes a long time to return its first result byte). The caller
  blocks in a timeout-less `get()` throughout. When the bytes arrive, the completion hops onto an
  `athena-jdbc-*` thread, the blocking parse runs there, and the call completes. A second request
  on the same event loop succeeds. A slow response alone recovers.
- **Lethal run (`slowResponseWithMidFlightCloseWedgesForever`) — the same slow response, executor shut down mid-flight.** While the response is
  still held, the repro calls `shutdown()` on the completion executor — what
  `ConnectionConfiguration.close()` does when the connection that issued the request is closed
  with the request outstanding. When the response then arrives, the SDK logs its fallback line
  and completes the future on the event loop, where the parse blocks forever. The repro asserts
  the fallback frame (`MakeAsyncHttpRequestStage.lambda$executeHttpRequest$6`) is on the wedged
  stack, shows a second request never completing, and reproduces the same
  `total=0, active=0, idle=0, waiting=0` Hikari exception.

Real output from the lethal run:

```
[aws-java-sdk-NettyEventLoop-repro-0] DEBUG software.amazon.awssdk...MakeAsyncHttpRequestStage - Could not complete the service call future on the provided FUTURE_COMPLETION_EXECUTOR. The future will be completed synchronously by thread aws-java-sdk-NettyEventLoop-repro-0. ...
Thread "aws-java-sdk-NettyEventLoop-repro-0" (state=WAITING):
    ...
    at java.base/java.io.BufferedReader.readLine(BufferedReader.java:436)
    at app//com.amazon.athena.client.results.parsing.GetQueryResultsStreamResponseParser.parse(GetQueryResultsStreamResponseParser.java:39)
    ...
    at app//software.amazon.awssdk.core.internal.http.pipeline.stages.MakeAsyncHttpRequestStage.lambda$executeHttpRequest$6(MakeAsyncHttpRequestStage.java:187)
    ...
    at app//io.netty.channel.nio.NioEventLoop.run(NioEventLoop.java:562)
A second request on the same event loop never completes. WEDGED.
```

The two runs differ by a single call: `completionExecutor.shutdown()` while the response was in
flight. A slow response is the precondition; the mid-flight close is what turns it into a
permanent wedge.

### Production-shaped reproduction: no hand-rolled `shutdown()` (`runTimeoutInterrupt`)

`MidFlightCloseRepro` proves the driver-side fallback is lethal, but it reaches the lethal state by
calling `shutdown()` on the completion executor itself. This scenario reaches it the way production
does, on the real stack: HikariCP 7.1.0 pool of real driver connections, jOOQ 3.21.6 `fetch`, and
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

## Workarounds

Both workarounds below address **Finding 1**. Neither one is a fix in the driver; both are
things a caller of the driver can do today.

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

### Workaround B: reflection executor hop (code only, fragile)

Where Workaround A is not usable — for example, a caller specifically wants the streaming
fetcher's lower per-row overhead — the same "the attached `thenApply` runs on whichever thread
completes the future" mechanism that causes the bug can be used to fix it, entirely from calling
code, with the driver jar untouched on disk. The fix is exactly the one-word change the driver
itself needs (`thenApplyAsync` instead of `thenApply`), applied from the outside by intercepting
the future the driver attaches its `thenApply` to.

`ReflectionExecutorHopRepro`:

1. Builds a real `AthenaConnection` through the driver's own public
   `ConnectionConfiguration.from(...)` + `new AthenaConnection(configuration)` path (the same path
   `AthenaDriver.connect(...)` uses internally), pooled behind HikariCP.
2. Unwraps the pooled connection to `AthenaConnection` and reflects into its private
   `configuration` field.
3. Calls `configuration.getAthenaStreamingClient()` once, to force the real
   `AthenaStreamingAsyncClient` to be lazily built and cached.
4. Wraps that real client in a `java.lang.reflect.Proxy` that delegates every method to it
   unchanged, except `getQueryResultsStream`: for that method, instead of returning the SDK's own
   future, it returns a bridge future that is completed via
   `real.whenCompleteAsync((result, throwable) -> ..., dedicatedExecutor)`. Because the driver's
   `thenApply` is attached to the bridge future before the bridge future is completed (attachment
   happens synchronously right after the call returns, long before any network response arrives),
   that plain `thenApply` runs on whichever thread completes its future — and now that thread is
   `dedicatedExecutor`'s worker thread, not the Netty event loop.
5. Reflectively replaces the private `ConnectionConfiguration.athenaStreamingClient` field with
   this proxy.

```bash
./gradlew runReflectionExecutorHop
```

Real output:

```
Forced lazy creation of the real AthenaStreamingAsyncClient: class software.amazon.awssdk.services.athenastreaming.DefaultAthenaStreamingAsyncClient
Replaced ConnectionConfiguration.athenaStreamingClient with an executor-hop proxy in front of the real client.
Parser thread: "athena-workaround-parse-executor-0"
(1) The blocking parse now runs on the dedicated executor thread, not on any aws-java-sdk-NettyEventLoop thread.
(2) A concurrent GetQueryExecution call on this connection's own AthenaAsyncClient completed (GetQueryExecutionResponse) while GetQueryResultsStream was still mid-body stalled.
(3) Hikari re-borrowed this connection in 0ms while the streaming fetch was still stalled. The pool never reported total=0.
```

The parse thread is now the dedicated executor's worker thread, confirmed by name, and never the
Netty event loop. This connection's own `AthenaAsyncClient` (the one used for
`StartQueryExecution` / `GetQueryExecution`) kept completing calls, and HikariCP could still
re-borrow the same connection, while the streaming fetch stayed mid-body stalled the whole time.

**Caveat.** This workaround shares its dependency with Finding 2: `ConnectionConfiguration
#setApiRequestTimeout` discards and lazily rebuilds `athenaStreamingClient` (among the other four
client fields) whenever the driver's reported network timeout changes, and HikariCP triggers that
on every validating borrow via `setNetworkTimeout`. Once that happens, `athenaStreamingClient` is
a fresh, un-patched client again, and the proxy is gone. In real use this workaround needs one of:

- re-apply the proxy after every validation cycle (fragile, timing-sensitive), or
- combine it with Finding 2's own workaround — pin `NetworkTimeoutMillis` to the pool's
  validation timeout — so the field is never invalidated in the first place.

It also depends on the driver's private field layout (`AthenaConnection.configuration`,
`ConnectionConfiguration.athenaStreamingClient`) and will break silently — falling back to the
unpatched, event-loop-blocking behavior with no error — on any driver upgrade that renames or
restructures those fields. Workaround A has none of these problems and should be preferred
wherever it is usable.

## Expected driver behavior

The proper fix belongs in the driver, not in caller-side workarounds. It is a one-word change,
demonstrated by Workaround B: `GetQueryResultsStreamQueryResultsFactory` should attach its parse
continuation with `thenApplyAsync(...)` (on a dedicated executor) instead of `thenApply(...)`, so
a blocking parse of a streamed response can never run on a Netty event-loop thread. Independently
of that, any timeout or cancellation path should close the streamed HTTP response rather than
leaving it, and the connection along with it, open indefinitely.
