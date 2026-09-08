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
├── streaming/   Finding 1: StreamingResponseStallRepro plus both workaround repros
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
