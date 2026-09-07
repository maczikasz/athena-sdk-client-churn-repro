package com.example.athenachurn;

import com.amazon.athena.client.results.GetQueryResultsStreamQueryResultsFactory;
import com.amazon.athena.client.results.ResultParserFactory;
import com.amazon.athena.jdbc.AthenaConnection;
import com.amazon.athena.jdbc.configuration.ConnectionConfiguration;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import software.amazon.awssdk.services.athena.AthenaAsyncClient;
import software.amazon.awssdk.services.athena.model.QueryExecution;
import software.amazon.awssdk.services.athenastreaming.AthenaStreamingAsyncClient;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Workaround scenario: a reflection-based "executor hop" applied entirely from this project's own
 * code, with the AWS driver jar untouched on disk.
 *
 * <p>The root cause is that {@code GetQueryResultsStreamQueryResultsFactory} attaches its blocking
 * parse continuation with a plain {@code CompletableFuture.thenApply(...)}, so it runs on whatever
 * thread completes the underlying {@code getQueryResultsStream} future — the Netty event loop. If
 * that future is made to complete asynchronously on a dedicated executor instead, the attached
 * {@code thenApply} runs on that executor too (plain, synchronous {@code thenApply} runs on the
 * completing thread when the future was not yet done at attachment time, which is always true
 * here). This program builds a dynamic {@link Proxy} in front of the driver's real
 * {@link AthenaStreamingAsyncClient} that does exactly that for every
 * {@code getQueryResultsStream} call, then reflectively swaps it into a live driver connection's
 * private {@code ConnectionConfiguration.athenaStreamingClient} field.
 *
 * <p><b>Caveat (see the README, Finding 2):</b> {@code ConnectionConfiguration#setApiRequestTimeout}
 * discards and lazily rebuilds this same field whenever the driver's reported network timeout
 * changes, which HikariCP triggers via {@code Connection#setNetworkTimeout} on pool validation.
 * Once that happens, this field is a fresh, un-patched client again. In real use this proxy must
 * either be re-applied after every validation cycle, or {@code NetworkTimeoutMillis} must be
 * pinned to the pool's own validation timeout (Finding 2's workaround) so the field is never
 * invalidated in the first place.
 */
public final class ReflectionExecutorHopRepro {

    private ReflectionExecutorHopRepro() {
    }

    public static void main(String[] args) throws Exception {
        try (MockAthenaServer mock = MockAthenaServer.startOnRandomPort()) {
            HikariConfig hikariConfig = new HikariConfig();
            hikariConfig.setDataSource(new ReflectionHopAthenaDataSource(mock.baseUrl()));
            hikariConfig.setPoolName("mcp-server-athena-write-pool-reflection-hop");
            hikariConfig.setMaximumPoolSize(1);
            hikariConfig.setConnectionTimeout(3_000);
            hikariConfig.setInitializationFailTimeout(-1);
            AtomicInteger hikariThreadNumber = new AtomicInteger();
            hikariConfig.setThreadFactory(task -> {
                Thread thread = new Thread(task, "hikari-reflection-hop-" + hikariThreadNumber.getAndIncrement());
                thread.setDaemon(true);
                return thread;
            });

            try (HikariDataSource pool = new HikariDataSource(hikariConfig)) {
                Connection pooled = pool.getConnection();
                AthenaConnection athenaConnection = pooled.unwrap(AthenaConnection.class);

                Field configurationField = AthenaConnection.class.getDeclaredField("configuration");
                configurationField.setAccessible(true);
                ConnectionConfiguration configuration = (ConnectionConfiguration) configurationField.get(athenaConnection);

                // Force lazy creation of the real streaming client, exactly as issuing a query would.
                AthenaStreamingAsyncClient realStreamingClient = configuration.getAthenaStreamingClient();
                System.out.println("Forced lazy creation of the real AthenaStreamingAsyncClient: "
                    + realStreamingClient.getClass());

                ExecutorService dedicatedExecutor = Executors.newSingleThreadExecutor(task -> {
                    Thread thread = new Thread(task, "athena-workaround-parse-executor-0");
                    thread.setDaemon(true);
                    return thread;
                });

                AthenaStreamingAsyncClient proxyClient = (AthenaStreamingAsyncClient) Proxy.newProxyInstance(
                    AthenaStreamingAsyncClient.class.getClassLoader(),
                    new Class<?>[] {AthenaStreamingAsyncClient.class},
                    executorHopHandler(realStreamingClient, dedicatedExecutor)
                );

                Field streamingClientField = ConnectionConfiguration.class.getDeclaredField("athenaStreamingClient");
                streamingClientField.setAccessible(true);
                streamingClientField.set(configuration, proxyClient);
                System.out.println("Replaced ConnectionConfiguration.athenaStreamingClient with an "
                    + "executor-hop proxy in front of the real client.");

                pooled.close();

                mock.armStreamingResponseHangOnce();
                var resultFactory = new GetQueryResultsStreamQueryResultsFactory(
                    (AthenaStreamingAsyncClient) configuration.getAthenaStreamingClient(),
                    Runnable::run,
                    new ResultParserFactory()
                );
                resultFactory.create(QueryExecution.builder().queryExecutionId("mock-query-1").build());

                if (!mock.awaitStreamingResponseHangStarted(15, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("The mock did not begin the partial streaming response");
                }

                String parserThreadName = awaitParserThreadName(5_000);
                if (parserThreadName == null) {
                    throw new IllegalStateException("The streaming parser thread was never observed");
                }
                System.out.println("Parser thread: \"" + parserThreadName + "\"");
                if (parserThreadName.startsWith("aws-java-sdk-NettyEventLoop")) {
                    throw new IllegalStateException(
                        "FAILED: the parser ran on the Netty event loop even with the proxy installed"
                    );
                }
                if (!parserThreadName.startsWith("athena-workaround-parse-executor")) {
                    throw new IllegalStateException(
                        "FAILED: the parser ran on an unexpected thread: " + parserThreadName
                    );
                }
                System.out.println("(1) The blocking parse now runs on the dedicated executor thread, "
                    + "not on any aws-java-sdk-NettyEventLoop thread.");

                // (2) This connection's own AthenaAsyncClient (used for StartQueryExecution / GetQueryExecution)
                // is unaffected: it keeps completing requests while the streaming fetch is stalled.
                AthenaAsyncClient athenaSdkClient = configuration.getAthenaSdkClient();
                CompletableFuture<?> concurrentCall = athenaSdkClient.getQueryExecution(
                    b -> b.queryExecutionId("mock-query-1")
                );
                Object response = concurrentCall.get(5, TimeUnit.SECONDS);
                System.out.println("(2) A concurrent GetQueryExecution call on this connection's own "
                    + "AthenaAsyncClient completed (" + response.getClass().getSimpleName()
                    + ") while GetQueryResultsStream was still mid-body stalled.");

                // (3) Hikari can still borrow this same connection: the pool does not report total=0.
                long borrowStart = System.nanoTime();
                Connection reborrowed = pool.getConnection();
                long borrowElapsedMillis = (System.nanoTime() - borrowStart) / 1_000_000;
                reborrowed.close();
                System.out.println("(3) Hikari re-borrowed this connection in " + borrowElapsedMillis
                    + "ms while the streaming fetch was still stalled. The pool never reported total=0.");

                mock.releaseStreamingResponse();
                dedicatedExecutor.shutdown();
            }
        }
    }

    private static InvocationHandler executorHopHandler(
        AthenaStreamingAsyncClient realClient, ExecutorService dedicatedExecutor
    ) {
        return (proxy, method, methodArgs) -> {
            if ("getQueryResultsStream".equals(method.getName())
                && CompletableFuture.class.equals(method.getReturnType())) {
                CompletableFuture<?> real;
                try {
                    real = (CompletableFuture<?>) method.invoke(realClient, methodArgs);
                } catch (InvocationTargetException e) {
                    throw e.getCause();
                }
                CompletableFuture<Object> bridge = new CompletableFuture<>();
                real.whenCompleteAsync((result, throwable) -> {
                    if (throwable != null) {
                        bridge.completeExceptionally(throwable);
                    } else {
                        bridge.complete(result);
                    }
                }, dedicatedExecutor);
                return bridge;
            }
            try {
                return method.invoke(realClient, methodArgs);
            } catch (InvocationTargetException e) {
                throw e.getCause();
            }
        };
    }

    private static String awaitParserThreadName(long timeoutMillis) throws InterruptedException {
        long deadline = System.nanoTime() + timeoutMillis * 1_000_000;
        while (System.nanoTime() < deadline) {
            for (Map.Entry<Thread, StackTraceElement[]> entry : Thread.getAllStackTraces().entrySet()) {
                if (Arrays.toString(entry.getValue()).contains("GetQueryResultsStreamResponseParser")) {
                    return entry.getKey().getName();
                }
            }
            Thread.sleep(25);
        }
        return null;
    }
}
