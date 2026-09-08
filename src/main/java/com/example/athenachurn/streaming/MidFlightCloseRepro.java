package com.example.athenachurn.streaming;

import com.amazon.athena.client.results.GetQueryResultsStreamQueryResultsFactory;
import com.amazon.athena.client.results.ResultParserFactory;
import com.example.athenachurn.datasource.BlockingAthenaDataSource;
import com.example.athenachurn.mock.MockAthenaServer;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientAsyncConfiguration;
import software.amazon.awssdk.core.client.config.SdkAdvancedAsyncClientOption;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.http.nio.netty.SdkEventLoopGroup;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.athena.AthenaAsyncClient;
import software.amazon.awssdk.services.athena.model.GetQueryExecutionRequest;
import software.amazon.awssdk.services.athena.model.QueryExecution;
import software.amazon.awssdk.services.athenastreaming.AthenaStreamingAsyncClient;

import java.net.URI;
import java.sql.SQLTransientConnectionException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Reproduces the production onset sequence of the event-loop wedge in two phases, both against
 * the same slow response. The two phases together show that a slow response alone is harmless:
 * the wedge needs a second ingredient.
 *
 * <p>Unlike {@link StreamingResponseStallRepro}, which injects an inline completion executor to
 * demonstrate the deadlock directly, this scenario uses the exact completion executor the driver
 * builds ({@code ConnectionConfiguration#createExecutor} in 3.8.0) and reaches the inline
 * completion the way production does: through the AWS SDK's rejected-execution fallback.
 *
 * <p><b>Phase A — a slow response alone recovers.</b> The mock holds the streaming response
 * before the first header byte, standing in for Athena taking tens of seconds to return a
 * result. The caller blocks in a timeout-less {@code get()} the whole time. When the bytes
 * arrive, the SDK hops the future completion onto the driver's {@code athena-jdbc} executor,
 * the blocking parse runs there, the Netty event loop stays free to deliver the body, and the
 * call completes. Nothing is wedged.
 *
 * <p><b>Phase B — the same slow response plus a mid-flight close wedges forever.</b> While the
 * response is still held, the completion executor is shut down — exactly what
 * {@code ConnectionConfiguration#close()} does when the connection that issued the request is
 * closed while the request is outstanding. When the response then arrives, the SDK cannot hop
 * to the executor ({@code RejectedExecutionException}), logs its fallback message at debug level
 * ("Could not complete the service call future on the provided FUTURE_COMPLETION_EXECUTOR. The
 * future will be completed synchronously by thread ..."), and completes the future synchronously
 * on the Netty event loop. The driver's plain
 * {@code thenApply(parse)} then runs its blocking {@code readLine} on that loop. The body bytes
 * the read waits for can only be delivered by the same loop, so the loop is blocked forever —
 * and because the SDK's timeout timers run on that loop too, no timeout can ever fire to break
 * it. Every client sharing the loop is dead with it: Hikari reports
 * {@code total=0, active=0, idle=0, waiting=0} with no cause, until the JVM restarts.
 */
public final class MidFlightCloseRepro {

    private MidFlightCloseRepro() {
    }

    public static void main(String[] args) throws Exception {
        // The SDK logs its rejected-hop fallback message at debug level only. Surface it so the
        // run output carries the SDK's own statement of what happened.
        System.setProperty(
            "org.slf4j.simpleLogger.log.software.amazon.awssdk.core.internal.http.pipeline.stages.MakeAsyncHttpRequestStage",
            "debug"
        );
        AtomicInteger eventLoopNumber = new AtomicInteger();
        SdkEventLoopGroup eventLoopGroup = SdkEventLoopGroup.builder()
            .numberOfThreads(1)
            .threadFactory(task -> {
                Thread thread = new Thread(
                    task, "aws-java-sdk-NettyEventLoop-repro-" + eventLoopNumber.getAndIncrement()
                );
                thread.setDaemon(true);
                return thread;
            })
            .build();
        // The mock holds the response idle while it stalls, so the SDK's 30s read timeout must be
        // out of the way. In production the timer cannot save the lethal phase anyway: it runs on
        // the same event loop the parse blocks.
        SdkAsyncHttpClient httpClient = NettyNioAsyncHttpClient.builder()
            .eventLoopGroup(eventLoopGroup)
            .readTimeout(Duration.ofMinutes(5))
            .build();

        // The driver's completion executor, byte for byte: ConnectionConfiguration#createExecutor
        // in 3.8.0 builds this pool, registers it as FUTURE_COMPLETION_EXECUTOR on every async
        // client it creates, passes it to the result factories, and shuts it down in close().
        int cpus = Runtime.getRuntime().availableProcessors();
        AtomicInteger completionThreadNumber = new AtomicInteger();
        ThreadPoolExecutor completionExecutor = new ThreadPoolExecutor(
            Math.max(8, cpus), Math.max(64, cpus * 2),
            10, TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(1000),
            task -> {
                Thread thread = new Thread(task, "athena-jdbc-" + completionThreadNumber.getAndIncrement());
                thread.setDaemon(true);
                return thread;
            }
        );
        completionExecutor.allowCoreThreadTimeOut(true);

        try (MockAthenaServer mock = MockAthenaServer.startOnRandomPort()) {
            var credentials = StaticCredentialsProvider.create(AwsBasicCredentials.create("dummy", "dummy-secret"));
            URI endpoint = URI.create(mock.baseUrl());
            ClientAsyncConfiguration driverCompletions = ClientAsyncConfiguration.builder()
                .advancedOption(SdkAdvancedAsyncClientOption.FUTURE_COMPLETION_EXECUTOR, completionExecutor)
                .build();
            AthenaStreamingAsyncClient streamingClient = AthenaStreamingAsyncClient.builder()
                .region(Region.EU_CENTRAL_1)
                .endpointOverride(endpoint)
                .credentialsProvider(credentials)
                .httpClient(httpClient)
                .asyncConfiguration(driverCompletions)
                .build();
            AthenaAsyncClient athenaClient = AthenaAsyncClient.builder()
                .region(Region.EU_CENTRAL_1)
                .endpointOverride(endpoint)
                .credentialsProvider(credentials)
                .httpClient(httpClient)
                .asyncConfiguration(driverCompletions)
                .build();
            var resultFactory = new GetQueryResultsStreamQueryResultsFactory(
                streamingClient, completionExecutor, new ResultParserFactory()
            );

            phaseA(mock, resultFactory, athenaClient);
            phaseB(mock, resultFactory, athenaClient, completionExecutor);
        }
    }

    private static void phaseA(
        MockAthenaServer mock,
        GetQueryResultsStreamQueryResultsFactory resultFactory,
        AthenaAsyncClient athenaClient
    ) throws Exception {
        System.out.println("=== Phase A: slow response, executor alive ===");
        mock.armStreamingHeaderHoldOnce();

        CompletableFuture<String> completedOn = new CompletableFuture<>();
        Thread borrower = new Thread(() -> {
            try {
                // The driver's statement blocks on this stage with a timeout-less get; the caller
                // thread mirrors it.
                resultFactory.create(QueryExecution.builder().queryExecutionId("mock-query-1").build())
                    .thenApply(results -> Thread.currentThread().getName())
                    .toCompletableFuture()
                    .whenComplete((thread, failure) -> {
                        if (failure != null) {
                            completedOn.completeExceptionally(failure);
                        } else {
                            completedOn.complete(thread);
                        }
                    })
                    .get();
            } catch (Exception e) {
                completedOn.completeExceptionally(e);
            }
        }, "borrower-benign");
        borrower.setDaemon(true);
        borrower.start();

        if (!mock.awaitStreamingRequestArrived(5, TimeUnit.SECONDS)) {
            throw new IllegalStateException("The streaming request never reached the mock server");
        }
        // Stand-in for the tens of seconds Athena can take to return the first byte. The caller's
        // own timeout (if it has one) fires during this window and reports the call slow — an
        // observation, not a wedge.
        Thread.sleep(2_000);
        mock.releaseStreamingHeaders();

        String completionThread = completedOn.get(10, TimeUnit.SECONDS);
        System.out.println("Slow response completed. The blocking parse ran on: " + completionThread);
        if (!completionThread.startsWith("athena-jdbc-")) {
            throw new IllegalStateException(
                "Expected the completion hop onto the driver's executor, got: " + completionThread
            );
        }
        athenaClient.getQueryExecution(
            GetQueryExecutionRequest.builder().queryExecutionId("mock-query-1").build()
        ).get(5, TimeUnit.SECONDS);
        System.out.println("A second request on the same event loop completes normally. RECOVERED.");
        System.out.println();
    }

    private static void phaseB(
        MockAthenaServer mock,
        GetQueryResultsStreamQueryResultsFactory resultFactory,
        AthenaAsyncClient athenaClient,
        ThreadPoolExecutor completionExecutor
    ) throws Exception {
        System.out.println("=== Phase B: the same slow response, executor shut down mid-flight ===");
        mock.armStreamingHeaderHoldOnce();

        Thread borrower = new Thread(() -> {
            try {
                resultFactory.create(QueryExecution.builder().queryExecutionId("mock-query-1").build())
                    .toCompletableFuture()
                    .get();
            } catch (Exception ignored) {
                // The lethal phase never completes this future.
            }
        }, "borrower-lethal");
        borrower.setDaemon(true);
        borrower.start();

        if (!mock.awaitStreamingRequestArrived(5, TimeUnit.SECONDS)) {
            throw new IllegalStateException("The streaming request never reached the mock server");
        }
        // The spark: ConnectionConfiguration#close() shuts this executor down. In production this
        // happens when the pool closes the connection whose streamed request is still in flight.
        completionExecutor.shutdown();
        Thread.sleep(200);
        mock.releaseStreamingHeaders();

        if (!awaitParserOnNettyEventLoop(5_000)) {
            throw new IllegalStateException(
                "The blocking parse did not land on the Netty event loop - rejection fallback not reproduced"
            );
        }
        String parserStack = parserStackOnNettyEventLoop();
        if (!parserStack.contains("lambda$executeHttpRequest$6")) {
            throw new IllegalStateException(
                "The event-loop parse did not go through the SDK's rejected-hop fallback "
                    + "(MakeAsyncHttpRequestStage.lambda$executeHttpRequest$6):\n" + parserStack
            );
        }
        System.out.println("The debug line above (\"Could not complete the service call future...\") is the");
        System.out.println("SDK's rejected-hop fallback, and the lambda$executeHttpRequest$6 frame below is its");
        System.out.println("synchronous completion path. The parse now blocks the sole Netty event loop:");
        System.out.print(parserStack);

        try {
            athenaClient.getQueryExecution(
                GetQueryExecutionRequest.builder().queryExecutionId("mock-query-1").build()
            ).get(3, TimeUnit.SECONDS);
            throw new IllegalStateException("A request on the wedged event loop unexpectedly completed");
        } catch (TimeoutException expected) {
            System.out.println("A second request on the same event loop never completes. WEDGED.");
        }

        HikariConfig hikariConfig = new HikariConfig();
        BlockingAthenaDataSource blockingDataSource = new BlockingAthenaDataSource(athenaClient);
        hikariConfig.setDataSource(blockingDataSource);
        hikariConfig.setPoolName("mcp-server-athena-write-pool-repro");
        hikariConfig.setMaximumPoolSize(1);
        hikariConfig.setConnectionTimeout(3_000);
        hikariConfig.setInitializationFailTimeout(-1);
        AtomicInteger hikariThreadNumber = new AtomicInteger();
        hikariConfig.setThreadFactory(task -> {
            Thread thread = new Thread(task, "hikari-repro-" + hikariThreadNumber.getAndIncrement());
            thread.setDaemon(true);
            return thread;
        });
        try (HikariDataSource pool = new HikariDataSource(hikariConfig)) {
            if (!blockingDataSource.awaitConnectionAttempt(5, TimeUnit.SECONDS)) {
                throw new IllegalStateException("Hikari did not start an Athena connection attempt");
            }
            try {
                pool.getConnection();
                throw new IllegalStateException("Hikari unexpectedly created an Athena connection");
            } catch (SQLTransientConnectionException expected) {
                if (!expected.getMessage().contains("Connection is not available, request timed out after 3")
                    || !expected.getMessage().contains("(total=0, active=0, idle=0, waiting=0)")) {
                    throw expected;
                }
                System.out.println("Reproduced the production-facing exception:");
                expected.printStackTrace(System.out);
            }
        }

        if (borrower.isAlive()) {
            System.out.println("The original caller is still parked in a timeout-less get() - the same frame");
            System.out.println("the pool's connection-adder thread shows in a wedged production JVM.");
        }
        System.out.println();
        System.out.println("Phase A and Phase B differ by one call: completionExecutor.shutdown() while the");
        System.out.println("response was in flight. A slow response is the precondition; the mid-flight close");
        System.out.println("is what turns it into a permanent wedge.");
    }

    private static boolean awaitParserOnNettyEventLoop(long timeoutMillis) throws InterruptedException {
        long deadline = System.nanoTime() + timeoutMillis * 1_000_000;
        while (System.nanoTime() < deadline) {
            for (Map.Entry<Thread, StackTraceElement[]> entry : Thread.getAllStackTraces().entrySet()) {
                if (entry.getKey().getName().startsWith("aws-java-sdk-NettyEventLoop")
                    && Arrays.toString(entry.getValue()).contains("GetQueryResultsStreamResponseParser")) {
                    return true;
                }
            }
            Thread.sleep(25);
        }
        return false;
    }

    private static String parserStackOnNettyEventLoop() {
        StringBuilder output = new StringBuilder();
        for (Map.Entry<Thread, StackTraceElement[]> entry : Thread.getAllStackTraces().entrySet()) {
            if (entry.getKey().getName().startsWith("aws-java-sdk-NettyEventLoop")
                && Arrays.toString(entry.getValue()).contains("GetQueryResultsStreamResponseParser")) {
                output.append("Thread \"").append(entry.getKey().getName()).append("\" (state=")
                    .append(entry.getKey().getState()).append("):\n");
                for (StackTraceElement frame : entry.getValue()) {
                    output.append("    at ").append(frame).append('\n');
                }
            }
        }
        return output.toString();
    }
}
