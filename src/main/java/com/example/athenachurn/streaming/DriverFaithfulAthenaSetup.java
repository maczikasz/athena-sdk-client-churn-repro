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
import software.amazon.awssdk.services.athenastreaming.AthenaStreamingAsyncClient;

import java.net.URI;
import java.sql.SQLTransientConnectionException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Everything {@link MidFlightCloseRepro} needs that is NOT the repro itself: a miniature but
 * faithful copy of the production topology, plus the probes that verify the outcome.
 *
 * <p>The topology is deliberately built with no shortcuts:
 *
 * <ul>
 *   <li><b>One Netty event loop.</b> Production has more loop threads, but each connection is
 *       pinned to one loop, so a single thread models "the loop that owns the wedged channel"
 *       exactly. The read timeout is raised to 5 minutes so the mock's deliberate stall is not
 *       cut short; in production the timer cannot save the lethal case anyway, because it runs
 *       on the same event loop the parse blocks.</li>
 *   <li><b>The driver's completion executor, byte for byte.</b> The same {@code ThreadPoolExecutor}
 *       that {@code ConnectionConfiguration#createExecutor} builds in driver 3.8.0: core
 *       {@code max(8, cpus)}, max {@code max(64, 2*cpus)}, 10s keepalive, queue capacity 1000,
 *       threads named {@code athena-jdbc-*}, {@code allowCoreThreadTimeOut(true)}. The driver
 *       registers this pool as {@code FUTURE_COMPLETION_EXECUTOR} on every async client it
 *       creates, passes it to the result factories, and shuts it down in {@code close()}.</li>
 *   <li><b>Two SDK clients on the shared loop.</b> The {@link AthenaStreamingAsyncClient} the
 *       streaming fetcher uses, and a regular {@link AthenaAsyncClient} as the innocent
 *       bystander standing in for every other pool and request in the JVM.</li>
 *   <li><b>The driver's actual result factory.</b> {@link GetQueryResultsStreamQueryResultsFactory},
 *       whose {@code create()} does {@code getQueryResultsStream(request, toBlockingInputStream())
 *       .thenApply(parse)}. Plain {@code thenApply}: the blocking parse runs on whatever thread
 *       completes the future. That is the loaded gun both scenarios pull the trigger on.</li>
 * </ul>
 */
final class DriverFaithfulAthenaSetup implements AutoCloseable {

    private final MockAthenaServer mock;
    private final ThreadPoolExecutor completionExecutor;
    private final AthenaStreamingAsyncClient streamingClient;
    private final AthenaAsyncClient athenaClient;
    private final GetQueryResultsStreamQueryResultsFactory resultFactory;

    static DriverFaithfulAthenaSetup start() throws Exception {
        return new DriverFaithfulAthenaSetup(MockAthenaServer.startOnRandomPort());
    }

    private DriverFaithfulAthenaSetup(MockAthenaServer mock) {
        this.mock = mock;

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
        SdkAsyncHttpClient httpClient = NettyNioAsyncHttpClient.builder()
            .eventLoopGroup(eventLoopGroup)
            .readTimeout(Duration.ofMinutes(5))
            .build();

        int cpus = Runtime.getRuntime().availableProcessors();
        AtomicInteger completionThreadNumber = new AtomicInteger();
        this.completionExecutor = new ThreadPoolExecutor(
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

        var credentials = StaticCredentialsProvider.create(AwsBasicCredentials.create("dummy", "dummy-secret"));
        URI endpoint = URI.create(mock.baseUrl());
        ClientAsyncConfiguration driverCompletions = ClientAsyncConfiguration.builder()
            .advancedOption(SdkAdvancedAsyncClientOption.FUTURE_COMPLETION_EXECUTOR, completionExecutor)
            .build();
        this.streamingClient = AthenaStreamingAsyncClient.builder()
            .region(Region.EU_CENTRAL_1)
            .endpointOverride(endpoint)
            .credentialsProvider(credentials)
            .httpClient(httpClient)
            .asyncConfiguration(driverCompletions)
            .build();
        this.athenaClient = AthenaAsyncClient.builder()
            .region(Region.EU_CENTRAL_1)
            .endpointOverride(endpoint)
            .credentialsProvider(credentials)
            .httpClient(httpClient)
            .asyncConfiguration(driverCompletions)
            .build();
        this.resultFactory = new GetQueryResultsStreamQueryResultsFactory(
            streamingClient, completionExecutor, new ResultParserFactory()
        );
    }

    MockAthenaServer mock() {
        return mock;
    }

    ThreadPoolExecutor completionExecutor() {
        return completionExecutor;
    }

    AthenaAsyncClient athenaClient() {
        return athenaClient;
    }

    GetQueryResultsStreamQueryResultsFactory resultFactory() {
        return resultFactory;
    }

    /**
     * Waits until a Netty event-loop thread has the driver's blocking parser on its stack, or
     * the timeout passes. This frame on an event-loop thread IS the wedge.
     */
    boolean awaitParserOnNettyEventLoop(long timeoutMillis) throws InterruptedException {
        long deadline = System.nanoTime() + timeoutMillis * 1_000_000;
        while (System.nanoTime() < deadline) {
            if (!parserStackOnNettyEventLoop().isEmpty()) {
                return true;
            }
            Thread.sleep(25);
        }
        return false;
    }

    /** The full stack of any Netty event-loop thread currently inside the driver's parser. */
    String parserStackOnNettyEventLoop() {
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

    /**
     * Points a real Hikari pool at the wedged event loop and asserts it fails the way production
     * does: {@code SQLTransientConnectionException} with {@code total=0, active=0, idle=0,
     * waiting=0} and no cause, because the pool's connection-creation request can never complete
     * on the dead loop.
     */
    void assertHikariReportsTotalZero() throws Exception {
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
    }

    @Override
    public void close() {
        mock.close();
    }
}
