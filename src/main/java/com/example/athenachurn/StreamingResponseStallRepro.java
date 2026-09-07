package com.example.athenachurn;

import com.amazon.athena.client.results.GetQueryResultsStreamQueryResultsFactory;
import com.amazon.athena.client.results.ResultParserFactory;
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
import software.amazon.awssdk.services.athena.model.QueryExecution;
import software.amazon.awssdk.services.athenastreaming.AthenaStreamingAsyncClient;

import java.net.URI;
import java.sql.SQLTransientConnectionException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Reproduces the driver's GetQueryResultsStream parser blocking its response-completion thread,
 * which prevents a second Athena request from completing on the same Netty event loop.
 */
public final class StreamingResponseStallRepro {

    private StreamingResponseStallRepro() {
    }

    public static void main(String[] args) throws Exception {
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
        try (MockAthenaServer mock = MockAthenaServer.startOnRandomPort()) {
            var credentials = StaticCredentialsProvider.create(AwsBasicCredentials.create("dummy", "dummy-secret"));
            URI endpoint = URI.create(mock.baseUrl());
            ClientAsyncConfiguration inlineCompletions = ClientAsyncConfiguration.builder()
                .advancedOption(SdkAdvancedAsyncClientOption.FUTURE_COMPLETION_EXECUTOR, Runnable::run)
                .build();
            AthenaStreamingAsyncClient streamingClient = AthenaStreamingAsyncClient.builder()
                .region(Region.EU_CENTRAL_1)
                .endpointOverride(endpoint)
                .credentialsProvider(credentials)
                .httpClient(httpClient)
                .asyncConfiguration(inlineCompletions)
                .build();
            AthenaAsyncClient athenaClient = AthenaAsyncClient.builder()
                .region(Region.EU_CENTRAL_1)
                .endpointOverride(endpoint)
                .credentialsProvider(credentials)
                .httpClient(httpClient)
                .asyncConfiguration(inlineCompletions)
                .build();
            mock.armStreamingResponseHangOnce();
            var resultFactory = new GetQueryResultsStreamQueryResultsFactory(
                streamingClient, Runnable::run, new ResultParserFactory()
            );
            resultFactory.create(QueryExecution.builder().queryExecutionId("mock-query-1").build());

            if (!mock.awaitStreamingResponseHangStarted(15, TimeUnit.SECONDS)) {
                throw new IllegalStateException("The mock did not begin the partial streaming response");
            }
            if (!awaitParserOnNettyEventLoop(5_000)) {
                throw new IllegalStateException("The streaming parser was not found on the Netty event loop");
            }

            BlockingAthenaDataSource blockingDataSource = new BlockingAthenaDataSource(athenaClient);
            HikariConfig hikariConfig = new HikariConfig();
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

            HikariDataSource pool = new HikariDataSource(hikariConfig);
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

            System.out.println("Reproduced: GetQueryResultsStream parser blocks the sole Netty event loop.");
            System.out.println("Hikari remains at total=0 because its Athena connection request cannot complete.");
            printParserStack();
            mock.releaseStreamingResponse();
        }
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

    private static void printParserStack() {
        for (Map.Entry<Thread, StackTraceElement[]> entry : Thread.getAllStackTraces().entrySet()) {
            if (entry.getKey().getName().startsWith("aws-java-sdk-NettyEventLoop")
                && Arrays.toString(entry.getValue()).contains("GetQueryResultsStreamResponseParser")) {
                System.out.println("Thread \"" + entry.getKey().getName() + "\" (state="
                    + entry.getKey().getState() + "):");
                for (StackTraceElement frame : entry.getValue()) {
                    System.out.println("    at " + frame);
                }
            }
        }
    }
}
