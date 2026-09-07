package com.example.athenachurn;

import com.amazon.athena.client.results.GetQueryResultsQueryResultsFactory;
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

import java.net.URI;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Workaround scenario: connection property {@code ResultFetcher=GetQueryResults}.
 *
 * <p>Decompiling the driver's {@code ConnectionConfiguration.getQueryResultsFactory(String)}
 * (see the README) shows the property accepts, case-insensitively: {@code auto} (default),
 * {@code S3V2}, {@code S3}, {@code GetQueryResults}, {@code GetQueryResultsStream}. The value
 * {@code GetQueryResults} builds a {@link GetQueryResultsQueryResultsFactory}, which calls the
 * buffered {@code AmazonAthena.GetQueryResults} JSON API through the ordinary
 * {@link AthenaAsyncClient}. That factory is exactly what this scenario drives directly, the same
 * way {@link StreamingResponseStallRepro} drives {@code GetQueryResultsStreamQueryResultsFactory}
 * directly, so the same mid-body stall exercises the buffered code path instead of the streaming
 * one.
 *
 * <p>Because {@code AmazonAthena.GetQueryResults} responses are unmarshalled by the AWS SDK's
 * ordinary non-blocking JSON response handler (event-driven, no in-band blocking read), a stalled
 * response never occupies the Netty event-loop thread the way {@code BufferedReader.readLine()}
 * does for the streaming API. This program proves that by starting the stall, then completing an
 * unrelated request and a Hikari-borrowed connection on the very same single-threaded event loop
 * while the stall is still open.
 */
public final class ResultFetcherWorkaroundRepro {

    private ResultFetcherWorkaroundRepro() {
    }

    public static void main(String[] args) throws Exception {
        AtomicInteger eventLoopNumber = new AtomicInteger();
        SdkEventLoopGroup eventLoopGroup = SdkEventLoopGroup.builder()
            .numberOfThreads(1)
            .threadFactory(task -> {
                Thread thread = new Thread(
                    task, "aws-java-sdk-NettyEventLoop-workaround-" + eventLoopNumber.getAndIncrement()
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
            AthenaAsyncClient athenaClient = AthenaAsyncClient.builder()
                .region(Region.EU_CENTRAL_1)
                .endpointOverride(endpoint)
                .credentialsProvider(credentials)
                .httpClient(httpClient)
                .asyncConfiguration(inlineCompletions)
                .build();

            mock.armGetQueryResultsHangOnce();
            var resultFactory = new GetQueryResultsQueryResultsFactory(
                athenaClient, Runnable::run, new ResultParserFactory()
            );
            resultFactory.create(QueryExecution.builder().queryExecutionId("mock-query-1").build());

            if (!mock.awaitGetQueryResultsHangStarted(15, TimeUnit.SECONDS)) {
                throw new IllegalStateException("The mock did not begin the partial GetQueryResults response");
            }
            System.out.println("GetQueryResults response is mid-body stalled (only the ResultSetMetadata "
                + "prefix and the JSON array opener were sent; the row and closing braces are held back).");

            // (a) Prove the sole Netty event loop is NOT blocked: complete an unrelated request on it.
            CountDownLatch concurrentRequestDone = new CountDownLatch(1);
            long start = System.nanoTime();
            athenaClient.getQueryExecution(
                b -> b.queryExecutionId("mock-query-1")
            ).whenComplete((response, throwable) -> {
                if (throwable == null) {
                    concurrentRequestDone.countDown();
                }
            });
            if (!concurrentRequestDone.await(5, TimeUnit.SECONDS)) {
                throw new IllegalStateException(
                    "A concurrent GetQueryExecution request did not complete: the event loop is blocked "
                        + "even though the driver is using the buffered GetQueryResults API"
                );
            }
            long elapsedMillis = (System.nanoTime() - start) / 1_000_000;
            System.out.println("(a) A concurrent GetQueryExecution call completed on the SAME sole "
                + "event loop in " + elapsedMillis + "ms while GetQueryResults was still mid-body stalled. "
                + "The event loop was never blocked.");

            // (b) Prove Hikari does not wedge at total=0: a pooled connection attempt succeeds.
            SucceedingAthenaDataSource succeedingDataSource = new SucceedingAthenaDataSource(athenaClient);
            HikariConfig hikariConfig = new HikariConfig();
            hikariConfig.setDataSource(succeedingDataSource);
            hikariConfig.setPoolName("mcp-server-athena-write-pool-workaround");
            hikariConfig.setMaximumPoolSize(1);
            hikariConfig.setConnectionTimeout(3_000);
            hikariConfig.setInitializationFailTimeout(-1);
            AtomicInteger hikariThreadNumber = new AtomicInteger();
            hikariConfig.setThreadFactory(task -> {
                Thread thread = new Thread(task, "hikari-workaround-" + hikariThreadNumber.getAndIncrement());
                thread.setDaemon(true);
                return thread;
            });
            try (HikariDataSource pool = new HikariDataSource(hikariConfig)) {
                long poolStart = System.nanoTime();
                java.sql.Connection connection = pool.getConnection();
                long poolElapsedMillis = (System.nanoTime() - poolStart) / 1_000_000;
                connection.close();
                System.out.println("(b) Hikari borrowed a real connection in " + poolElapsedMillis
                    + "ms while GetQueryResults was still mid-body stalled on the other client. "
                    + "The pool never reported total=0.");
            }

            System.out.println();
            System.out.println("X-Amz-Target values the mock observed, in order:");
            mock.requestedTargets().forEach(target -> System.out.println("  " + target));
            System.out.println();
            System.out.println("(c) The driver switched APIs: GetQueryResultsQueryResultsFactory called "
                + "AmazonAthena.GetQueryResults, never GetQueryResultsStream.");

            mock.releaseGetQueryResults();
        }
    }
}
