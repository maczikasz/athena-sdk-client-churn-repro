package com.example.athenachurn.soak;

import com.example.athenachurn.mock.MockAthenaServer;
import org.jooq.DSLContext;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The wedge in five steps, each forced so the test is deterministic. {@link HealthCheckSoakTest}
 * shows the same five steps happening on their own.
 *
 * <p>Pool of one connection, so every borrow is the same physical connection. Real HikariCP, real
 * driver 3.8.0 (streaming fetcher), real jOOQ, real Reactor timeout. The only fake is Athena.
 */
class WedgeTest {

    private static final Duration PROBE_TIMEOUT = Duration.ofSeconds(3);

    @Test
    void timeoutThenDetachThenCloseWedgesTheEventLoop() throws Exception {
        try (MockAthenaServer athena = MockAthenaServer.startOnRandomPort();
             AffectedAthenaPool pool = AffectedAthenaPool.start(athena.baseUrl(), Duration.ofHours(8), 1)) {
            DSLContext dsl = pool.dsl();
            pool.warmUp();

            // 1. The health probe times out on a slow query. Reactor interrupts the worker. The driver
            //    rethrows with no SQLState, so HikariCP keeps the connection. The statement's own
            //    poll -> fetch chain keeps running with nobody waiting for it.
            athena.armQueryRunningHold();
            athena.armStreamingHeaderHoldOnce();
            String probe = Mono.fromCallable(() -> { dsl.fetch(HealthProbe.PROBE_2); return "UP"; })
                .subscribeOn(Schedulers.boundedElastic())
                .timeout(PROBE_TIMEOUT, Mono.just("DOWN"))
                .onErrorResume(e -> Mono.just("DOWN"))
                .block();
            assertTrue(probe.startsWith("DOWN"), probe);

            // 2. Something borrows the connection again. HikariCP calls setNetworkTimeout around
            //    isValid, and the driver nulls its SDK clients without closing them. The orphaned
            //    chain still holds the old streaming client; the driver no longer does.
            Thread.sleep(700); // past HikariCP's 500 ms alive-bypass window, so isValid runs
            pool.dataSource().getConnection().close();

            // 3. The query completes. The orphaned chain fetches the stream on the detached client.
            //    The mock holds the response headers so the request stays in flight.
            athena.releaseQueryCompletion();
            assertTrue(athena.awaitStreamingRequestArrived(15, TimeUnit.SECONDS), "orphaned chain never fetched the stream");

            // 4. The connection is physically closed (maxLifetime in production, eviction here).
            //    ConnectionConfiguration.close() closes the clients it still references - not the
            //    detached one - and shuts the completion executor down.
            pool.dataSource().getHikariPoolMXBean().softEvictConnections();
            Thread.sleep(300);

            // 5. The held response arrives. No executor to hop to, so the SDK completes on the Netty
            //    event loop, where the driver's blocking parse parks forever.
            athena.releaseStreamingHeaders();
            Thread.sleep(2_500);

            List<String> parked = EventLoopWatchdog.parkedInStreamingParser();
            assertFalse(parked.isEmpty(), "expected a Netty event-loop thread parked in GetQueryResultsStreamResponseParser.parse");
            System.out.println("WEDGED: " + parked);
        }
    }
}
