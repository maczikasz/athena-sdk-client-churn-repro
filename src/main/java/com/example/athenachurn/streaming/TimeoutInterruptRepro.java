package com.example.athenachurn.streaming;

import com.amazon.athena.jdbc.AthenaDriver;
import com.amazon.athena.jdbc.configuration.ConnectionParameters;
import com.example.athenachurn.mock.MockAthenaServer;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.HikariPoolMXBean;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.conf.RenderQuotedNames;
import org.jooq.impl.DSL;
import org.jooq.impl.DefaultConfiguration;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The production-shaped onset, end to end, on the real stack: HikariCP 7.1.0 pool of real
 * driver 3.8.0 connections, a jOOQ 3.21.6 {@code fetch}, and a Reactor 3.8.7
 * {@code Mono.fromCallable(..).subscribeOn(boundedElastic()).timeout(T, fallback)} around it.
 * That is the exact shape of the Develocity mcp-server Athena connectivity health probe.
 *
 * <p>Unlike {@link MidFlightCloseRepro}, nothing here calls {@code shutdown()} by hand. The mock
 * keeps the query in state RUNNING past the timeout, then flips it to SUCCEEDED a configurable
 * delay later. Production data (Athena query history plus health-transition logs) shows one wedge
 * where the query completed 0.42 s after the timeout fired, and six recoveries where it completed
 * 5 s or more after. This scenario sweeps that delay and reports, for each run:
 * <ul>
 *   <li>what the driver did after the interrupt (did it try StopQueryExecution? did it ever issue
 *       GetQueryResultsStream?),</li>
 *   <li>whether the Hikari pool still serves connections,</li>
 *   <li>whether any Netty event-loop thread is parked in the streaming result parser.</li>
 * </ul>
 *
 * <p>The benign (long) delay runs first. A wedge kills the shared SDK event loop for the whole
 * JVM, so once one run wedges, every later run in the same process is meaningless.
 *
 * <p>The verified pieces of the chain, from bytecode: Reactor's {@code SchedulerTask.dispose()}
 * interrupts the boundedElastic worker; the driver rethrows the interrupt as a {@code SQLException}
 * without SQLState; Hikari's {@code checkException} does not evict on that; jOOQ closes the
 * Statement, which — because the statement is still EXECUTING — calls {@code cancel()} and so
 * {@code StopQueryExecution}. What this scenario measures is whether any of that ends with a
 * streamed response completing on the event loop after the completion executor is gone.
 */
public final class TimeoutInterruptRepro {

    // Production uses PT30S. The ratio to the completion delay is what matters, not the absolute.
    private static final Duration PROBE_TIMEOUT = Duration.ofSeconds(3);
    private static final long[] DEFAULT_COMPLETION_DELAYS_MILLIS = {5_000, 400};
    private static final String PROBE_SQL = "SELECT id FROM build WHERE build_start_date = current_date - interval '1' day"
        + " AND build_start_time = '000000' LIMIT 1";

    private TimeoutInterruptRepro() {
    }

    public static void main(String[] args) throws Exception {
        long[] delays = args.length == 0
            ? DEFAULT_COMPLETION_DELAYS_MILLIS
            : Arrays.stream(args[0].split(",")).mapToLong(s -> Long.parseLong(s.trim())).toArray();

        try (MockAthenaServer mock = MockAthenaServer.startOnRandomPort();
             HikariDataSource pool = productionShapedPool(mock.baseUrl())) {
            DSLContext dsl = productionShapedDsl(pool);

            // Warm the pool and the SDK clients the way a running server would have.
            dsl.fetch("select 1 one from (values(1))");
            System.out.println("Pool warm. " + poolState(pool));

            String phases = System.getProperty("phases", "sweep,B,C,E,D,F");
            int trials = Integer.getInteger("trials", 1);
            if (phases.equals("C") || phases.equals("F") || phases.equals("G")) {
                int wedged = 0;
                for (int i = 1; i <= trials; i++) {
                    System.out.println();
                    System.out.println("=== Phase " + phases + " trial " + i + "/" + trials + " ===");
                    boolean ok = phases.equals("C")
                        ? phaseCEvictDuringOrphanedStreamFetch(mock, pool, dsl, StreamHold.HEADERS, 1_500)
                        : phaseFDetachedClientThenClose(mock, !phases.equals("G"));
                    System.out.println("VERDICT phase" + phases + " trial " + i + " -> " + (ok ? "recovered" : "WEDGED"));
                    if (!ok) {
                        wedged++;
                        drainCheck(dsl, pool);
                        break; // the shared loop group is damaged; further trials are not independent
                    }
                }
                System.out.println("SUMMARY phase" + phases + ": " + wedged + " wedge(s) in " + trials + " trial(s) (stopped at first wedge)");
                return;
            }

            for (long delayMillis : delays) {
                System.out.println();
                System.out.println("=== Query completes " + delayMillis + " ms AFTER the " + PROBE_TIMEOUT.toMillis()
                    + " ms probe timeout ===");
                boolean healthy = runOnce(mock, pool, dsl, Duration.ofMillis(delayMillis));
                System.out.println("VERDICT delay=" + delayMillis + "ms -> " + (healthy ? "recovered" : "WEDGED"));
                if (!healthy) {
                    System.out.println("Shared event loop is wedged; later runs in this JVM would be meaningless. Stopping.");
                    return;
                }
            }

            System.out.println();
            System.out.println("=== Phase B: explicit Statement.close() while the query is RUNNING — is StopQueryExecution sent? ===");
            phaseBStatementCloseWhileRunning(mock, pool);

            System.out.println();
            System.out.println("=== Phase C: orphaned stream fetch in flight + Hikari eviction (real ConnectionConfiguration.close()) ===");
            boolean healthy = phaseCEvictDuringOrphanedStreamFetch(mock, pool, dsl, StreamHold.HEADERS, 1_500);
            System.out.println("VERDICT phaseC (headers held, release 1.5 s after eviction) -> " + (healthy ? "recovered" : "WEDGED"));
            if (!healthy) {
                return;
            }

            System.out.println();
            System.out.println("=== Phase E: as C, but the held headers are released ~50 ms after eviction ===");
            healthy = phaseCEvictDuringOrphanedStreamFetch(mock, pool, dsl, StreamHold.HEADERS, 50);
            System.out.println("VERDICT phaseE (headers held, release 50 ms after eviction) -> " + (healthy ? "recovered" : "WEDGED"));
            if (!healthy) {
                return;
            }

            System.out.println();
            System.out.println("=== Phase D: orphaned stream MID-BODY (headers + metadata delivered, data row held) + Hikari eviction ===");
            healthy = phaseCEvictDuringOrphanedStreamFetch(mock, pool, dsl, StreamHold.MID_BODY, 1_500);
            System.out.println("VERDICT phaseD (mid-body hold) -> " + (healthy ? "recovered" : "WEDGED"));
            if (!healthy) {
                return;
            }

            System.out.println();
            System.out.println("=== Phase F: orphan's SDK client detached by Hikari's setNetworkTimeout (Finding 2), THEN close() ===");
            healthy = phaseFDetachedClientThenClose(mock, true);
            System.out.println("VERDICT phaseF -> " + (healthy ? "recovered" : "WEDGED"));
        }
    }

    /**
     * The one sequence in which the real {@code ConnectionConfiguration.close()} can leave a
     * streamed response in flight: a one-connection pool so every borrow hits the same physical
     * connection. (1) A probe times out; its orphaned poll -> fetch chain holds the streaming SDK
     * client it was created with. (2) The connection is returned and, after Hikari's 500 ms
     * alive-bypass window, borrowed again: Hikari calls {@code setNetworkTimeout} around
     * {@code isValid}, and the driver's {@code setApiRequestTimeout} nulls its five client fields
     * without closing them (Finding 2). The orphan's client is now referenced only by its own
     * in-flight request. (3) The orphan issues GetQueryResultsStream; the mock holds the headers.
     * (4) Hikari evicts the connection: {@code close()} closes only the clients it still
     * references — not the orphan's — and shuts both executors down. (5) The mock releases the
     * headers. The response future completes; the hop onto the completion executor is rejected;
     * the SDK completes it on the Netty event loop; the blocking parse runs there.
     */
    /** @param evict false = Phase G: detachment only, no physical close. Tests whether Finding 2 alone suffices. */
    private static boolean phaseFDetachedClientThenClose(MockAthenaServer mock, boolean evict) throws Exception {
        try (HikariDataSource single = productionShapedPool(mock.baseUrl(), 1)) {
            DSLContext dsl = productionShapedDsl(single);
            dsl.fetch("select 1 one from (values(1))");
            int targetsBefore = mock.requestedTargets().size();

            mock.armQueryRunningHold();
            mock.armStreamingHeaderHoldOnce();
            String outcome = Mono.fromCallable(() -> { dsl.fetch(PROBE_SQL); return "UP"; })
                .subscribeOn(Schedulers.boundedElastic())
                .timeout(PROBE_TIMEOUT, Mono.just("DOWN (probe exceeded " + PROBE_TIMEOUT + ")"))
                .onErrorResume(e -> Mono.just("DOWN (threw: " + e + ")"))
                .block();
            System.out.println("(1) Probe result: " + outcome + ". " + poolState(single));

            // (2) Past Hikari's alive-bypass window, a borrow validates via isValid, which calls
            // setNetworkTimeout twice; each call makes the driver null its SDK clients.
            Thread.sleep(700);
            try (java.sql.Connection c = single.getConnection()) {
                System.out.println("(2) Re-borrowed the same connection (isValid ran; driver clients nulled and rebuilt). "
                    + "Orphan chain still polling.");
            }

            // (3) Let the orphan see SUCCEEDED and issue the stream; the mock holds the headers.
            mock.releaseQueryCompletion();
            if (!mock.awaitStreamingRequestArrived(15, TimeUnit.SECONDS)) {
                System.out.println("The orphaned chain never issued GetQueryResultsStream.");
                mock.releaseStreamingHeaders();
                return true;
            }
            System.out.println("(3) Orphaned GetQueryResultsStream in flight on the DETACHED client (headers held).");

            if (evict) {
                // (4) Real close(): closes the current clients, skips the detached one, shuts executors.
                single.getHikariPoolMXBean().softEvictConnections();
                Thread.sleep(300);
                System.out.println("(4) Evicted -> ConnectionConfiguration.close() ran. " + poolState(single));
            } else {
                System.out.println("(4) SKIPPED (Phase G): no physical close; only the detachment from step (2).");
            }

            // (5) The held response now arrives.
            mock.releaseStreamingHeaders();
            Thread.sleep(2_500);
            System.out.println("Requests this phase: " + mock.requestedTargets().subList(
                Math.min(targetsBefore, mock.requestedTargets().size()), mock.requestedTargets().size()));

            List<String> parked = eventLoopThreadsInParser();
            if (!parked.isEmpty()) {
                System.out.println("(5) Event-loop thread(s) parked in GetQueryResultsStreamResponseParser.parse:");
                parked.forEach(line -> System.out.println("  " + line));
            } else {
                System.out.println("(5) No event-loop thread is parked in the streaming parser.");
            }
            boolean served;
            try {
                Mono.fromCallable(() -> dsl.fetch("select 1 one from (values(1))"))
                    .subscribeOn(Schedulers.boundedElastic())
                    .block(Duration.ofSeconds(10));
                served = true;
            } catch (RuntimeException e) {
                served = false;
                System.out.println("Follow-up query failed: " + describe(e));
            }
            System.out.println("Follow-up query " + (served ? "succeeded" : "did not complete in 10 s") + ". " + poolState(single));
            return served && parked.isEmpty();
        }
    }

    private enum StreamHold { HEADERS, MID_BODY }

    /**
     * Isolates one link of the inferred chain: the driver's {@code close(boolean)} calls
     * {@code cancel()} (and so StopQueryExecution) only when the statement is still EXECUTING.
     * jOOQ's error path did not trigger it in the sweep above; this checks whether a plain JDBC
     * close during execution does.
     */
    private static void phaseBStatementCloseWhileRunning(MockAthenaServer mock, HikariDataSource pool) throws Exception {
        int stopsBefore = mock.stopQueryExecutionCount();
        mock.armQueryRunningHold();
        try (java.sql.Connection connection = pool.getConnection()) {
            java.sql.Statement statement = connection.createStatement();
            Thread executor = new Thread(() -> {
                try (java.sql.ResultSet rs = statement.executeQuery(PROBE_SQL)) {
                    rs.next();
                } catch (Exception e) {
                    System.out.println("executeQuery ended with: " + describe(e));
                }
            }, "phaseB-executor");
            executor.start();
            Thread.sleep(1_500); // query is RUNNING, executor thread parked in the driver's wait
            System.out.println("Calling Statement.close() while RUNNING ...");
            statement.close();
            Thread.sleep(500);
            System.out.println("StopQueryExecution attempts after Statement.close(): " + (mock.stopQueryExecutionCount() - stopsBefore));
            mock.releaseQueryCompletion();
            executor.join(10_000);
            System.out.println("Requests seen in phase B: " + tail(mock.requestedTargets(), 12));
        }
    }

    /**
     * The sufficiency test on the real path. After a caller-side timeout the driver's async
     * poll -> fetch chain is ownerless; once the query SUCCEEDs it issues GetQueryResultsStream.
     * The mock holds that stream's headers so the response is in flight. Then Hikari evicts every
     * idle connection ({@code softEvictConnections}), which physically closes them:
     * {@code AthenaConnection.close()} -> {@code ConnectionConfiguration.close()} -> executor
     * shutdown — the real driver code, not a hand-rolled {@code shutdown()}. Then the mock
     * releases the stream. If the SDK falls back to completing on the event loop and the parser
     * blocks there, the loop wedges and the follow-up query never returns.
     */
    private static boolean phaseCEvictDuringOrphanedStreamFetch(
        MockAthenaServer mock, HikariDataSource pool, DSLContext dsl, StreamHold hold, long releaseDelayAfterEvictionMillis
    ) throws Exception {
        int targetsBefore = mock.requestedTargets().size();
        mock.armQueryRunningHold();
        if (hold == StreamHold.HEADERS) {
            mock.armStreamingHeaderHoldOnce();
        } else {
            mock.armStreamingResponseHangOnce();
        }

        String outcome = Mono.fromCallable(() -> { dsl.fetch(PROBE_SQL); return "UP"; })
            .subscribeOn(Schedulers.boundedElastic())
            .timeout(PROBE_TIMEOUT, Mono.just("DOWN (probe exceeded " + PROBE_TIMEOUT + ")"))
            .onErrorResume(e -> Mono.just("DOWN (threw: " + e + ")"))
            .block();
        System.out.println("Probe result: " + outcome + ". " + poolState(pool));

        // Let the orphaned chain see SUCCEEDED and issue the stream fetch; the mock holds the headers.
        Thread.sleep(300);
        mock.releaseQueryCompletion();
        boolean arrived = hold == StreamHold.HEADERS
            ? mock.awaitStreamingRequestArrived(15, TimeUnit.SECONDS)
            : mock.awaitStreamingResponseHangStarted(15, TimeUnit.SECONDS);
        if (!arrived) {
            System.out.println("The orphaned chain never issued GetQueryResultsStream; nothing to race against.");
            System.out.println("Requests: " + tail(mock.requestedTargets(), 10));
            release(mock, hold);
            return true;
        }
        System.out.println("Orphaned GetQueryResultsStream is in flight (" + (hold == StreamHold.HEADERS
            ? "headers held by the mock" : "headers + metadata delivered, data row held") + ").");

        // Physically close the connections: real ConnectionConfiguration.close() on each.
        System.out.println("Evicting idle connections through Hikari ...");
        pool.getHikariPoolMXBean().softEvictConnections();
        Thread.sleep(releaseDelayAfterEvictionMillis);
        System.out.println("After eviction (+" + releaseDelayAfterEvictionMillis + " ms): " + poolState(pool));

        // Now the held response arrives, after its completion executor has been shut down.
        release(mock, hold);
        Thread.sleep(2_000);
        System.out.println("Requests this phase: " + mock.requestedTargets().subList(
            Math.min(targetsBefore, mock.requestedTargets().size()), mock.requestedTargets().size()));

        List<String> parked = eventLoopThreadsInParser();
        if (!parked.isEmpty()) {
            System.out.println("Event-loop thread(s) parked in GetQueryResultsStreamResponseParser.parse:");
            parked.forEach(line -> System.out.println("  " + line));
        } else {
            System.out.println("No event-loop thread is parked in the streaming parser.");
        }
        boolean served;
        try {
            Mono.fromCallable(() -> dsl.fetch("select 1 one from (values(1))"))
                .subscribeOn(Schedulers.boundedElastic())
                .block(Duration.ofSeconds(10));
            served = true;
        } catch (RuntimeException e) {
            served = false;
            System.out.println("Follow-up query failed: " + describe(e));
        }
        System.out.println("Follow-up query " + (served ? "succeeded" : "did not complete in 10 s") + ". " + poolState(pool));
        return served && parked.isEmpty();
    }

    /**
     * Production lost both pools over ~40 minutes, not instantly: every new request is assigned
     * round-robin to one of the shared event loops, and each one that lands on the wedged loop
     * hangs forever. Fire several follow-ups concurrently and count how many never return.
     */
    private static void drainCheck(DSLContext dsl, HikariDataSource pool) throws Exception {
        int n = 12;
        java.util.concurrent.ExecutorService ex = java.util.concurrent.Executors.newFixedThreadPool(n);
        List<java.util.concurrent.Future<String>> futures = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            futures.add(ex.submit(() -> { dsl.fetch("select 1 one from (values(1))"); return "ok"; }));
        }
        int hung = 0;
        for (java.util.concurrent.Future<String> f : futures) {
            try {
                f.get(8, TimeUnit.SECONDS);
            } catch (TimeoutException e) {
                hung++;
            } catch (Exception e) {
                System.out.println("  follow-up failed: " + describe(e));
            }
        }
        System.out.println("Drain check: " + hung + "/" + n + " concurrent follow-up queries did not return within 8 s. " + poolState(pool));
        ex.shutdownNow();
    }

    private static void release(MockAthenaServer mock, StreamHold hold) {
        if (hold == StreamHold.HEADERS) {
            mock.releaseStreamingHeaders();
        } else {
            mock.releaseStreamingResponse();
        }
    }

    private static List<String> tail(List<String> list, int n) {
        return list.subList(Math.max(0, list.size() - n), list.size());
    }

    /** @return true if the pool still serves queries afterwards, false if it wedged. */
    private static boolean runOnce(MockAthenaServer mock, HikariDataSource pool, DSLContext dsl, Duration completionDelay)
        throws Exception {
        int targetsBefore = mock.requestedTargets().size();
        int stopsBefore = mock.stopQueryExecutionCount();
        mock.armQueryRunningHold();

        AtomicReference<String> workerThread = new AtomicReference<>();
        AtomicReference<Throwable> workerFailure = new AtomicReference<>();
        long started = System.nanoTime();

        // The AdaptiveHealthProbe shape, verbatim: blocking probe on boundedElastic, Mono.timeout
        // with a fallback value. On timeout Reactor cancels upstream, which interrupts the worker.
        String outcome = Mono.fromCallable(() -> {
                workerThread.set(Thread.currentThread().getName());
                try {
                    dsl.fetch(PROBE_SQL);
                    return "UP";
                } catch (Throwable t) {
                    workerFailure.set(t);
                    throw t;
                }
            })
            .subscribeOn(Schedulers.boundedElastic())
            .timeout(PROBE_TIMEOUT, Mono.just("DOWN (probe exceeded " + PROBE_TIMEOUT + ")"))
            .onErrorResume(e -> Mono.just("DOWN (threw: " + e + ")"))
            .block();
        long timeoutFiredAtMillis = (System.nanoTime() - started) / 1_000_000;
        System.out.println("Probe result after " + timeoutFiredAtMillis + " ms: " + outcome);

        // Give the interrupted worker a moment to unwind through jOOQ / Hikari, then look at it.
        Thread.sleep(150);
        Throwable failure = workerFailure.get();
        System.out.println("Worker " + workerThread.get() + " -> "
            + (failure == null ? "still running (not interrupted!)" : describe(failure)));
        System.out.println("Requests the driver sent after the timeout so far: "
            + mock.requestedTargets().subList(Math.min(targetsBefore, mock.requestedTargets().size()), mock.requestedTargets().size()));
        System.out.println("StopQueryExecution attempts: " + (mock.stopQueryExecutionCount() - stopsBefore)
            + " (answered AccessDenied, as a read-only role would)");
        System.out.println(poolState(pool));

        // Now the query "finishes" on the Athena side, completionDelay after the timeout.
        long sinceTimeout = (System.nanoTime() - started) / 1_000_000 - PROBE_TIMEOUT.toMillis();
        long remaining = completionDelay.toMillis() - sinceTimeout;
        if (remaining > 0) {
            Thread.sleep(remaining);
        }
        mock.releaseQueryCompletion();
        System.out.println("Query flipped to SUCCEEDED " + ((System.nanoTime() - started) / 1_000_000 - PROBE_TIMEOUT.toMillis())
            + " ms after the timeout.");

        // Let any trailing poll / stream / completion land.
        Thread.sleep(2_000);
        System.out.println("All requests this run: " + mock.requestedTargets().subList(
            Math.min(targetsBefore, mock.requestedTargets().size()), mock.requestedTargets().size()));

        // Disconfirming probe 1: is any event-loop thread parked in the streaming parser?
        List<String> parked = eventLoopThreadsInParser();
        if (!parked.isEmpty()) {
            System.out.println("Event-loop thread(s) parked in GetQueryResultsStreamResponseParser.parse:");
            parked.forEach(line -> System.out.println("  " + line));
        } else {
            System.out.println("No event-loop thread is parked in the streaming parser.");
        }

        // Disconfirming probe 2: can the pool still serve a query within a bounded time?
        boolean served;
        try {
            Mono.fromCallable(() -> dsl.fetch("select 1 one from (values(1))"))
                .subscribeOn(Schedulers.boundedElastic())
                .block(Duration.ofSeconds(10));
            served = true;
        } catch (RuntimeException e) {
            served = !(rootCause(e) instanceof TimeoutException) && !e.getClass().getSimpleName().contains("Timeout");
            System.out.println("Follow-up query failed: " + describe(e));
        }
        System.out.println("Follow-up query " + (served ? "succeeded" : "did not complete in 10 s") + ". " + poolState(pool));
        return served && parked.isEmpty();
    }

    // ---- wiring: mirrors AthenaJdbcSupport in Develocity, with the incident-time streaming fetcher

    private static HikariDataSource productionShapedPool(String mockBaseUrl) {
        return productionShapedPool(mockBaseUrl, 15);
    }

    private static HikariDataSource productionShapedPool(String mockBaseUrl, int maxPoolSize) {
        Properties properties = new Properties();
        properties.put(ConnectionParameters.ATHENA_ENDPOINT_PARAMETER.name(), mockBaseUrl);
        properties.put(ConnectionParameters.ATHENA_STREAMING_ENDPOINT_PARAMETER.name(), mockBaseUrl);
        properties.put(ConnectionParameters.REGION_PARAMETER.name(), "eu-central-1");
        properties.put(ConnectionParameters.WORK_GROUP_PARAMETER.name(), "primary");
        properties.put(ConnectionParameters.CONNECTION_TEST_PARAMETER.name(), "false");
        properties.put(ConnectionParameters.CREDENTIALS_PROVIDER_PARAMETER.name(), "Static");
        properties.put(ConnectionParameters.USER_PARAMETER.name(), "dummy");
        properties.put(ConnectionParameters.PASSWORD_PARAMETER.name(), "dummy-secret");
        // The fetcher that was in production at incident time.
        properties.put(ConnectionParameters.RESULT_FETCHER.name(), "GetQueryResultsStream");
        properties.put(ConnectionParameters.NETWORK_TIMEOUT_MILLIS.name(), "15000");

        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:athena://");
        config.setDriverClassName(AthenaDriver.class.getName());
        config.setDataSourceProperties(properties);
        config.setPoolName("mcp-server-athena-pool-" + maxPoolSize);
        config.setMaximumPoolSize(maxPoolSize);
        config.setConnectionTimeout(Duration.ofSeconds(30).toMillis());
        config.setValidationTimeout(Duration.ofSeconds(15).toMillis());
        config.setMaxLifetime(Duration.ofHours(8).toMillis());
        config.setRegisterMbeans(true);
        return new HikariDataSource(config);
    }

    private static DSLContext productionShapedDsl(HikariDataSource pool) {
        DefaultConfiguration configuration = new DefaultConfiguration();
        configuration.set(SQLDialect.DEFAULT);
        configuration.set(pool);
        configuration.settings().setRenderQuotedNames(RenderQuotedNames.NEVER);
        return DSL.using(configuration);
    }

    // ---- diagnostics

    private static String poolState(HikariDataSource pool) {
        HikariPoolMXBean mx = pool.getHikariPoolMXBean();
        return "Hikari total=" + mx.getTotalConnections() + " active=" + mx.getActiveConnections()
            + " idle=" + mx.getIdleConnections() + " waiting=" + mx.getThreadsAwaitingConnection();
    }

    private static List<String> eventLoopThreadsInParser() {
        List<String> parked = new ArrayList<>();
        for (Map.Entry<Thread, StackTraceElement[]> entry : Thread.getAllStackTraces().entrySet()) {
            Thread thread = entry.getKey();
            if (!thread.getName().contains("NettyEventLoop")) {
                continue;
            }
            for (StackTraceElement frame : entry.getValue()) {
                if (frame.getClassName().endsWith("GetQueryResultsStreamResponseParser")) {
                    parked.add(thread.getName() + " " + thread.getState() + " at " + frame);
                    break;
                }
            }
        }
        return parked;
    }

    private static String describe(Throwable t) {
        Throwable root = rootCause(t);
        String sqlState = "";
        if (root instanceof java.sql.SQLException) {
            sqlState = " SQLState=" + ((java.sql.SQLException) root).getSQLState();
        }
        return t.getClass().getSimpleName() + " -> root " + root.getClass().getSimpleName() + sqlState
            + ": " + root.getMessage();
    }

    private static Throwable rootCause(Throwable t) {
        Throwable cause = t;
        while (cause.getCause() != null && cause.getCause() != cause) {
            cause = cause.getCause();
        }
        return cause;
    }
}
