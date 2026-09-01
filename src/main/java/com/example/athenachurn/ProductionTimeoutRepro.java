package com.example.athenachurn;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.HikariPoolMXBean;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLTransientConnectionException;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Scenarios 3-5: whether the client-construction churn from {@link AthenaClientChurnRepro} (and
 * a completely different, deterministic failure mode) can produce the production symptom
 * {@code Connection is not available, request timed out after 3000ms (total=...)}, and whether
 * a wedged pool ever recovers on its own.
 *
 * <p>This runs against a local mock Athena endpoint ({@link MockAthenaServer}), entirely
 * offline, over real HTTPS with a throwaway self-signed certificate. It is a separate program
 * from {@link AthenaClientChurnRepro} (scenarios 1-2) and must run in its own JVM: it creates
 * and destroys several Hikari pools and takes real wall-clock time (Hikari timeouts, pool
 * recovery), and none of that should share process state with the offline churn counting.
 */
public final class ProductionTimeoutRepro {

    private static final int POOL_SIZE = 15;
    private static final long CONNECTION_TIMEOUT_MILLIS = 3000;

    public static void main(String[] args) throws Exception {
        MockAthenaServer mock = MockAthenaServer.startOnRandomPort();
        System.out.println("Mock Athena/S3 endpoint listening at " + mock.baseUrl());
        System.out.println();

        try {
            scenario3PhaseA(mock);
            scenario3PhaseB(mock, false);
            scenario3PhaseB(mock, true);
            scenario4RecoveryFromSlowness(mock);
            scenario4RecoveryFromFailures(mock);
            scenario5FrozenBadCredential(mock);
        } finally {
            mock.close();
        }
    }

    // ---------------------------------------------------------------------------------------
    // Scenario 3, phase A: healthy baseline.
    // ---------------------------------------------------------------------------------------

    private static void scenario3PhaseA(MockAthenaServer mock) throws Exception {
        header("Scenario 3, phase A: healthy baseline (mock latency 150ms/call)");
        mock.setLatencyMillis(150);
        mock.setFailing(false);
        mock.setAcceptedAccessKeyId(null);

        try (HikariDataSource pool = buildPool(mock, "phaseA", "dummy", false)) {
            waitForPoolToFill(pool, POOL_SIZE, 15_000);
            LoadResult result = runBorrowLoad(pool, 20, 8, CONNECTION_TIMEOUT_MILLIS);
            System.out.println("  " + result.describe());
        }
        System.out.println();
    }

    // ---------------------------------------------------------------------------------------
    // Scenario 3, phase B: slow Athena after all connections are evicted, unpinned vs pinned.
    // ---------------------------------------------------------------------------------------

    private static void scenario3PhaseB(MockAthenaServer mock, boolean pinned) throws Exception {
        header("Scenario 3, phase B (" + (pinned ? "PINNED" : "UNPINNED")
                + "): mock latency raised to 2500ms/call (~7.5s per connection) after eviction");
        mock.setLatencyMillis(150);
        mock.setFailing(false);
        mock.setAcceptedAccessKeyId(null);

        try (HikariDataSource pool = buildPool(mock, "phaseB-" + (pinned ? "pinned" : "unpinned"), "dummy", pinned)) {
            waitForPoolToFill(pool, POOL_SIZE, 15_000);
            System.out.println("  Pool warm at " + poolStatus(pool) + ". Raising mock latency and evicting all connections.");

            mock.setLatencyMillis(2500);
            HikariPoolMXBean mxBean = pool.getHikariPoolMXBean();
            mxBean.softEvictConnections();

            // 650ms > Hikari's 500ms aliveBypassWindow: forces isConnectionDead validation (and
            // therefore the churn-driving setNetworkTimeout calls) to run on every borrow that
            // reuses an existing connection, exactly like scenarios 1-2.
            Set<Integer> distinctClients = ConcurrentHashMap.newKeySet();
            LoadResult result = runBorrowLoadWithClientTracking(pool, 30, 5, CONNECTION_TIMEOUT_MILLIS, distinctClients, 650);
            System.out.println("  " + result.describe());
            System.out.println("  Distinct athenaSdkClient instances observed during this phase: " + distinctClients.size());
            if (!result.timeoutMessages.isEmpty()) {
                System.out.println("  Verbatim exception (first occurrence): " + result.timeoutMessages.get(0));
            }
        }
        System.out.println();
    }

    // ---------------------------------------------------------------------------------------
    // Scenario 4: recovery behavior.
    // ---------------------------------------------------------------------------------------

    private static void scenario4RecoveryFromSlowness(MockAthenaServer mock) throws Exception {
        header("Scenario 4a: recovery after mock latency drops back to 150ms");
        mock.setLatencyMillis(150);
        mock.setFailing(false);
        mock.setAcceptedAccessKeyId(null);

        try (HikariDataSource pool = buildPool(mock, "recovery-slow", "dummy", false)) {
            waitForPoolToFill(pool, POOL_SIZE, 15_000);
            mock.setLatencyMillis(1200);
            pool.getHikariPoolMXBean().softEvictConnections();
            // Put a little borrow pressure on the wedged pool, same as phase B, briefly.
            runBorrowLoad(pool, 20, 1, CONNECTION_TIMEOUT_MILLIS);

            System.out.println("  Restoring mock latency to 150ms and measuring recovery time.");
            long start = System.nanoTime();
            mock.setLatencyMillis(150);
            boolean recovered = waitUntilBorrowsSucceed(pool, 60_000);
            long elapsedMillis = (System.nanoTime() - start) / 1_000_000;
            System.out.println("  Recovered: " + recovered + " after " + elapsedMillis + "ms. Pool status: " + poolStatus(pool));
        }
        System.out.println();
    }

    private static void scenario4RecoveryFromFailures(MockAthenaServer mock) throws Exception {
        header("Scenario 4b: recovery after a 60s window of mock 500s");
        mock.setLatencyMillis(150);
        mock.setFailing(false);
        mock.setAcceptedAccessKeyId(null);

        try (HikariDataSource pool = buildPool(mock, "recovery-failures", "dummy", false)) {
            waitForPoolToFill(pool, POOL_SIZE, 15_000);
            mock.setFailing(true);
            pool.getHikariPoolMXBean().softEvictConnections();

            System.out.println("  Mock returning 500s for 60s, with borrow pressure applied throughout.");
            long failWindowStart = System.nanoTime();
            while ((System.nanoTime() - failWindowStart) < 60_000_000_000L) {
                runBorrowLoad(pool, 10, 1, CONNECTION_TIMEOUT_MILLIS);
            }

            System.out.println("  Restoring the mock to healthy and measuring recovery time.");
            long start = System.nanoTime();
            mock.setFailing(false);
            boolean recovered = waitUntilBorrowsSucceed(pool, 60_000);
            long elapsedMillis = (System.nanoTime() - start) / 1_000_000;
            System.out.println("  Recovered: " + recovered + " after " + elapsedMillis + "ms. Pool status: " + poolStatus(pool));
        }
        System.out.println();
    }

    // ---------------------------------------------------------------------------------------
    // Scenario 5: frozen bad credential - a pool that never recovers on its own.
    // ---------------------------------------------------------------------------------------

    private static void scenario5FrozenBadCredential(MockAthenaServer mock) throws Exception {
        header("Scenario 5: a deterministically-bad, process-frozen credential wedges the pool forever");
        mock.setLatencyMillis(150);
        mock.setFailing(false);
        mock.setAcceptedAccessKeyId(null);

        System.out.println("  Phase A: pool built with AccessKeyId=key-A, mock currently accepts any key.");
        try (HikariDataSource poolA = buildPool(mock, "scenario5-keyA", "key-A", false)) {
            waitForPoolToFill(poolA, POOL_SIZE, 15_000);
            LoadResult healthy = runBorrowLoad(poolA, 15, 2, CONNECTION_TIMEOUT_MILLIS);
            System.out.println("  " + healthy.describe());

            System.out.println();
            System.out.println("  Phase B: mock now only accepts key-B. key-A (frozen in poolA's config) is");
            System.out.println("  rejected with 403 UnrecognizedClientException, exactly like a rotated real");
            System.out.println("  AWS credential. Evicting poolA's connections and retrying borrows for ~130s.");
            mock.setAcceptedAccessKeyId("key-B");
            poolA.getHikariPoolMXBean().softEvictConnections();

            long wedgeStart = System.nanoTime();
            AtomicInteger attempts = new AtomicInteger();
            AtomicInteger failures = new AtomicInteger();
            CopyOnWriteArrayList<String> messages = new CopyOnWriteArrayList<>();
            long deadline = wedgeStart + 130_000_000_000L;
            while (System.nanoTime() < deadline) {
                attempts.incrementAndGet();
                try (Connection c = poolA.getConnection()) {
                    // Would only happen if the wedge somehow cleared; it should not.
                } catch (SQLTransientConnectionException e) {
                    failures.incrementAndGet();
                    if (messages.size() < 3) {
                        messages.add(e.getMessage());
                    }
                } catch (SQLException e) {
                    failures.incrementAndGet();
                    if (messages.size() < 3) {
                        messages.add(e.getClass().getSimpleName() + ": " + e.getMessage());
                    }
                }
            }
            long wedgeElapsedMillis = (System.nanoTime() - wedgeStart) / 1_000_000;
            System.out.println("  Retried for " + wedgeElapsedMillis + "ms: " + attempts.get() + " borrow attempts, "
                    + failures.get() + " failed.");
            for (String m : messages) {
                System.out.println("  Verbatim exception: " + m);
            }

            System.out.println();
            System.out.println("  Direct proof the endpoint itself is healthy: a fresh connect() with key-B, ");
            System.out.println("  made independently of poolA, while poolA is still wedged:");
            Properties direct = baseProperties(mock, "key-B");
            try (Connection direct1 = new com.amazon.athena.jdbc.AthenaDriver().connect("jdbc:athena://", direct)) {
                System.out.println("  Direct connect with key-B while poolA is wedged: SUCCEEDED (" + direct1 + ")");
            } catch (SQLException e) {
                System.out.println("  Direct connect with key-B while poolA is wedged: FAILED unexpectedly: " + e);
            }

            System.out.println();
            System.out.println("  Phase C: does poolA (still configured with key-A) ever recover on its own?");
            boolean poolARecovered = waitUntilBorrowsSucceed(poolA, 10_000);
            System.out.println("  poolA self-recovery within 10s of extra waiting: " + poolARecovered
                    + " (expected: false - its AccessKeyId is frozen at pool-creation time)");

            System.out.println();
            System.out.println("  A brand NEW pool built with key-B recovers immediately - only a fresh");
            System.out.println("  pool/process (which re-reads the credential) fixes this, not waiting:");
            try (HikariDataSource poolB = buildPool(mock, "scenario5-keyB", "key-B", false)) {
                waitForPoolToFill(poolB, POOL_SIZE, 15_000);
                LoadResult recovered = runBorrowLoad(poolB, 15, 2, CONNECTION_TIMEOUT_MILLIS);
                System.out.println("  New pool with key-B: " + recovered.describe());
            }
        }
        System.out.println();
    }

    // ---------------------------------------------------------------------------------------
    // Pool / load-test plumbing.
    // ---------------------------------------------------------------------------------------

    private static Properties baseProperties(MockAthenaServer mock, String accessKeyId) {
        Properties dataSourceProperties = new Properties();
        dataSourceProperties.put("Region", "eu-central-1");
        dataSourceProperties.put("Workgroup", "primary");
        dataSourceProperties.put("Catalog", "AwsDataCatalog");
        dataSourceProperties.put("Database", "default");
        dataSourceProperties.put("AccessKeyId", accessKeyId);
        dataSourceProperties.put("SecretAccessKey", "dummy-secret");
        dataSourceProperties.put("ConnectionTest", "true");
        dataSourceProperties.put("AthenaEndpoint", mock.baseUrl());
        dataSourceProperties.put("S3Endpoint", mock.baseUrl());
        dataSourceProperties.put("AthenaStreamingEndpoint", mock.baseUrl());
        dataSourceProperties.put("ResultFetcher", "GetQueryResults");
        return dataSourceProperties;
    }

    private static HikariDataSource buildPool(MockAthenaServer mock, String name, String accessKeyId, boolean pinned) {
        Properties dataSourceProperties = baseProperties(mock, accessKeyId);
        long validationTimeout = 5_000L;
        if (pinned) {
            validationTimeout = 15_000L;
            dataSourceProperties.put("NetworkTimeoutMillis", "15000");
        }

        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl("jdbc:athena://");
        hikariConfig.setDataSourceProperties(dataSourceProperties);
        hikariConfig.setMaximumPoolSize(POOL_SIZE);
        hikariConfig.setValidationTimeout(validationTimeout);
        hikariConfig.setConnectionTimeout(CONNECTION_TIMEOUT_MILLIS);
        hikariConfig.setPoolName(name);
        return new HikariDataSource(hikariConfig);
    }

    private static void waitForPoolToFill(HikariDataSource pool, int target, long timeoutMillis) throws InterruptedException {
        long deadline = System.nanoTime() + timeoutMillis * 1_000_000L;
        HikariPoolMXBean mxBean = pool.getHikariPoolMXBean();
        while (System.nanoTime() < deadline) {
            if (mxBean.getTotalConnections() >= target && mxBean.getIdleConnections() >= 1) {
                return;
            }
            Thread.sleep(100);
        }
    }

    private static boolean waitUntilBorrowsSucceed(HikariDataSource pool, long timeoutMillis) throws InterruptedException {
        long deadline = System.nanoTime() + timeoutMillis * 1_000_000L;
        HikariPoolMXBean mxBean = pool.getHikariPoolMXBean();
        while (System.nanoTime() < deadline) {
            if (mxBean.getTotalConnections() >= POOL_SIZE) {
                try (Connection c = pool.getConnection()) {
                    return true;
                } catch (SQLException ignored) {
                    // keep waiting
                }
            }
            Thread.sleep(250);
        }
        return false;
    }

    private static String poolStatus(HikariDataSource pool) {
        HikariPoolMXBean mxBean = pool.getHikariPoolMXBean();
        return "total=" + mxBean.getTotalConnections() + ", active=" + mxBean.getActiveConnections()
                + ", idle=" + mxBean.getIdleConnections() + ", waiting=" + mxBean.getThreadsAwaitingConnection();
    }

    /** Runs {@code threads * borrowsPerThread} borrow/return cycles concurrently, back to back. */
    private static LoadResult runBorrowLoad(HikariDataSource pool, int threads, int borrowsPerThread, long connectionTimeoutMillis)
            throws InterruptedException {
        return runBorrowLoadWithClientTracking(pool, threads, borrowsPerThread, connectionTimeoutMillis, null, 0);
    }

    /**
     * @param idleBetweenBorrowsMillis if greater than 0, each thread sleeps this long after
     *     returning a connection before borrowing again. HikariCP's 500ms {@code aliveBypassWindow}
     *     skips {@code isConnectionDead} validation - and therefore skips the
     *     {@code setNetworkTimeout} calls that drive the churn bug - for a connection borrowed
     *     again within 500ms of being returned. Pass something above 500ms to actually exercise
     *     validation on every borrow, exactly as scenarios 1-2 do.
     */
    private static LoadResult runBorrowLoadWithClientTracking(
            HikariDataSource pool, int threads, int borrowsPerThread, long connectionTimeoutMillis,
            Set<Integer> distinctClientsOut, long idleBetweenBorrowsMillis)
            throws InterruptedException {
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        CountDownLatch done = new CountDownLatch(threads);
        CopyOnWriteArrayList<Long> latenciesMillis = new CopyOnWriteArrayList<>();
        AtomicInteger successes = new AtomicInteger();
        AtomicInteger timeouts = new AtomicInteger();
        AtomicInteger otherFailures = new AtomicInteger();
        CopyOnWriteArrayList<String> timeoutMessages = new CopyOnWriteArrayList<>();

        for (int t = 0; t < threads; t++) {
            executor.submit(() -> {
                try {
                    for (int i = 0; i < borrowsPerThread; i++) {
                        long start = System.nanoTime();
                        try (Connection c = pool.getConnection()) {
                            long elapsedMillis = (System.nanoTime() - start) / 1_000_000;
                            latenciesMillis.add(elapsedMillis);
                            successes.incrementAndGet();
                            if (distinctClientsOut != null) {
                                try {
                                    recordClientIdentity(c, distinctClientsOut);
                                } catch (ReflectiveOperationException ignored) {
                                    // best-effort instrumentation only
                                }
                            }
                        } catch (SQLTransientConnectionException e) {
                            timeouts.incrementAndGet();
                            if (timeoutMessages.size() < 5) {
                                timeoutMessages.add(e.getMessage());
                            }
                        } catch (SQLException e) {
                            otherFailures.incrementAndGet();
                        }
                        if (idleBetweenBorrowsMillis > 0) {
                            try {
                                Thread.sleep(idleBetweenBorrowsMillis);
                            } catch (InterruptedException interrupted) {
                                Thread.currentThread().interrupt();
                                return;
                            }
                        }
                    }
                } finally {
                    done.countDown();
                }
            });
        }
        done.await(120, TimeUnit.SECONDS);
        executor.shutdownNow();

        return new LoadResult(successes.get(), timeouts.get(), otherFailures.get(), latenciesMillis, timeoutMessages);
    }

    private static void recordClientIdentity(Connection pooled, Set<Integer> out) throws ReflectiveOperationException, SQLException {
        Object athenaConnection = pooled.unwrap(com.amazon.athena.jdbc.AthenaConnection.class);
        Field configField = findField(athenaConnection.getClass(), "configuration");
        configField.setAccessible(true);
        Object configuration = configField.get(athenaConnection);
        configuration.getClass().getMethod("getAthenaClient").invoke(configuration);
        Field clientField = findField(configuration.getClass(), "athenaSdkClient");
        clientField.setAccessible(true);
        Object client = clientField.get(configuration);
        if (client != null) {
            out.add(System.identityHashCode(client));
        }
    }

    private static Field findField(Class<?> type, String name) {
        for (Class<?> c = type; c != null; c = c.getSuperclass()) {
            try {
                return c.getDeclaredField(name);
            } catch (NoSuchFieldException ignored) {
                // keep walking up
            }
        }
        return null;
    }

    private static void header(String text) {
        System.out.println(text);
        System.out.println("-".repeat(Math.min(text.length(), 100)));
    }

    private static final class LoadResult {
        final int successes;
        final int timeouts;
        final int otherFailures;
        final CopyOnWriteArrayList<Long> latenciesMillis;
        final CopyOnWriteArrayList<String> timeoutMessages;

        LoadResult(int successes, int timeouts, int otherFailures, CopyOnWriteArrayList<Long> latenciesMillis,
                CopyOnWriteArrayList<String> timeoutMessages) {
            this.successes = successes;
            this.timeouts = timeouts;
            this.otherFailures = otherFailures;
            this.latenciesMillis = latenciesMillis;
            this.timeoutMessages = timeoutMessages;
        }

        String describe() {
            java.util.List<Long> sorted = new java.util.ArrayList<>(latenciesMillis);
            java.util.Collections.sort(sorted);
            long p50 = percentile(sorted, 50);
            long p99 = percentile(sorted, 99);
            return "borrows: successes=" + successes + ", timeouts=" + timeouts + ", otherFailures=" + otherFailures
                    + ", borrow p50=" + p50 + "ms, p99=" + p99 + "ms";
        }

        private static long percentile(java.util.List<Long> sorted, int pct) {
            if (sorted.isEmpty()) {
                return -1;
            }
            int idx = Math.min(sorted.size() - 1, (sorted.size() * pct) / 100);
            return sorted.get(idx);
        }
    }
}
