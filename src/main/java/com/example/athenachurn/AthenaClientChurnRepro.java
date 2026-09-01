package com.example.athenachurn;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * Reproduces a client-construction-churn defect in the AWS Athena JDBC driver 3.8.0.
 *
 * <p>{@code com.amazon.athena.jdbc.configuration.ConnectionConfiguration#setApiRequestTimeout}
 * discards its five lazily built AWS SDK clients (athenaClient, athenaSdkClient,
 * athenaStreamingClient, s3SdkClient, glueSdkClient) whenever the value differs from the current
 * one. It does not close them first. HikariCP calls {@code Connection#setNetworkTimeout} — which
 * this driver routes straight into {@code setApiRequestTimeout} — on every pool borrow that runs
 * validation, and again with a different hardcoded value when it retires a connection. Unless the
 * pool's validation timeout and the driver's own reported network timeout happen to be equal, the
 * driver rebuilds all five SDK clients on almost every borrow, and never closes the old ones.
 *
 * <p>This program runs the buggy scenario (default Hikari settings against the default driver
 * timeout) and a workaround scenario (the driver's {@code NetworkTimeoutMillis} pinned to
 * Hikari's validation timeout), and counts the distinct SDK client instances built in each case.
 * It needs no AWS account and no network access to AWS: {@code ConnectionTest=false} stops the
 * driver's own connect-time query, and constructing an SDK client resolves no credentials by
 * itself (credential resolution happens when a request is signed, not at construction).
 */
public final class AthenaClientChurnRepro {

    /** Field names on {@code ConnectionConfiguration} that hold the lazily built SDK clients. */
    private static final List<String> CLIENT_FIELDS =
            List.of("athenaClient", "athenaSdkClient", "s3SdkClient", "athenaStreamingClient", "glueSdkClient");

    private static final int[] BORROW_CHECKPOINTS = {5, 20, 40};

    public static void main(String[] args) throws Exception {
        System.out.println("Athena JDBC driver 3.8.0 - SDK client construction churn reproduction");
        System.out.println("========================================================================");
        System.out.println();

        Set<String> startThreads = matchingThreadNames();

        System.out.println("Scenario A: default Hikari validation timeout, driver default network timeout (BUG)");
        System.out.println("--------------------------------------------------------------------------------");
        Result buggy = runScenario(false);
        printTable(buggy);
        Set<String> peakThreads = matchingThreadNames();
        System.out.printf(
                "Threads matching 'athena-jdbc'/'aws-java-sdk-NettyEventLoop' right after scenario A: %d%n",
                peakThreads.size());
        System.out.println();

        System.out.println("Scenario B: NetworkTimeoutMillis pinned to Hikari's validationTimeout (WORKAROUND)");
        System.out.println("--------------------------------------------------------------------------------");
        Result workaround = runScenario(true);
        printTable(workaround);
        System.out.println();

        System.out.println("Waiting 12 seconds before the final thread count, to show thread counts stay bounded");
        System.out.println("(3.8.0 fixed the pre-3.8 thread leak with allowCoreThreadTimeOut; this repro is about");
        System.out.println("client CONSTRUCTION churn, a separate, still-open defect).");
        Thread.sleep(12_000);
        Set<String> endThreads = matchingThreadNames();
        System.out.printf(
                "Threads matching 'athena-jdbc'/'aws-java-sdk-NettyEventLoop' - start: %d, end: %d%n",
                startThreads.size(), endThreads.size());
        System.out.println();

        int buggyAt40 = buggy.countAtBorrow("athenaSdkClient", 40);
        int workaroundAt40 = workaround.countAtBorrow("athenaSdkClient", 40);
        System.out.println("SUMMARY");
        System.out.println("-------");
        System.out.printf(
                "BUG REPRODUCED: 40 borrows created %d distinct AthenaAsyncClient instances; "
                        + "with NetworkTimeoutMillis pinned: %d%n",
                buggyAt40, workaroundAt40);
    }

    private static Result runScenario(boolean pinNetworkTimeout) throws Exception {
        Properties dataSourceProperties = new Properties();
        dataSourceProperties.put("Region", "eu-central-1");
        dataSourceProperties.put("Workgroup", "primary");
        dataSourceProperties.put("Catalog", "AwsDataCatalog");
        dataSourceProperties.put("Database", "default");
        dataSourceProperties.put("AccessKeyId", "dummy");
        dataSourceProperties.put("SecretAccessKey", "dummy");
        // Stops the driver issuing its own connect-time "select 1"; this whole repro runs offline.
        dataSourceProperties.put("ConnectionTest", "false");

        long validationTimeoutMillis = 5_000L;
        if (pinNetworkTimeout) {
            // Make the value HikariCP will set equal to the value the driver already reports,
            // so ConnectionConfiguration#setApiRequestTimeout's equals-early-return fires and the
            // SDK clients are never discarded.
            dataSourceProperties.put("NetworkTimeoutMillis", String.valueOf(validationTimeoutMillis));
        }

        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl("jdbc:athena://");
        hikariConfig.setDataSourceProperties(dataSourceProperties);
        hikariConfig.setMaximumPoolSize(1);
        hikariConfig.setMinimumIdle(1);
        hikariConfig.setValidationTimeout(validationTimeoutMillis);
        hikariConfig.setConnectionTimeout(30_000);
        hikariConfig.setPoolName(pinNetworkTimeout ? "workaround-pool" : "buggy-pool");

        Map<String, Set<Integer>> distinctIdentitiesByField = new LinkedHashMap<>();
        Map<String, Map<Integer, Integer>> countAtBorrowByField = new LinkedHashMap<>();
        for (String field : CLIENT_FIELDS) {
            distinctIdentitiesByField.put(field, new LinkedHashSet<>());
            countAtBorrowByField.put(field, new LinkedHashMap<>());
        }

        try (HikariDataSource dataSource = new HikariDataSource(hikariConfig)) {
            int maxBorrows = BORROW_CHECKPOINTS[BORROW_CHECKPOINTS.length - 1];
            for (int borrow = 1; borrow <= maxBorrows; borrow++) {
                try (Connection pooled = dataSource.getConnection()) {
                    Object athenaConnection = unwrap(pooled);
                    Object configuration = configurationOf(athenaConnection);

                    // Client construction is lazy: touch the getter to force it, exactly as
                    // issuing a query would.
                    invokeNoArg(configuration, "getAthenaClient");

                    for (String field : CLIENT_FIELDS) {
                        Object client = readField(configuration, field);
                        if (client != null) {
                            distinctIdentitiesByField.get(field).add(System.identityHashCode(client));
                        }
                    }
                }

                // HikariCP's aliveBypassWindow is 500ms: a borrow inside that window skips
                // isConnectionDead entirely, so setNetworkTimeout is never called and no churn
                // happens. Sleep past it so every borrow actually validates.
                Thread.sleep(650);

                for (int checkpoint : BORROW_CHECKPOINTS) {
                    if (borrow == checkpoint) {
                        for (String field : CLIENT_FIELDS) {
                            countAtBorrowByField.get(field).put(checkpoint, distinctIdentitiesByField.get(field).size());
                        }
                    }
                }
            }
        }

        Result result = new Result();
        result.countAtBorrowByField = countAtBorrowByField;
        result.distinctIdentitiesByField = distinctIdentitiesByField;
        return result;
    }

    private static void printTable(Result result) {
        StringBuilder header = new StringBuilder(String.format("%-10s", "borrows"));
        for (String field : CLIENT_FIELDS) {
            header.append(String.format(" | %-16s", field));
        }
        System.out.println(header);
        for (int checkpoint : BORROW_CHECKPOINTS) {
            StringBuilder row = new StringBuilder(String.format("%-10d", checkpoint));
            for (String field : CLIENT_FIELDS) {
                row.append(String.format(" | %-16d", result.countAtBorrow(field, checkpoint)));
            }
            System.out.println(row);
        }
    }

    /** Unwraps HikariCP's proxy connection down to the driver's own {@code AthenaConnection}. */
    private static Object unwrap(Connection pooled) throws SQLException {
        try {
            return pooled.unwrap(Class.forName("com.amazon.athena.jdbc.AthenaConnection"));
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException(e);
        } catch (SQLException unwrapFailed) {
            // Fall back to reflecting through Hikari's proxy delegate field.
            try {
                Object candidate = pooled;
                while (candidate != null && !candidate.getClass().getName().equals("com.amazon.athena.jdbc.AthenaConnection")) {
                    Field delegate = findField(candidate.getClass(), "delegate");
                    if (delegate == null) {
                        break;
                    }
                    delegate.setAccessible(true);
                    candidate = delegate.get(candidate);
                }
                if (candidate == null) {
                    throw unwrapFailed;
                }
                return candidate;
            } catch (ReflectiveOperationException reflectionFailed) {
                throw unwrapFailed;
            }
        }
    }

    private static Object configurationOf(Object athenaConnection) throws ReflectiveOperationException {
        Field field = findField(athenaConnection.getClass(), "configuration");
        field.setAccessible(true);
        return field.get(athenaConnection);
    }

    private static Object readField(Object target, String name) throws ReflectiveOperationException {
        Field field = findField(target.getClass(), name);
        field.setAccessible(true);
        return field.get(target);
    }

    private static void invokeNoArg(Object target, String methodName) throws ReflectiveOperationException {
        target.getClass().getMethod(methodName).invoke(target);
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

    private static Set<String> matchingThreadNames() {
        Set<String> names = new LinkedHashSet<>();
        for (Thread t : Thread.getAllStackTraces().keySet()) {
            String name = t.getName();
            if (name.startsWith("athena-jdbc") || name.startsWith("aws-java-sdk-NettyEventLoop")) {
                names.add(name);
            }
        }
        return names;
    }

    private static final class Result {
        Map<String, Map<Integer, Integer>> countAtBorrowByField;
        Map<String, Set<Integer>> distinctIdentitiesByField;

        int countAtBorrow(String field, int checkpoint) {
            return countAtBorrowByField.get(field).getOrDefault(checkpoint, -1);
        }
    }
}
