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
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Soak test of the mcp-server Athena health probe against a randomly slow mock Athena, on the
 * pool configuration the incident build shipped with (Develocity before 2026-08-31):
 * <ul>
 *   <li>{@code maxLifetime} 30 min (scaled down here), {@code connectionTimeout} 3 s,
 *       {@code validationTimeout} 15 s,</li>
 *   <li>{@code connectionInitSql("select 1")} on top of the driver's own ConnectionTest,</li>
 *   <li>no {@code NetworkTimeoutMillis} pin, so HikariCP's {@code setNetworkTimeout} around every
 *       validation and every close makes the driver null its SDK clients without closing them,</li>
 *   <li>{@code ResultFetcher=GetQueryResultsStream}.</li>
 * </ul>
 * The probe is {@code AdaptiveHealthProbe} verbatim in shape: blocking jOOQ fetch on
 * {@code boundedElastic}, {@code Mono.timeout} with a DOWN fallback, short failure cadence while
 * DOWN. Alongside it run background "tool" queries, like MCP traffic. HikariCP logs at DEBUG so
 * every connection close carries its reason. A watchdog reports any Netty event-loop thread parked
 * in the streaming parser, with the timestamp, so the close that preceded it can be read off the
 * log. Run with {@code -XX:ActiveProcessorCount=1} to get the production pod's two-loop SDK group.
 */
public final class HealthCheckSoakRepro {

    /** What one soak run observed. {@code wedged} is the assertion the test makes. */
    public record Result(boolean wedged, String wedgeDetail, int probes, int timeouts, int toolQueries, int toolFailures,
                         int slowQueries, int slowApiCalls, String poolState) {
        public String summary() {
            return "probes=" + probes + " timeouts=" + timeouts + " tool ok/fail=" + toolQueries + "/" + toolFailures
                + " slow-queries=" + slowQueries + " slow-api=" + slowApiCalls + " wedged=" + wedged + " " + poolState;
        }
    }


    private static final String PROBE_1 = "select 1 one from (values(1))";
    private static final String PROBE_2 = "SELECT id FROM build WHERE build_start_date = current_date - interval '1' day"
        + " AND build_start_time = '000000' LIMIT 1";

    private HealthCheckSoakRepro() {
    }

    public static void main(String[] args) throws Exception {
        Result result = run(Duration.ofSeconds(Long.getLong("soak.seconds", 600)));
        System.out.println(ts() + " SUMMARY " + result.summary());
    }

    /**
     * Runs the soak for {@code runFor} (or until a wedge is detected) with the remaining settings
     * taken from {@code soak.*} system properties. Used by {@code main} and by the JUnit test.
     */
    public static Result run(Duration runFor) throws Exception {
        Duration probeTimeout = Duration.ofSeconds(Long.getLong("soak.probeTimeoutSeconds", 10));
        Duration passInterval = Duration.ofSeconds(Long.getLong("soak.passIntervalSeconds", 20));
        Duration failureInterval = Duration.ofSeconds(Long.getLong("soak.failureIntervalSeconds", 5));
        Duration maxLifetime = Duration.ofSeconds(Long.getLong("soak.maxLifetimeSeconds", 120));
        double slowQueryProbability = Double.parseDouble(System.getProperty("soak.slowQueryProbability", "0.25"));
        long slowMin = Long.getLong("soak.slowQueryMinMillis", 2_000);
        long slowMax = Long.getLong("soak.slowQueryMaxMillis", 25_000);
        double slowApiProbability = Double.parseDouble(System.getProperty("soak.slowApiCallProbability", "0.02"));
        long slowApiMillis = Long.getLong("soak.slowApiCallMillis", 4_000);
        int toolThreads = Integer.getInteger("soak.toolThreads", 3);
        long toolPauseMillis = Long.getLong("soak.toolPauseMillis", 1_500);

        System.out.println("Soak: " + runFor + ", probe timeout " + probeTimeout + ", pass/fail cadence " + passInterval + "/"
            + failureInterval + ", maxLifetime " + maxLifetime + ", slow-query p=" + slowQueryProbability + " [" + slowMin + ".."
            + slowMax + " ms], slow-api p=" + slowApiProbability + " (" + slowApiMillis + " ms), tool threads " + toolThreads);
        System.out.println("Event loops in this JVM's shared SDK group: expect 2 with -XX:ActiveProcessorCount=1 (cores="
            + Runtime.getRuntime().availableProcessors() + ")");

        try (MockAthenaServer mock = MockAthenaServer.startOnRandomPort();
             HikariDataSource pool = incidentBuildPool(mock.baseUrl(), maxLifetime)) {
            mock.enableSoakMode(slowQueryProbability, slowMin, slowMax, slowApiProbability, slowApiMillis);
            DSLContext dsl = dsl(pool);
            dsl.fetch(PROBE_1);
            System.out.println(ts() + " pool warm: " + poolState(pool));

            AtomicInteger probes = new AtomicInteger(), timeouts = new AtomicInteger(), toolQueries = new AtomicInteger(),
                toolFailures = new AtomicInteger();
            ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2, r -> {
                Thread t = new Thread(r, "scheduling-1"); t.setDaemon(true); return t;
            });
            final boolean[] up = {true};

            // The health probe: AdaptiveHealthProbe.run() shape, rescheduled adaptively after each run.
            Runnable[] probe = new Runnable[1];
            probe[0] = () -> {
                probes.incrementAndGet();
                Mono.fromCallable(() -> { dsl.fetch(PROBE_1); dsl.fetch(PROBE_2); return "UP"; })
                    .subscribeOn(Schedulers.boundedElastic())
                    .timeout(probeTimeout, Mono.just("DOWN"))
                    .onErrorResume(e -> Mono.just("DOWN(threw " + e.getClass().getSimpleName() + ")"))
                    .subscribe(health -> {
                        boolean isUp = health.equals("UP");
                        if (!isUp) {
                            timeouts.incrementAndGet();
                        }
                        if (isUp != up[0]) {
                            System.out.println(ts() + " HEALTH " + (up[0] ? "UP" : "DOWN") + " -> " + health + "   " + poolState(pool));
                        }
                        up[0] = isUp;
                        scheduler.schedule(probe[0], (isUp ? passInterval : failureInterval).toMillis(), TimeUnit.MILLISECONDS);
                    });
            };
            scheduler.schedule(probe[0], 1, TimeUnit.SECONDS);

            // Background tool traffic.
            List<Thread> tools = new ArrayList<>();
            for (int i = 0; i < toolThreads; i++) {
                Thread t = new Thread(() -> {
                    while (!Thread.currentThread().isInterrupted()) {
                        try {
                            dsl.fetch("select drv_chunk_id chunk_id, COUNT(*) count from build where build_start_date = current_date");
                            toolQueries.incrementAndGet();
                        } catch (Exception e) {
                            toolFailures.incrementAndGet();
                        }
                        try { Thread.sleep(toolPauseMillis); } catch (InterruptedException e) { return; }
                    }
                }, "tool-" + i);
                t.setDaemon(true); t.start(); tools.add(t);
            }

            // Watchdog.
            Instant end = Instant.now().plus(runFor);
            Instant lastReport = Instant.now();
            boolean wedged = false;
            String wedgeDetail = "";
            while (Instant.now().isBefore(end)) {
                Thread.sleep(1_000);
                List<String> parked = eventLoopThreadsInParser();
                if (!parked.isEmpty() && !wedged) {
                    wedged = true;
                    System.out.println(ts() + " *** WEDGE DETECTED *** event-loop thread parked in the streaming parser:");
                    parked.forEach(l -> System.out.println("    " + l));
                    System.out.println("    " + poolState(pool) + "  (look for the 'Closing connection' DEBUG line just before this)");
                    wedgeDetail = ts() + " " + String.join("; ", parked) + " " + poolState(pool);
                }
                if (Duration.between(lastReport, Instant.now()).getSeconds() >= 60) {
                    lastReport = Instant.now();
                    System.out.println(ts() + " status: probes=" + probes + " timeouts=" + timeouts + " tool ok/fail=" + toolQueries + "/"
                        + toolFailures + " slow-queries=" + mock.slowQueriesInjected() + " slow-api=" + mock.slowApiCallsInjected()
                        + " stops=" + mock.stopQueryExecutionCount() + " " + poolState(pool));
                }
                if (wedged && Duration.between(lastReport, Instant.now()).getSeconds() >= 30) {
                    break;
                }
            }
            tools.forEach(Thread::interrupt);
            scheduler.shutdownNow();
            return new Result(wedged, wedgeDetail, probes.get(), timeouts.get(), toolQueries.get(), toolFailures.get(),
                mock.slowQueriesInjected(), mock.slowApiCallsInjected(), poolState(pool));
        }
    }

    // Incident-build pool: AthenaJdbcSupport as of before dv commit b47971c (2026-08-31).
    private static HikariDataSource incidentBuildPool(String mockBaseUrl, Duration maxLifetime) {
        Properties properties = new Properties();
        properties.put(ConnectionParameters.ATHENA_ENDPOINT_PARAMETER.name(), mockBaseUrl);
        properties.put(ConnectionParameters.ATHENA_STREAMING_ENDPOINT_PARAMETER.name(), mockBaseUrl);
        properties.put(ConnectionParameters.REGION_PARAMETER.name(), "eu-central-1");
        properties.put(ConnectionParameters.WORK_GROUP_PARAMETER.name(), "primary");
        properties.put(ConnectionParameters.CREDENTIALS_PROVIDER_PARAMETER.name(), "Static");
        properties.put(ConnectionParameters.USER_PARAMETER.name(), "dummy");
        properties.put(ConnectionParameters.PASSWORD_PARAMETER.name(), "dummy-secret");
        properties.put(ConnectionParameters.RESULT_FETCHER.name(), "GetQueryResultsStream");
        // ConnectionTest left at the driver default (true): one "select 1" per new connection.
        // NetworkTimeoutMillis deliberately NOT set: Hikari's setNetworkTimeout changes the value on
        // every validation and every close, and the driver nulls its SDK clients each time.

        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:athena://");
        config.setDriverClassName(AthenaDriver.class.getName());
        config.setDataSourceProperties(properties);
        config.setPoolName("mcp-server-athena-pool");
        config.setMaximumPoolSize(15);
        config.setConnectionTimeout(Duration.ofSeconds(3).toMillis());
        config.setValidationTimeout(Duration.ofSeconds(15).toMillis());
        config.setMaxLifetime(maxLifetime.toMillis());
        config.setConnectionInitSql("select 1");
        config.setRegisterMbeans(true);
        return new HikariDataSource(config);
    }

    private static DSLContext dsl(HikariDataSource pool) {
        DefaultConfiguration configuration = new DefaultConfiguration();
        configuration.set(SQLDialect.DEFAULT);
        configuration.set(pool);
        configuration.settings().setRenderQuotedNames(RenderQuotedNames.NEVER);
        return DSL.using(configuration);
    }

    private static String poolState(HikariDataSource pool) {
        HikariPoolMXBean mx = pool.getHikariPoolMXBean();
        return "Hikari total=" + mx.getTotalConnections() + " active=" + mx.getActiveConnections() + " idle=" + mx.getIdleConnections()
            + " waiting=" + mx.getThreadsAwaitingConnection();
    }

    private static List<String> eventLoopThreadsInParser() {
        List<String> parked = new ArrayList<>();
        for (Map.Entry<Thread, StackTraceElement[]> entry : Thread.getAllStackTraces().entrySet()) {
            if (!entry.getKey().getName().contains("NettyEventLoop")) {
                continue;
            }
            for (StackTraceElement frame : entry.getValue()) {
                if (frame.getClassName().endsWith("GetQueryResultsStreamResponseParser")) {
                    parked.add(entry.getKey().getName() + " " + entry.getKey().getState() + " at " + frame);
                    break;
                }
            }
        }
        return parked;
    }

    private static String ts() {
        return java.time.LocalTime.now().toString().substring(0, 12);
    }
}
