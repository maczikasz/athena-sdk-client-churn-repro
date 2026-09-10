package com.example.athenachurn.soak;

import org.jooq.DSLContext;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/**
 * The mcp-server Athena connectivity probe, same shape as {@code AdaptiveHealthProbe}: a blocking
 * jOOQ fetch on {@code boundedElastic}, wrapped in {@code Mono.timeout} with a DOWN fallback, and
 * rescheduled after each run - a long interval while UP, a short one while DOWN.
 *
 * <p>The timeout is the first ingredient of the wedge. When it fires, Reactor cancels the upstream,
 * which interrupts the worker thread. The driver rethrows that as a SQLException without SQLState,
 * so HikariCP does not evict the connection; and the statement's poll -> fetch chain keeps running
 * with nobody waiting for it.
 */
public final class HealthProbe {

    static final String PROBE_1 = "select 1 one from (values(1))";
    static final String PROBE_2 = "SELECT id FROM build WHERE build_start_date = current_date - interval '1' day"
        + " AND build_start_time = '000000' LIMIT 1";

    private final DSLContext dsl;
    private final SoakSettings settings;
    private final Timeline timeline;
    private final Supplier<String> poolState;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "scheduling-1");
        t.setDaemon(true);
        return t;
    });
    private final AtomicInteger runs = new AtomicInteger();
    private final AtomicInteger timeouts = new AtomicInteger();
    private volatile boolean up = true;

    private HealthProbe(DSLContext dsl, SoakSettings settings, Timeline timeline, Supplier<String> poolState) {
        this.dsl = dsl;
        this.settings = settings;
        this.timeline = timeline;
        this.poolState = poolState;
    }

    public static HealthProbe start(DSLContext dsl, SoakSettings settings, Timeline timeline, Supplier<String> poolState) {
        HealthProbe probe = new HealthProbe(dsl, settings, timeline, poolState);
        probe.scheduler.schedule(probe::run, 1, TimeUnit.SECONDS);
        return probe;
    }

    private void run() {
        int n = runs.incrementAndGet();
        Mono.fromCallable(() -> {
                dsl.fetch(PROBE_1);
                dsl.fetch(PROBE_2);
                return "UP";
            })
            .subscribeOn(Schedulers.boundedElastic())
            .timeout(settings.probeTimeout(), Mono.just("DOWN (probe exceeded " + settings.probeTimeout() + ")"))
            .onErrorResume(e -> Mono.just("DOWN (" + e.getClass().getSimpleName() + ")"))
            .subscribe(health -> {
                boolean isUp = health.equals("UP");
                if (!isUp) {
                    timeouts.incrementAndGet();
                    timeline.note("probe #" + n + " " + health + "   " + poolState.get());
                }
                if (isUp && !up) {
                    timeline.note("probe #" + n + " UP again");
                }
                up = isUp;
                if (!scheduler.isShutdown()) {
                    scheduler.schedule(this::run, (isUp ? settings.probePassInterval() : settings.probeFailureInterval()).toMillis(),
                        TimeUnit.MILLISECONDS);
                }
            });
    }

    public int runs() {
        return runs.get();
    }

    public int timeouts() {
        return timeouts.get();
    }

    public String stats() {
        return "probes=" + runs + " timeouts=" + timeouts;
    }

    public void stop() {
        scheduler.shutdownNow();
    }
}
