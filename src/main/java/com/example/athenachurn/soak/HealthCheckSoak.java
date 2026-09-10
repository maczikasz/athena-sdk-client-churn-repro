package com.example.athenachurn.soak;

import com.example.athenachurn.mock.MockAthenaServer;

import java.time.Duration;
import java.time.Instant;

/**
 * The scenario. Everything else in this package is wiring; this class is the story.
 *
 * <p>An application process, on the pool configuration it shipped with before our fix,
 * runs its Athena health probe every few seconds and serves tool queries in the background,
 * against an Athena that is sometimes slow. Nothing is forced. The question is whether the shared
 * Netty event loop wedges, and if it does, which HikariCP close preceded it.
 */
public final class HealthCheckSoak {

    private HealthCheckSoak() {
    }

    public static SoakResult run(SoakSettings settings) throws Exception {
        Timeline timeline = new Timeline();
        try (MockAthenaServer athena = MockAthenaServer.startOnRandomPort();
             AffectedAthenaPool pool = AffectedAthenaPool.start(athena.baseUrl(), settings.maxLifetime())) {

            athena.enableSoakMode(settings.slowQueryProbability(), settings.slowQueryMin().toMillis(),
                settings.slowQueryMax().toMillis(), settings.slowApiCallProbability(), settings.slowApiCall().toMillis());
            pool.warmUp();
            timeline.note("pool warm, " + pool.state());

            HealthProbe probe = HealthProbe.start(pool.dsl(), settings, timeline, pool::state);
            ToolTraffic tools = ToolTraffic.start(pool.dsl(), settings.toolThreads(), settings.toolPause());
            EventLoopWatchdog watchdog = new EventLoopWatchdog();

            Instant deadline = Instant.now().plus(settings.runFor());
            Instant lastStatus = Instant.now();
            while (Instant.now().isBefore(deadline) && !watchdog.wedged()) {
                Thread.sleep(500);
                watchdog.check(timeline, pool::state);
                if (Duration.between(lastStatus, Instant.now()).getSeconds() >= 60) {
                    lastStatus = Instant.now();
                    timeline.note("status: " + probe.stats() + " " + tools.stats() + " " + athena.slowQueriesInjected()
                        + " slow queries injected, " + pool.state());
                }
            }

            tools.stop();
            probe.stop();
            return new SoakResult(watchdog.wedged(), watchdog.detail(), probe.runs(), probe.timeouts(), tools.succeeded(),
                tools.failed(), athena.slowQueriesInjected(), athena.slowApiCallsInjected(), pool.state(), timeline);
        }
    }
}
