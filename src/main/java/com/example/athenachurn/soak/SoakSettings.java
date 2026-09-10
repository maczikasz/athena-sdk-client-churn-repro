package com.example.athenachurn.soak;

import java.time.Duration;

/**
 * Knobs, all overridable with {@code -Dsoak.<name>=<value>} (Gradle: {@code -Psoak.<name>=<value>}).
 * Production values are in the comments; the defaults are scaled so a run takes minutes.
 */
public record SoakSettings(
    Duration runFor,
    Duration probeTimeout,          // production PT30S
    Duration probePassInterval,     // production PT5M
    Duration probeFailureInterval,  // production PT30S
    Duration maxLifetime,           // affected build: 30 min
    double slowQueryProbability,
    Duration slowQueryMin,
    Duration slowQueryMax,
    double slowApiCallProbability,
    Duration slowApiCall,
    int toolThreads,
    Duration toolPause
) {
    public static SoakSettings fromSystemProperties() {
        return new SoakSettings(
            seconds("soak.seconds", 120),
            seconds("soak.probeTimeoutSeconds", 10),
            seconds("soak.passIntervalSeconds", 8),
            seconds("soak.failureIntervalSeconds", 5),
            seconds("soak.maxLifetimeSeconds", 40),
            Double.parseDouble(System.getProperty("soak.slowQueryProbability", "0.25")),
            Duration.ofMillis(Long.getLong("soak.slowQueryMinMillis", 2_000)),
            Duration.ofMillis(Long.getLong("soak.slowQueryMaxMillis", 25_000)),
            Double.parseDouble(System.getProperty("soak.slowApiCallProbability", "0.02")),
            Duration.ofMillis(Long.getLong("soak.slowApiCallMillis", 4_000)),
            Integer.getInteger("soak.toolThreads", 3),
            Duration.ofMillis(Long.getLong("soak.toolPauseMillis", 1_500))
        );
    }

    private static Duration seconds(String property, long defaultValue) {
        return Duration.ofSeconds(Long.getLong(property, defaultValue));
    }

    @Override
    public String toString() {
        return "run " + runFor + ", probe timeout " + probeTimeout + ", cadence " + probePassInterval + " up / "
            + probeFailureInterval + " down, maxLifetime " + maxLifetime + ", slow query p=" + slowQueryProbability + " ["
            + slowQueryMin.toMillis() + ".." + slowQueryMax.toMillis() + " ms], slow API p=" + slowApiCallProbability + " ("
            + slowApiCall.toMillis() + " ms), " + toolThreads + " tool threads";
    }
}
