package com.example.athenachurn.streaming;

import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * The health-probe soak as a test that FAILS when the shared Netty event loop wedges. Run it in
 * IntelliJ with "Repeat: Until Failure", or via Gradle: {@code ./gradlew test --tests '*SoakTest*'}
 * ({@code soakUntilWedge} repeats up to 20 times and stops at the first failure).
 *
 * <p>On failure the message carries the HikariCP close lines and the driver lines around the wedge,
 * taken from the SLF4J simple-logger file this class routes logging to, so the trigger can be read
 * directly from the assertion. Settings come from {@code soak.*} system properties (see
 * {@link HealthCheckSoakRepro}); the defaults here are the 2-minute configuration that wedged in
 * 2 of 4 runs. In IntelliJ add {@code -XX:ActiveProcessorCount=1} to the run configuration's VM
 * options to get the production pod's two-loop SDK group (the Gradle test task sets it).
 */
class HealthCheckSoakTest {

    private static final Path LOG;

    static {
        // Must run before the first LoggerFactory.getLogger call in this JVM: slf4j-simple reads
        // its configuration once. Hikari at DEBUG is what names the close.
        try {
            LOG = Files.createTempFile("athena-soak-", ".log");
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        System.setProperty("org.slf4j.simpleLogger.logFile", LOG.toString());
        System.setProperty("org.slf4j.simpleLogger.log.com.zaxxer.hikari", "debug");
        System.setProperty("org.slf4j.simpleLogger.showDateTime", "true");
        System.setProperty("org.slf4j.simpleLogger.dateTimeFormat", "HH:mm:ss.SSS");
        System.setProperty("soak.passIntervalSeconds", System.getProperty("soak.passIntervalSeconds", "8"));
        System.setProperty("soak.maxLifetimeSeconds", System.getProperty("soak.maxLifetimeSeconds", "40"));
    }

    @Test
    void healthProbeSoakDoesNotWedgeTheEventLoop() throws Exception {
        runOnce();
    }

    /** Gradle-side equivalent of IntelliJ's "Repeat: Until Failure": stops at the first wedge. */
    @RepeatedTest(20)
    void soakUntilWedge() throws Exception {
        runOnce();
    }

    private static void runOnce() throws Exception {
        long logStart = Files.exists(LOG) ? Files.size(LOG) : 0;
        Duration runFor = Duration.ofSeconds(Long.getLong("soak.seconds", 120));
        HealthCheckSoakRepro.Result result = HealthCheckSoakRepro.run(runFor);
        System.out.println("soak result: " + result.summary());
        assertFalse(result.wedged(), () -> "Event loop wedged. " + result.summary() + "\n" + result.wedgeDetail()
            + "\n\nHikariCP closes and probe timeouts before the wedge (from " + LOG + "):\n" + relevantLog(logStart));
    }

    private static String relevantLog(long from) {
        try {
            List<String> lines = Files.readAllLines(LOG);
            return lines.stream()
                .filter(l -> l.contains("Closing connection") || l.contains("onErrorDropped") || l.contains("NettyEventLoop")
                    || l.contains("Connection is not available") || l.contains("has state SUCCEEDED"))
                .skip(Math.max(0, lines.size() - 400))
                .map(l -> l.length() > 220 ? l.substring(0, 220) : l)
                .collect(Collectors.joining("\n"));
        } catch (IOException e) {
            return "(could not read " + LOG + ": " + e + ")";
        }
    }
}
