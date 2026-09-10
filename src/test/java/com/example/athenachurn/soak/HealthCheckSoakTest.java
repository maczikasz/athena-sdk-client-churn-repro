package com.example.athenachurn.soak;

import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * {@link HealthCheckSoak} as a test that FAILS when the event loop wedges. Run it in IntelliJ with
 * "Repeat: Until Failure" (add {@code -XX:ActiveProcessorCount=1} to the VM options), or via Gradle:
 * {@code ./gradlew test --tests '*SoakTest*'} - {@code soakUntilWedge} repeats up to 20 times and
 * {@code failFast} stops at the first wedge. The failure message carries the HikariCP close lines
 * that preceded the wedge.
 */
class HealthCheckSoakTest {

    static {
        SoakLog.install();
    }

    @Test
    void healthProbeSoakDoesNotWedgeTheEventLoop() throws Exception {
        assertNoWedge();
    }

    @RepeatedTest(20)
    void soakUntilWedge() throws Exception {
        assertNoWedge();
    }

    private static void assertNoWedge() throws Exception {
        SoakResult result = HealthCheckSoak.run(SoakSettings.fromSystemProperties());
        System.out.println("soak result: " + result.summary());
        assertFalse(result.wedged(), () -> "Event loop wedged. " + result.summary()
            + "\n\nTimeline:\n" + String.join("\n", result.timeline().lines())
            + "\n\nEvidence from " + SoakLog.file() + ":\n" + SoakLog.evidence());
    }
}
