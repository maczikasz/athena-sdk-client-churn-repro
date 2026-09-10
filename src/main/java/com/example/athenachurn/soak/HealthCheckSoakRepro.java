package com.example.athenachurn.soak;

/** Command-line entry point: {@code ./gradlew runSoak [-Psoak.<name>=<value> ...]}. */
public final class HealthCheckSoakRepro {

    private HealthCheckSoakRepro() {
    }

    public static void main(String[] args) throws Exception {
        SoakLog.install();
        SoakSettings settings = SoakSettings.fromSystemProperties();
        System.out.println("Soak: " + settings);
        System.out.println("SDK event loops: expect 2 with -XX:ActiveProcessorCount=1 (cores=" + Runtime.getRuntime().availableProcessors()
            + "). Full log with HikariCP DEBUG: " + SoakLog.file());

        SoakResult result = HealthCheckSoak.run(settings);

        System.out.println();
        System.out.println("SUMMARY " + result.summary());
        if (result.wedged()) {
            System.out.println();
            System.out.println("Evidence (HikariCP closes, the probe interrupt, event-loop activity):");
            System.out.println(SoakLog.evidence());
        }
    }
}
