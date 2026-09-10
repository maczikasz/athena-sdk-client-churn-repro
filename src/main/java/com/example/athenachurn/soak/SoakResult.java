package com.example.athenachurn.soak;

/** What one soak run observed. {@code wedged} is what the test asserts on. */
public record SoakResult(
    boolean wedged,
    String wedgeDetail,
    int probeRuns,
    int probeTimeouts,
    int toolQueriesSucceeded,
    int toolQueriesFailed,
    int slowQueriesInjected,
    int slowApiCallsInjected,
    String poolState,
    Timeline timeline
) {
    public String summary() {
        return "probes=" + probeRuns + " timeouts=" + probeTimeouts + " tool ok/fail=" + toolQueriesSucceeded + "/" + toolQueriesFailed
            + " slow-queries=" + slowQueriesInjected + " slow-api=" + slowApiCallsInjected + " wedged=" + wedged + " " + poolState;
    }
}
