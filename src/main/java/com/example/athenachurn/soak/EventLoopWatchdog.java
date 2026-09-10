package com.example.athenachurn.soak;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Detects the wedge: a Netty event-loop thread parked inside the driver's streaming result parser.
 * That frame - {@code GetQueryResultsStreamResponseParser.parse} - is the one in the production
 * thread dumps.
 */
public final class EventLoopWatchdog {

    private boolean wedged;
    private String detail = "";

    public void check(Timeline timeline, Supplier<String> poolState) {
        if (wedged) {
            return;
        }
        List<String> parked = parkedInStreamingParser();
        if (!parked.isEmpty()) {
            wedged = true;
            detail = String.join("; ", parked) + "  " + poolState.get();
            timeline.note("*** WEDGE *** " + detail);
        }
    }

    public boolean wedged() {
        return wedged;
    }

    public String detail() {
        return detail;
    }

    /** Netty event-loop threads whose stack contains the driver's streaming parser, with their top frame there. */
    public static List<String> parkedInStreamingParser() {
        List<String> parked = new ArrayList<>();
        for (Map.Entry<Thread, StackTraceElement[]> entry : Thread.getAllStackTraces().entrySet()) {
            Thread thread = entry.getKey();
            if (!thread.getName().contains("NettyEventLoop")) {
                continue;
            }
            for (StackTraceElement frame : entry.getValue()) {
                if (frame.getClassName().endsWith("GetQueryResultsStreamResponseParser")) {
                    parked.add(thread.getName() + " " + thread.getState() + " at " + frame);
                    break;
                }
            }
        }
        return parked;
    }
}
