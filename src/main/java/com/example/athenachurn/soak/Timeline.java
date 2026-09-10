package com.example.athenachurn.soak;

import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;

/** One line per notable event, timestamped. Printed as it happens, and kept for the result. */
public final class Timeline {

    private final List<String> lines = new ArrayList<>();

    public synchronized void note(String event) {
        String line = LocalTime.now().toString().substring(0, 12) + "  " + event;
        lines.add(line);
        System.out.println(line);
    }

    public synchronized List<String> lines() {
        return List.copyOf(lines);
    }
}
