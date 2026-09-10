package com.example.athenachurn.soak;

import org.jooq.DSLContext;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/** Background queries, like MCP tool calls: they keep the pool busy and its connections aging. */
public final class ToolTraffic {

    private static final String TOOL_QUERY =
        "select drv_chunk_id chunk_id, COUNT(*) count from build where build_start_date = current_date";

    private final List<Thread> threads = new ArrayList<>();
    private final AtomicInteger succeeded = new AtomicInteger();
    private final AtomicInteger failed = new AtomicInteger();

    public static ToolTraffic start(DSLContext dsl, int threads, Duration pause) {
        ToolTraffic traffic = new ToolTraffic();
        for (int i = 0; i < threads; i++) {
            Thread t = new Thread(() -> {
                while (!Thread.currentThread().isInterrupted()) {
                    try {
                        dsl.fetch(TOOL_QUERY);
                        traffic.succeeded.incrementAndGet();
                    } catch (Exception e) {
                        traffic.failed.incrementAndGet(); // mostly "Connection is not available" at the 3 s timeout
                    }
                    try {
                        Thread.sleep(pause.toMillis());
                    } catch (InterruptedException e) {
                        return;
                    }
                }
            }, "tool-" + i);
            t.setDaemon(true);
            t.start();
            traffic.threads.add(t);
        }
        return traffic;
    }

    public int succeeded() {
        return succeeded.get();
    }

    public int failed() {
        return failed.get();
    }

    public String stats() {
        return "tool ok/fail=" + succeeded + "/" + failed;
    }

    public void stop() {
        threads.forEach(Thread::interrupt);
    }
}
