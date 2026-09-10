package com.example.athenachurn.soak;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Routes SLF4J (slf4j-simple) to a file with HikariCP at DEBUG, so the console shows only the
 * {@link Timeline} and the file holds the evidence: every {@code Closing connection ...: (reason)}.
 * {@link #install()} must run before the first logger is created in the JVM.
 */
public final class SoakLog {

    private static Path file;

    private SoakLog() {
    }

    public static synchronized Path install() {
        if (file != null) {
            return file;
        }
        try {
            file = Files.createTempFile("athena-soak-", ".log");
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        System.setProperty("org.slf4j.simpleLogger.logFile", file.toString());
        System.setProperty("org.slf4j.simpleLogger.log.com.zaxxer.hikari", "debug");
        System.setProperty("org.slf4j.simpleLogger.showDateTime", "true");
        System.setProperty("org.slf4j.simpleLogger.dateTimeFormat", "HH:mm:ss.SSS");
        return file;
    }

    public static Path file() {
        return file;
    }

    /**
     * The lines that tell the story around a wedge: HikariCP closes with their reason, the probe's
     * interrupt ({@code onErrorDropped}), and anything the SDK did on an event-loop thread.
     */
    public static String evidence() {
        if (file == null) {
            return "(SoakLog not installed)";
        }
        try {
            List<String> lines = Files.readAllLines(file);
            return lines.stream()
                .filter(l -> l.contains("Closing connection") || l.contains("onErrorDropped") || l.contains("NettyEventLoop"))
                .skip(Math.max(0, lines.size() - 400))
                .map(l -> l.replaceAll(" (DEBUG|INFO|WARN|ERROR) [a-zA-Z.]+ - ", " ").replace("athena-pool - ", ""))
                .map(l -> l.length() > 200 ? l.substring(0, 200) : l)
                .collect(Collectors.joining("\n"));
        } catch (IOException e) {
            return "(could not read " + file + ": " + e + ")";
        }
    }
}
