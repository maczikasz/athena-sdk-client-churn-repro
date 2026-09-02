package com.example.athenachurn;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsServer;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A local, offline stand-in for the Athena and S3 HTTPS endpoints, just complete enough for the
 * driver's own connection test ({@code ConnectionTest=true}) to succeed: it implements the AWS
 * JSON 1.1 targets {@code StartQueryExecution}, {@code GetQueryExecution} (always answers
 * SUCCEEDED — this mock does not model a real polling loop), and {@code GetQueryResults}
 * (answers one row with the value {@code "1"}), selected via the driver's own
 * {@code ResultFetcher=GetQueryResults} connection property so it never needs to fetch a result
 * file from S3 in Athena's proprietary binary metadata format.
 *
 * <p>The driver requires an HTTPS endpoint (it rejects plain HTTP with "is not an HTTPS
 * endpoint"), so this class generates a throwaway self-signed certificate at startup by
 * shelling out to the {@code keytool} that ships with the running JDK, and installs a matching
 * client-side trust store as the JVM's default trust store. Nothing is committed to the
 * repository: the certificate is regenerated into a temp directory on every run.
 *
 * <p>Two knobs make this useful for the timeout/recovery scenarios: {@link #setLatencyMillis}
 * adds a sleep before every response, and {@link #setFailing} makes every response a 500.
 */
final class MockAthenaServer implements AutoCloseable {

    private final HttpsServer server;
    private final AtomicLong latencyMillis = new AtomicLong(0);
    private final AtomicBooleanLike failing = new AtomicBooleanLike(false);
    private final Map<String, AtomicInteger> callCounts = new ConcurrentHashMap<>();
    private volatile boolean verbose = false;
    /** When non-null, only this AWS access key ID (from the SigV4 Authorization header's
     * Credential= component) is accepted; every other key gets a 403 UnrecognizedClientException,
     * exactly like a rotated or disabled real AWS credential would. */
    private volatile String acceptedAccessKeyId = null;
    /** One-shot: the next request whose X-Amz-Target ends with this string never gets a
     * response at all (no headers, nothing) - the TCP connection is just held open. Consumed
     * atomically so exactly one matching request hangs. */
    private final java.util.concurrent.atomic.AtomicReference<String> hangOnceTarget =
            new java.util.concurrent.atomic.AtomicReference<>();
    private static final long HANG_MAX_MILLIS = 180_000;

    private MockAthenaServer(HttpsServer server) {
        this.server = server;
    }

    static MockAthenaServer startOnRandomPort() throws Exception {
        SSLContext sslContext = generateSelfSignedContextAndInstallTrust();

        HttpsServer server = HttpsServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.setHttpsConfigurator(new HttpsConfigurator(sslContext));
        MockAthenaServer mock = new MockAthenaServer(server);
        server.createContext("/", mock::handle);
        server.setExecutor(Executors.newCachedThreadPool(MockAthenaServer::newDaemonThread));
        server.start();
        return mock;
    }

    int port() {
        return server.getAddress().getPort();
    }

    String baseUrl() {
        return "https://127.0.0.1:" + port();
    }

    void setLatencyMillis(long millis) {
        latencyMillis.set(millis);
    }

    void setFailing(boolean value) {
        failing.set(value);
    }

    void setVerbose(boolean value) {
        this.verbose = value;
    }

    /** Restricts accepted requests to this access key ID, or pass {@code null} to accept any. */
    void setAcceptedAccessKeyId(String accessKeyId) {
        this.acceptedAccessKeyId = accessKeyId;
    }

    /**
     * Arms a one-shot hang: the next request whose {@code X-Amz-Target} ends with
     * {@code targetSuffix} gets no response at all - not even headers - and the handler thread
     * just parks, holding the TCP connection open, for up to {@link #HANG_MAX_MILLIS}. Exactly
     * one request is hung; every other request (before or after) is answered normally. This is
     * deliberately different from high latency: latency still completes and answers, a hang
     * never sends a single response byte, so it exercises the client's own dead-peer detection
     * (or the total absence of it) instead of a slow-but-working endpoint.
     */
    void armHangOnce(String targetSuffix) {
        hangOnceTarget.set(targetSuffix);
    }

    /** Extracts the access key ID from a SigV4 {@code Authorization} header, or null if absent. */
    private static String accessKeyIdFrom(String authorizationHeader) {
        if (authorizationHeader == null) {
            return null;
        }
        int credentialIdx = authorizationHeader.indexOf("Credential=");
        if (credentialIdx < 0) {
            return null;
        }
        String afterCredential = authorizationHeader.substring(credentialIdx + "Credential=".length());
        int slashIdx = afterCredential.indexOf('/');
        return slashIdx < 0 ? afterCredential : afterCredential.substring(0, slashIdx);
    }

    int callCount(String target) {
        AtomicInteger counter = callCounts.get(target);
        return counter == null ? 0 : counter.get();
    }

    Map<String, Integer> callCountsSnapshot() {
        Map<String, Integer> snapshot = new ConcurrentHashMap<>();
        callCounts.forEach((k, v) -> snapshot.put(k, v.get()));
        return snapshot;
    }

    void resetCallCounts() {
        callCounts.clear();
    }

    @Override
    public void close() {
        server.stop(0);
    }

    private void handle(HttpExchange exchange) throws IOException {
        String target = exchange.getRequestHeaders().getFirst("X-Amz-Target");
        byte[] bodyBytes = readAll(exchange.getRequestBody());
        String body = new String(bodyBytes, StandardCharsets.UTF_8);
        String key = target == null ? "S3:" + exchange.getRequestURI() : target;
        callCounts.computeIfAbsent(key, k -> new AtomicInteger()).incrementAndGet();

        String armedHang = hangOnceTarget.get();
        if (target != null && armedHang != null && target.endsWith(armedHang)
                && hangOnceTarget.compareAndSet(armedHang, null)) {
            if (verbose) {
                System.out.println("[mock] HANGING on " + key + " (no response will ever be sent)");
            }
            try {
                Thread.sleep(HANG_MAX_MILLIS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            // Deliberately return without ever calling sendResponseHeaders: the client gets
            // nothing, ever, from this request - not even a connection-level error - until its
            // own client-side timeout (if any) gives up on it.
            return;
        }

        long delay = latencyMillis.get();
        if (delay > 0) {
            try {
                Thread.sleep(delay);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        if (verbose) {
            System.out.println("[mock] " + key + " body=" + body);
        }

        String requestedKey = accessKeyIdFrom(exchange.getRequestHeaders().getFirst("Authorization"));
        String pinnedKey = acceptedAccessKeyId;

        String response;
        int status = 200;
        if (failing.get()) {
            status = 500;
            response = "{\"__type\":\"InternalFailure\",\"message\":\"mock endpoint is in failure mode\"}";
        } else if (pinnedKey != null && !pinnedKey.equals(requestedKey)) {
            status = 403;
            response = "{\"__type\":\"UnrecognizedClientException\","
                    + "\"message\":\"The security token included in the request is invalid.\"}";
        } else if (target == null) {
            status = 404;
            response = "{}";
        } else if (target.endsWith("StartQueryExecution")) {
            response = "{\"QueryExecutionId\":\"mock-query-1\"}";
        } else if (target.endsWith("GetQueryExecution")) {
            response = "{\"QueryExecution\":{"
                    + "\"QueryExecutionId\":\"mock-query-1\","
                    + "\"Query\":\"SELECT 1\","
                    + "\"StatementType\":\"DML\","
                    + "\"ResultConfiguration\":{\"OutputLocation\":\"s3://mock-bucket/results/\"},"
                    + "\"QueryExecutionContext\":{\"Database\":\"default\",\"Catalog\":\"AwsDataCatalog\"},"
                    + "\"Status\":{\"State\":\"SUCCEEDED\","
                    + "\"SubmissionDateTime\":1700000000.0,\"CompletionDateTime\":1700000000.5},"
                    + "\"Statistics\":{\"EngineExecutionTimeInMillis\":10,\"DataScannedInBytes\":0,"
                    + "\"TotalExecutionTimeInMillis\":10,\"QueryQueueTimeInMillis\":0,\"QueryPlanningTimeInMillis\":1,"
                    + "\"ServiceProcessingTimeInMillis\":1},"
                    + "\"WorkGroup\":\"primary\"}}";
        } else if (target.endsWith("GetQueryResults")) {
            response = "{\"UpdateCount\":0,\"ResultSet\":{"
                    + "\"Rows\":[{\"Data\":[{\"VarCharValue\":\"_col0\"}]},{\"Data\":[{\"VarCharValue\":\"1\"}]}],"
                    + "\"ResultSetMetadata\":{\"ColumnInfo\":[{\"CatalogName\":\"hive\",\"SchemaName\":\"\","
                    + "\"TableName\":\"\",\"Name\":\"_col0\",\"Label\":\"_col0\",\"Type\":\"integer\","
                    + "\"Precision\":10,\"Scale\":0,\"Nullable\":\"UNKNOWN\",\"CaseSensitive\":false}]}}}";
        } else {
            response = "{}";
        }

        byte[] respBytes = response.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/x-amz-json-1.1");
        exchange.sendResponseHeaders(status, respBytes.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(respBytes);
        }
    }

    private static byte[] readAll(java.io.InputStream in) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte[] buffer = new byte[4096];
        int read;
        while ((read = in.read(buffer)) != -1) {
            out.write(buffer, 0, read);
        }
        return out.toByteArray();
    }

    /**
     * Generates a throwaway self-signed certificate with {@code keytool}, builds an
     * {@link SSLContext} from it for the server side, and installs a matching trust store as
     * the JVM's default ({@code javax.net.ssl.trustStore} system properties) so the driver's
     * own HTTPS client trusts it.
     */
    private static SSLContext generateSelfSignedContextAndInstallTrust() throws Exception {
        Path dir = Files.createTempDirectory("athena-mock-tls");
        Path keystore = dir.resolve("keystore.jks");
        Path cert = dir.resolve("mock.cer");
        Path truststore = dir.resolve("truststore.jks");
        String password = "changeit";
        String keytool = System.getProperty("java.home") + "/bin/keytool";

        run(keytool, "-genkeypair", "-alias", "mockathena", "-keyalg", "RSA", "-keysize", "2048",
                "-validity", "1", "-keystore", keystore.toString(), "-storepass", password,
                "-keypass", password, "-dname", "CN=127.0.0.1, OU=Test, O=Test, L=Test, ST=Test, C=US",
                "-ext", "SAN=IP:127.0.0.1,DNS:localhost");
        run(keytool, "-exportcert", "-alias", "mockathena", "-keystore", keystore.toString(),
                "-storepass", password, "-file", cert.toString());
        run(keytool, "-importcert", "-noprompt", "-alias", "mockathena", "-keystore",
                truststore.toString(), "-storepass", password, "-file", cert.toString());

        System.setProperty("javax.net.ssl.trustStore", truststore.toString());
        System.setProperty("javax.net.ssl.trustStorePassword", password);

        char[] passwordChars = password.toCharArray();
        KeyStore ks = KeyStore.getInstance("JKS");
        try (FileInputStream fis = new FileInputStream(keystore.toFile())) {
            ks.load(fis, passwordChars);
        }
        KeyManagerFactory kmf = KeyManagerFactory.getInstance("SunX509");
        kmf.init(ks, passwordChars);
        SSLContext sslContext = SSLContext.getInstance("TLS");
        sslContext.init(kmf.getKeyManagers(), null, null);
        return sslContext;
    }

    private static Thread newDaemonThread(Runnable r) {
        Thread t = new Thread(r);
        t.setDaemon(true);
        return t;
    }

    private static void run(String... command) throws IOException, InterruptedException {
        Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
        byte[] output = readAll(process.getInputStream());
        boolean finished = process.waitFor(30, TimeUnit.SECONDS);
        if (!finished || process.exitValue() != 0) {
            throw new IllegalStateException(
                    "keytool command failed: " + List.of(command) + "\n" + new String(output, StandardCharsets.UTF_8));
        }
    }

    /** A tiny volatile boolean box, since {@code java.util.concurrent.atomic} has no such type. */
    private static final class AtomicBooleanLike {
        private volatile boolean value;

        AtomicBooleanLike(boolean initial) {
            this.value = initial;
        }

        void set(boolean v) {
            this.value = v;
        }

        boolean get() {
            return value;
        }
    }
}
