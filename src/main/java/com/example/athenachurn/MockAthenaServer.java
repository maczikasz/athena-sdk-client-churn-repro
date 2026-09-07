package com.example.athenachurn;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsServer;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
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
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Local HTTPS implementation of the Athena calls needed to connect and fetch one streamed result,
 * or one buffered result page through the GetQueryResults API.
 */
final class MockAthenaServer implements AutoCloseable {

    private static final long HANG_MAX_MILLIS = 180_000;

    private final HttpsServer server;
    private final AtomicBoolean hangStreamingResponseOnce = new AtomicBoolean();
    private volatile CountDownLatch streamingHangStarted;
    private volatile CountDownLatch releaseStreamingResponse;
    private final AtomicBoolean hangGetQueryResultsOnce = new AtomicBoolean();
    private volatile CountDownLatch getQueryResultsHangStarted;
    private volatile CountDownLatch releaseGetQueryResults;
    private final List<String> requestedTargets = new CopyOnWriteArrayList<>();

    private MockAthenaServer(HttpsServer server) {
        this.server = server;
    }

    static MockAthenaServer startOnRandomPort() throws Exception {
        HttpsServer server = HttpsServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.setHttpsConfigurator(new HttpsConfigurator(generateSelfSignedContextAndInstallTrust()));
        MockAthenaServer mock = new MockAthenaServer(server);
        server.createContext("/", mock::handle);
        server.setExecutor(Executors.newCachedThreadPool(r -> {
            Thread thread = new Thread(r, "mock-athena");
            thread.setDaemon(true);
            return thread;
        }));
        server.start();
        return mock;
    }

    String baseUrl() {
        return "https://127.0.0.1:" + server.getAddress().getPort();
    }

    void armStreamingResponseHangOnce() {
        hangStreamingResponseOnce.set(true);
        streamingHangStarted = new CountDownLatch(1);
        releaseStreamingResponse = new CountDownLatch(1);
    }

    boolean awaitStreamingResponseHangStarted(long timeout, TimeUnit unit) throws InterruptedException {
        return streamingHangStarted.await(timeout, unit);
    }

    void releaseStreamingResponse() {
        releaseStreamingResponse.countDown();
    }

    /** Arms a one-time mid-body stall on the buffered GetQueryResults response. */
    void armGetQueryResultsHangOnce() {
        hangGetQueryResultsOnce.set(true);
        getQueryResultsHangStarted = new CountDownLatch(1);
        releaseGetQueryResults = new CountDownLatch(1);
    }

    boolean awaitGetQueryResultsHangStarted(long timeout, TimeUnit unit) throws InterruptedException {
        return getQueryResultsHangStarted.await(timeout, unit);
    }

    void releaseGetQueryResults() {
        releaseGetQueryResults.countDown();
    }

    /** Every X-Amz-Target header value seen so far, in request order. */
    List<String> requestedTargets() {
        return List.copyOf(requestedTargets);
    }

    @Override
    public void close() {
        server.stop(0);
    }

    private void handle(HttpExchange exchange) throws IOException {
        if ("GET".equals(exchange.getRequestMethod())) {
            requestedTargets.add("(streaming) GetQueryResultsStream");
            handleStreamingResults(exchange);
            return;
        }

        String target = exchange.getRequestHeaders().getFirst("X-Amz-Target");
        requestedTargets.add(target);
        if (target.endsWith("GetQueryResults")) {
            handleBufferedGetQueryResults(exchange);
            return;
        }

        String response;
        if (target.endsWith("StartQueryExecution")) {
            response = "{\"QueryExecutionId\":\"mock-query-1\"}";
        } else if (target.endsWith("GetQueryExecution")) {
            response = "{\"QueryExecution\":{\"QueryExecutionId\":\"mock-query-1\","
                + "\"Query\":\"SELECT 1\",\"StatementType\":\"DML\","
                + "\"ResultConfiguration\":{\"OutputLocation\":\"s3://mock-bucket/results/\"},"
                + "\"QueryExecutionContext\":{\"Database\":\"default\",\"Catalog\":\"AwsDataCatalog\"},"
                + "\"Status\":{\"State\":\"SUCCEEDED\",\"SubmissionDateTime\":1700000000.0,"
                + "\"CompletionDateTime\":1700000000.5},\"Statistics\":{\"EngineExecutionTimeInMillis\":10,"
                + "\"DataScannedInBytes\":0,\"TotalExecutionTimeInMillis\":10,\"QueryQueueTimeInMillis\":0,"
                + "\"QueryPlanningTimeInMillis\":1,\"ServiceProcessingTimeInMillis\":1},"
                + "\"WorkGroup\":\"primary\"}}";
        } else {
            throw new IOException("Unexpected Athena request target: " + target);
        }

        byte[] bytes = response.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/x-amz-json-1.1");
        exchange.sendResponseHeaders(200, bytes.length);
        try (OutputStream output = exchange.getResponseBody()) {
            output.write(bytes);
        }
    }

    /**
     * AmazonAthena.GetQueryResults: the buffered, non-streaming result page API. Response body is
     * one JSON document (not one-row-per-line like GetQueryResultsStream). To reproduce the same
     * "mid-body stall" shape as the streaming scenario, this can send the response in two chunked
     * writes with an artificial pause between them, holding the HTTP connection open with only
     * part of the JSON document delivered.
     */
    private void handleBufferedGetQueryResults(HttpExchange exchange) throws IOException {
        boolean hang = hangGetQueryResultsOnce.compareAndSet(true, false);

        String columnInfo = "{\"CatalogName\":\"hive\",\"SchemaName\":\"\",\"TableName\":\"\","
            + "\"Name\":\"one\",\"Label\":\"one\",\"Type\":\"integer\",\"Precision\":10,\"Scale\":0,"
            + "\"Nullable\":\"UNKNOWN\",\"CaseSensitive\":false}";
        String firstHalf = "{\"ResultSet\":{\"ResultSetMetadata\":{\"ColumnInfo\":[" + columnInfo + "]},"
            + "\"Rows\":[";
        String secondHalf = "{\"Data\":[{\"VarCharValue\":\"1\"}]}]},\"UpdateCount\":0}";

        exchange.getResponseHeaders().set("Content-Type", "application/x-amz-json-1.1");
        exchange.sendResponseHeaders(200, 0);
        try (OutputStream output = exchange.getResponseBody()) {
            output.write(firstHalf.getBytes(StandardCharsets.UTF_8));
            output.flush();
            if (hang) {
                getQueryResultsHangStarted.countDown();
                try {
                    releaseGetQueryResults.await(HANG_MAX_MILLIS, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            output.write(secondHalf.getBytes(StandardCharsets.UTF_8));
        }
    }

    private void handleStreamingResults(HttpExchange exchange) throws IOException {
        boolean hang = hangStreamingResponseOnce.compareAndSet(true, false);
        if (hang) {
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        String metadata = "{\"columnInfo\":[{\"catalogName\":\"hive\",\"schemaName\":\"\","
            + "\"tableName\":\"\",\"name\":\"one\",\"label\":\"one\",\"type\":\"integer\","
            + "\"precision\":10,\"scale\":0,\"nullable\":\"UNKNOWN\",\"caseSensitive\":false}]}\n";
        exchange.getResponseHeaders().set("Content-Type", "application/octet-stream");
        exchange.sendResponseHeaders(200, 0);
        try (OutputStream output = exchange.getResponseBody()) {
            output.write(metadata.getBytes(StandardCharsets.UTF_8));
            output.flush();
            if (hang) {
                streamingHangStarted.countDown();
                try {
                    releaseStreamingResponse.await(HANG_MAX_MILLIS, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            } else {
                output.write("{\"data\":[{\"varCharValue\":\"1\"}]}\n".getBytes(StandardCharsets.UTF_8));
            }
        }
    }

    private static SSLContext generateSelfSignedContextAndInstallTrust() throws Exception {
        Path directory = Files.createTempDirectory("athena-mock-tls");
        Path keystore = directory.resolve("keystore.jks");
        Path certificate = directory.resolve("mock.cer");
        Path truststore = directory.resolve("truststore.jks");
        String password = "changeit";
        String keytool = System.getProperty("java.home") + "/bin/keytool";

        run(keytool, "-genkeypair", "-alias", "mockathena", "-keyalg", "RSA", "-keysize", "2048",
            "-validity", "1", "-keystore", keystore.toString(), "-storepass", password, "-keypass", password,
            "-dname", "CN=127.0.0.1, OU=Test, O=Test, L=Test, ST=Test, C=US",
            "-ext", "SAN=IP:127.0.0.1,DNS:localhost");
        run(keytool, "-exportcert", "-alias", "mockathena", "-keystore", keystore.toString(),
            "-storepass", password, "-file", certificate.toString());
        run(keytool, "-importcert", "-noprompt", "-alias", "mockathena", "-keystore",
            truststore.toString(), "-storepass", password, "-file", certificate.toString());

        System.setProperty("javax.net.ssl.trustStore", truststore.toString());
        System.setProperty("javax.net.ssl.trustStorePassword", password);

        KeyStore keyStore = KeyStore.getInstance("JKS");
        try (FileInputStream input = new FileInputStream(keystore.toFile())) {
            keyStore.load(input, password.toCharArray());
        }
        KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance("SunX509");
        keyManagerFactory.init(keyStore, password.toCharArray());
        SSLContext context = SSLContext.getInstance("TLS");
        context.init(keyManagerFactory.getKeyManagers(), null, null);
        return context;
    }

    private static void run(String... command) throws IOException, InterruptedException {
        Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
        byte[] output = readAll(process.getInputStream());
        if (!process.waitFor(30, TimeUnit.SECONDS) || process.exitValue() != 0) {
            throw new IllegalStateException("keytool command failed: " + List.of(command) + "\n"
                + new String(output, StandardCharsets.UTF_8));
        }
    }

    private static byte[] readAll(java.io.InputStream input) throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        input.transferTo(output);
        return output.toByteArray();
    }
}
