package com.example.athenachurn.streaming;

import software.amazon.awssdk.services.athena.model.GetQueryExecutionRequest;
import software.amazon.awssdk.services.athena.model.QueryExecution;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Reproduces the production onset sequence of the event-loop wedge: the same slow response is
 * run twice, once with the driver's completion executor alive (recovers) and once with the
 * executor shut down while the response is in flight (wedges forever). The two runs differ by
 * a single {@code shutdown()} call, which shows that a slow response is only the precondition;
 * the mid-flight close is what turns it into a permanent wedge.
 *
 * <p>Unlike {@link StreamingResponseStallRepro}, which injects {@code Runnable::run} as the
 * completion executor and therefore assumes the inline completion, this scenario reaches it the
 * way production does: through the AWS SDK's rejected-execution fallback. All wiring lives in
 * {@link DriverFaithfulAthenaSetup}; this file is only the repro itself.
 */
public final class MidFlightCloseRepro {

    private MidFlightCloseRepro() {
    }

    public static void main(String[] args) throws Exception {
        // The SDK logs its rejected-hop fallback message at debug level only. Surface it so the
        // run output carries the SDK's own statement of what happened.
        System.setProperty(
            "org.slf4j.simpleLogger.log.software.amazon.awssdk.core.internal.http.pipeline.stages.MakeAsyncHttpRequestStage",
            "debug"
        );
        try (DriverFaithfulAthenaSetup setup = DriverFaithfulAthenaSetup.start()) {
            slowResponseWithExecutorAliveRecovers(setup);
            slowResponseWithMidFlightCloseWedgesForever(setup);
        }
    }

    /**
     * The benign case: fuel without a spark. Athena is slow to return the first result byte,
     * and nothing else happens. Any caller-side timeout fires during the stall and reports the
     * call slow — an observation, not a wedge. When the bytes arrive, the SDK hops the future
     * completion onto the driver's {@code athena-jdbc} executor, the blocking parse runs there,
     * the Netty event loop stays free to deliver the body, and the call completes. A slow
     * response alone recovers, which is why slow installs do not wedge on every slow query.
     */
    private static void slowResponseWithExecutorAliveRecovers(DriverFaithfulAthenaSetup setup) throws Exception {
        System.out.println("=== Benign: slow response, executor alive ===");
        // Hold the streaming response before the first header byte: a query that takes a long
        // time to return anything. The SDK response future stays incomplete while the hold lasts.
        setup.mock().armStreamingHeaderHoldOnce();

        // The driver's statement blocks on the returned stage with a timeout-less get(); the
        // borrower thread mirrors it, and additionally records which thread ran the parse.
        CompletableFuture<String> parseThreadName = new CompletableFuture<>();
        Thread borrower = startBorrower(setup, "borrower-benign", parseThreadName);

        if (!setup.mock().awaitStreamingRequestArrived(5, TimeUnit.SECONDS)) {
            throw new IllegalStateException("The streaming request never reached the mock server");
        }
        // Stand-in for the tens of seconds Athena can take under load.
        Thread.sleep(2_000);
        setup.mock().releaseStreamingHeaders();

        // The completion hop must land on the driver's executor, not the event loop.
        String completionThread = parseThreadName.get(10, TimeUnit.SECONDS);
        System.out.println("Slow response completed. The blocking parse ran on: " + completionThread);
        if (!completionThread.startsWith("athena-jdbc-")) {
            throw new IllegalStateException(
                "Expected the completion hop onto the driver's executor, got: " + completionThread
            );
        }
        borrower.join(5_000);

        // The bystander client shares the event loop; its request completing proves the loop
        // survived the slow response.
        setup.athenaClient().getQueryExecution(
            GetQueryExecutionRequest.builder().queryExecutionId("mock-query-1").build()
        ).get(5, TimeUnit.SECONDS);
        System.out.println("A second request on the same event loop completes normally. RECOVERED.");
        System.out.println();
    }

    /**
     * The lethal case: the same fuel, plus the spark. While the slow response is still in
     * flight, the completion executor is shut down — exactly what
     * {@code ConnectionConfiguration.close()} does when the JDBC connection that issued the
     * request is closed with the request outstanding. When the response then arrives, the SDK's
     * hop onto the executor is rejected, the SDK logs its fallback line and completes the future
     * synchronously on the Netty event loop, and the driver's plain {@code thenApply(parse)}
     * runs its blocking {@code readLine} there. The body bytes the read waits for can only be
     * delivered by the same loop, so the loop is dead forever — and the SDK's timeout timers run
     * on that loop too, so no timeout can ever fire to break it. Every client sharing the loop
     * dies with it.
     */
    private static void slowResponseWithMidFlightCloseWedgesForever(DriverFaithfulAthenaSetup setup) throws Exception {
        System.out.println("=== Lethal: the same slow response, executor shut down mid-flight ===");
        setup.mock().armStreamingHeaderHoldOnce();

        Thread borrower = startBorrower(setup, "borrower-lethal", new CompletableFuture<>());

        if (!setup.mock().awaitStreamingRequestArrived(5, TimeUnit.SECONDS)) {
            throw new IllegalStateException("The streaming request never reached the mock server");
        }
        // The spark. This is the ONLY line that differs from the benign case.
        setup.completionExecutor().shutdown();
        Thread.sleep(200);
        setup.mock().releaseStreamingHeaders();

        // The wedge: the driver's parser frame on an event-loop thread.
        if (!setup.awaitParserOnNettyEventLoop(5_000)) {
            throw new IllegalStateException(
                "The blocking parse did not land on the Netty event loop - rejection fallback not reproduced"
            );
        }
        // The stack must contain the SDK's rejected-hop fallback frame. That frame only exists
        // when the executor hop failed, so the repro cannot pass through some other route.
        String parserStack = setup.parserStackOnNettyEventLoop();
        if (!parserStack.contains("lambda$executeHttpRequest$6")) {
            throw new IllegalStateException(
                "The event-loop parse did not go through the SDK's rejected-hop fallback "
                    + "(MakeAsyncHttpRequestStage.lambda$executeHttpRequest$6):\n" + parserStack
            );
        }
        System.out.println("The debug line above (\"Could not complete the service call future...\") is the");
        System.out.println("SDK's rejected-hop fallback, and the lambda$executeHttpRequest$6 frame below is its");
        System.out.println("synchronous completion path. The parse now blocks the sole Netty event loop:");
        System.out.print(parserStack);

        // Blast radius 1: the bystander client on the shared loop can no longer complete anything.
        try {
            setup.athenaClient().getQueryExecution(
                GetQueryExecutionRequest.builder().queryExecutionId("mock-query-1").build()
            ).get(3, TimeUnit.SECONDS);
            throw new IllegalStateException("A request on the wedged event loop unexpectedly completed");
        } catch (TimeoutException expected) {
            System.out.println("A second request on the same event loop never completes. WEDGED.");
        }

        // Blast radius 2: a real Hikari pool on the same loop shows the production symptom —
        // total=0 with no cause, until the JVM restarts.
        setup.assertHikariReportsTotalZero();

        if (borrower.isAlive()) {
            System.out.println("The original caller is still parked in a timeout-less get() - the same frame");
            System.out.println("the pool's connection-adder thread shows in a wedged production JVM.");
        }
        System.out.println();
        System.out.println("The benign and lethal runs differ by one call: completionExecutor.shutdown() while");
        System.out.println("the response was in flight. A slow response is the precondition; the mid-flight");
        System.out.println("close is what turns it into a permanent wedge.");
    }

    /**
     * Starts a thread that requests the streamed result and blocks in a timeout-less
     * {@code get()}, the way the driver's statement does. Completes {@code parseThreadName}
     * with the name of the thread the parse chain continued on (the benign case asserts on it;
     * the lethal case never completes it).
     */
    private static Thread startBorrower(
        DriverFaithfulAthenaSetup setup, String name, CompletableFuture<String> parseThreadName
    ) {
        Thread borrower = new Thread(() -> {
            try {
                setup.resultFactory()
                    .create(QueryExecution.builder().queryExecutionId("mock-query-1").build())
                    .thenApply(results -> Thread.currentThread().getName())
                    .toCompletableFuture()
                    .whenComplete((thread, failure) -> {
                        if (failure != null) {
                            parseThreadName.completeExceptionally(failure);
                        } else {
                            parseThreadName.complete(thread);
                        }
                    })
                    .get();
            } catch (Exception e) {
                parseThreadName.completeExceptionally(e);
            }
        }, name);
        borrower.setDaemon(true);
        borrower.start();
        return borrower;
    }
}
