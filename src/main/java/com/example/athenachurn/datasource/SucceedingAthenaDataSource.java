package com.example.athenachurn.datasource;

import software.amazon.awssdk.services.athena.AthenaAsyncClient;
import software.amazon.awssdk.services.athena.model.StartQueryExecutionRequest;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLNonTransientConnectionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

/**
 * A {@link DataSource} that issues one real Athena request per {@link #getConnection()} call and,
 * once it completes, hands Hikari a minimal working {@link Connection} (a JDK dynamic proxy that
 * answers {@code isValid}/{@code isClosed} sensibly and no-ops everything else). Unlike
 * {@link BlockingAthenaDataSource} (which always fails, because in the streaming-stall scenario
 * the Athena request must never complete), this class is for workaround scenarios where the
 * request DOES complete and the point is to show Hikari can actually add the connection to the
 * pool instead of timing out at {@code total=0}.
 */
public final class SucceedingAthenaDataSource implements DataSource {

    private final AthenaAsyncClient athenaClient;
    private final CountDownLatch connectionAttemptStarted = new CountDownLatch(1);

    public SucceedingAthenaDataSource(AthenaAsyncClient athenaClient) {
        this.athenaClient = athenaClient;
    }

    public boolean awaitConnectionAttempt(long timeout, TimeUnit unit) throws InterruptedException {
        return connectionAttemptStarted.await(timeout, unit);
    }

    @Override
    public Connection getConnection() throws SQLException {
        connectionAttemptStarted.countDown();
        try {
            athenaClient.startQueryExecution(
                StartQueryExecutionRequest.builder().queryString("SELECT 1").workGroup("primary").build()
            ).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new SQLNonTransientConnectionException("Interrupted while connecting to Athena", e);
        } catch (ExecutionException e) {
            throw new SQLNonTransientConnectionException("Athena connection attempt failed", e.getCause());
        }
        return (Connection) Proxy.newProxyInstance(
            Connection.class.getClassLoader(),
            new Class<?>[] {Connection.class},
            (proxy, method, args) -> {
                switch (method.getName()) {
                    case "isValid":
                        return true;
                    case "isClosed":
                        return false;
                    case "close":
                        return null;
                    case "isWrapperFor":
                        return false;
                    case "unwrap":
                        throw new SQLException("Not a wrapper");
                    case "toString":
                        return "SucceedingAthenaConnection";
                    case "hashCode":
                        return System.identityHashCode(proxy);
                    case "equals":
                        return proxy == args[0];
                    default:
                        Class<?> returnType = method.getReturnType();
                        if (returnType == void.class) {
                            return null;
                        }
                        if (returnType == boolean.class) {
                            return false;
                        }
                        if (returnType.isPrimitive()) {
                            return 0;
                        }
                        return null;
                }
            }
        );
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        return getConnection();
    }

    @Override
    public PrintWriter getLogWriter() {
        return null;
    }

    @Override
    public void setLogWriter(PrintWriter out) {
    }

    @Override
    public void setLoginTimeout(int seconds) {
    }

    @Override
    public int getLoginTimeout() {
        return 0;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new SQLException("Not a wrapper for " + iface.getName());
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) {
        return false;
    }
}
