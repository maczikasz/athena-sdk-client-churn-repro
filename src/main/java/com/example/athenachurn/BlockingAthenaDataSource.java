package com.example.athenachurn;

import software.amazon.awssdk.services.athena.AthenaAsyncClient;
import software.amazon.awssdk.services.athena.model.StartQueryExecutionRequest;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

final class BlockingAthenaDataSource implements DataSource {

    private final AthenaAsyncClient athenaClient;
    private final CountDownLatch connectionAttemptStarted = new CountDownLatch(1);

    BlockingAthenaDataSource(AthenaAsyncClient athenaClient) {
        this.athenaClient = athenaClient;
    }

    boolean awaitConnectionAttempt(long timeout, TimeUnit unit) throws InterruptedException {
        return connectionAttemptStarted.await(timeout, unit);
    }

    @Override
    public Connection getConnection() throws SQLException {
        connectionAttemptStarted.countDown();
        try {
            athenaClient.startQueryExecution(
                StartQueryExecutionRequest.builder().queryString("SELECT 1").workGroup("primary").build()
            ).get();
            throw new SQLNonTransientConnectionException("The deliberately starved Athena request unexpectedly completed");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new SQLNonTransientConnectionException("Interrupted while connecting to Athena", e);
        } catch (ExecutionException e) {
            throw new SQLNonTransientConnectionException("Athena connection attempt failed", e.getCause());
        }
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
