package com.example.athenachurn;

import com.amazon.athena.jdbc.AthenaConnection;
import com.amazon.athena.jdbc.authentication.CredentialsProviderRegistry;
import com.amazon.athena.jdbc.authentication.StaticCredentialsProviderFactory;
import com.amazon.athena.jdbc.configuration.ConnectionConfiguration;
import com.amazon.athena.jdbc.configuration.ConnectionParameter;
import com.amazon.athena.jdbc.configuration.ConnectionParameters;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

/**
 * A {@link DataSource} that builds a real driver {@link AthenaConnection} through the driver's own
 * public {@code ConnectionConfiguration.from(...)} + {@code new AthenaConnection(configuration)}
 * path (the same path {@code AthenaDriver.connect(...)} uses internally), pointed at a mock
 * endpoint. {@code ConnectionTest=false} means construction issues no network calls, so this is
 * still fully offline.
 */
final class ReflectionHopAthenaDataSource implements DataSource {

    private final String mockBaseUrl;

    ReflectionHopAthenaDataSource(String mockBaseUrl) {
        this.mockBaseUrl = mockBaseUrl;
    }

    @Override
    public Connection getConnection() throws SQLException {
        Map<ConnectionParameter<?>, String> parameters = new HashMap<>();
        parameters.put(ConnectionParameters.ATHENA_ENDPOINT_PARAMETER, mockBaseUrl);
        parameters.put(ConnectionParameters.ATHENA_STREAMING_ENDPOINT_PARAMETER, mockBaseUrl);
        parameters.put(ConnectionParameters.REGION_PARAMETER, "eu-central-1");
        parameters.put(ConnectionParameters.WORK_GROUP_PARAMETER, "primary");
        parameters.put(ConnectionParameters.CONNECTION_TEST_PARAMETER, "false");
        parameters.put(ConnectionParameters.CREDENTIALS_PROVIDER_PARAMETER, "Static");
        parameters.put(ConnectionParameters.USER_PARAMETER, "dummy");
        parameters.put(ConnectionParameters.PASSWORD_PARAMETER, "dummy-secret");
        parameters.put(ConnectionParameters.RESULT_FETCHER, "GetQueryResultsStream");

        CredentialsProviderRegistry registry = new CredentialsProviderRegistry();
        registry.register(new StaticCredentialsProviderFactory());

        ConnectionConfiguration configuration = ConnectionConfiguration.from(parameters, registry);
        return new AthenaConnection(configuration);
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
