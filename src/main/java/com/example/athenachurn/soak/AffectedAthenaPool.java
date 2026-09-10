package com.example.athenachurn.soak;

import com.amazon.athena.jdbc.AthenaDriver;
import com.amazon.athena.jdbc.configuration.ConnectionParameters;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.HikariPoolMXBean;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.conf.RenderQuotedNames;
import org.jooq.impl.DSL;
import org.jooq.impl.DefaultConfiguration;

import java.time.Duration;
import java.util.Properties;

/**
 * The application's Athena pool exactly as configured before our fix, the build the production
 * incidents happened on. Four of these settings are what
 * make the wedge reachable; each is called out below.
 */
public final class AffectedAthenaPool implements AutoCloseable {

    private final HikariDataSource dataSource;
    private final DSLContext dsl;

    private AffectedAthenaPool(HikariDataSource dataSource) {
        this.dataSource = dataSource;
        DefaultConfiguration configuration = new DefaultConfiguration();
        configuration.set(SQLDialect.DEFAULT);
        configuration.set(dataSource);
        configuration.settings().setRenderQuotedNames(RenderQuotedNames.NEVER);
        this.dsl = DSL.using(configuration);
    }

    public static AffectedAthenaPool start(String athenaEndpoint, Duration maxLifetime) {
        Properties driver = new Properties();
        driver.put(ConnectionParameters.ATHENA_ENDPOINT_PARAMETER.name(), athenaEndpoint);
        driver.put(ConnectionParameters.ATHENA_STREAMING_ENDPOINT_PARAMETER.name(), athenaEndpoint);
        driver.put(ConnectionParameters.REGION_PARAMETER.name(), "eu-central-1");
        driver.put(ConnectionParameters.WORK_GROUP_PARAMETER.name(), "primary");
        driver.put(ConnectionParameters.CREDENTIALS_PROVIDER_PARAMETER.name(), "Static");
        driver.put(ConnectionParameters.USER_PARAMETER.name(), "dummy");
        driver.put(ConnectionParameters.PASSWORD_PARAMETER.name(), "dummy-secret");
        // (1) The streaming fetcher: the one whose parse blocks the completing thread.
        driver.put(ConnectionParameters.RESULT_FETCHER.name(), "GetQueryResultsStream");
        // (2) No NetworkTimeoutMillis. HikariCP calls setNetworkTimeout around every validation and
        //     every close; each call makes the driver drop its SDK clients without closing them, so
        //     a request in flight on an old client is no longer reachable from close().
        // ConnectionTest is left at the driver default (true): one "select 1" per new connection.

        HikariConfig hikari = new HikariConfig();
        hikari.setJdbcUrl("jdbc:athena://");
        hikari.setDriverClassName(AthenaDriver.class.getName());
        hikari.setDataSourceProperties(driver);
        hikari.setPoolName("athena-pool");
        hikari.setMaximumPoolSize(15);
        // (3) maxLifetime 30 min in production. A connection whose lifetime expires while borrowed
        //     is closed on return - and a probe waiting 30 s on a slow query is a long borrow.
        hikari.setMaxLifetime(maxLifetime.toMillis());
        // (4) 3 s acquisition timeout: the 9,410 "request timed out after 3000ms" lines in the
        //     production logs. Not part of the wedge, but part of the noise around it.
        hikari.setConnectionTimeout(Duration.ofSeconds(3).toMillis());
        hikari.setValidationTimeout(Duration.ofSeconds(15).toMillis());
        hikari.setConnectionInitSql("select 1");
        hikari.setRegisterMbeans(true);
        return new AffectedAthenaPool(new HikariDataSource(hikari));
    }

    public DSLContext dsl() {
        return dsl;
    }

    public void warmUp() {
        dsl.fetch("select 1 one from (values(1))");
    }

    public String state() {
        HikariPoolMXBean mx = dataSource.getHikariPoolMXBean();
        return "Hikari total=" + mx.getTotalConnections() + " active=" + mx.getActiveConnections() + " idle="
            + mx.getIdleConnections() + " waiting=" + mx.getThreadsAwaitingConnection();
    }

    @Override
    public void close() {
        dataSource.close();
    }
}
