package dev.frndpovoa.project1.databaseproxy.spring;

import dev.frndpovoa.project1.databaseproxy.spring.config.DatabaseProxyDataSourceProperties;
import dev.frndpovoa.project1.databaseproxy.spring.config.DatabaseProxyProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hibernate.engine.jdbc.connections.spi.ConnectionProvider;

import java.sql.SQLException;

@Slf4j
@RequiredArgsConstructor
public class DatabaseProxyConnectionProvider implements ConnectionProvider {
    private final DatabaseProxyProperties databaseProxyProperties;
    private final DatabaseProxyDataSourceProperties databaseProxyDataSourceProperties;

    @Override
    public java.sql.Connection getConnection() throws SQLException {
        log.debug("getConnection()");
        return new Connection(databaseProxyProperties, databaseProxyDataSourceProperties);
    }

    @Override
    public void closeConnection(java.sql.Connection connection) throws SQLException {
        log.debug("closeConnection()");
        connection.close();
    }

    @Override
    public boolean supportsAggressiveRelease() {
        return false;
    }

    @Override
    public boolean isUnwrappableAs(Class<?> aClass) {
        return false;
    }

    @Override
    public <T> T unwrap(Class<T> aClass) {
        return null;
    }
}
