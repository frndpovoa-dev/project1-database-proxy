package dev.frndpovoa.project1.databaseproxy.hibernate;

import dev.frndpovoa.project1.databaseproxy.ConnectionHolder;
import dev.frndpovoa.project1.databaseproxy.config.DatabaseProxyDataSourceProperties;
import dev.frndpovoa.project1.databaseproxy.config.DatabaseProxyProperties;
import dev.frndpovoa.project1.databaseproxy.jdbc.Connection;
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
        final Connection connection = new Connection(databaseProxyProperties, databaseProxyDataSourceProperties);
        ConnectionHolder.getDefaultInstance().pushConnection(connection);
        return connection;
    }

    @Override
    public void closeConnection(java.sql.Connection connection) throws SQLException {
        log.debug("closeConnection()");
        ConnectionHolder.getDefaultInstance().popConnection();
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
