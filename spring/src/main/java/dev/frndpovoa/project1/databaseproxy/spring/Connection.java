package dev.frndpovoa.project1.databaseproxy.spring;

import dev.frndpovoa.project1.databaseproxy.proto.DatabaseProxyGrpc;
import dev.frndpovoa.project1.databaseproxy.proto.Empty;
import dev.frndpovoa.project1.databaseproxy.spring.config.DatabaseProxyDataSourceProperties;
import dev.frndpovoa.project1.databaseproxy.spring.config.DatabaseProxyProperties;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import lombok.Getter;

import java.sql.*;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

@Getter
public class Connection implements java.sql.Connection {
    public static ThreadLocal<Connection> defaultInstance = new InheritableThreadLocal<>();

    public static Connection getDefaultInstance() {
        return defaultInstance.get();
    }

    private final ManagedChannel channel;
    private final DatabaseProxyDataSourceProperties databaseProxyDataSourceProperties;
    private final DatabaseProxyGrpc.DatabaseProxyBlockingStub blockingStub;
    private boolean autoCommit = true;
    private boolean closed = false;
    private boolean readOnly = false;

    public Connection(
            final DatabaseProxyProperties databaseProxyProperties,
            final DatabaseProxyDataSourceProperties databaseProxyDataSourceProperties
    ) {
        this.channel = ManagedChannelBuilder
                .forAddress(databaseProxyProperties.getHostname(), databaseProxyProperties.getPort())
                .usePlaintext()
                .build();
        this.blockingStub = DatabaseProxyGrpc
                .newBlockingStub(channel);
        this.databaseProxyDataSourceProperties = databaseProxyDataSourceProperties;
        defaultInstance.set(this);
    }

    @Override
    public Statement createStatement() throws SQLException {
        return new Statement(
                this,
                autoCommit,
                readOnly,
                blockingStub
        );
    }

    @Override
    public PreparedStatement prepareStatement(final String sql) throws SQLException {
        return new PreparedStatement(
                this,
                autoCommit,
                readOnly,
                sql,
                blockingStub
        );
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        return sql;
    }

    @Override
    public void setAutoCommit(final boolean autoCommit) throws SQLException {
        this.autoCommit = autoCommit;
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        return autoCommit;
    }

    @Override
    public void commit() throws SQLException {
        blockingStub.commitTransaction(TransactionHolder.getDefaultInstance().getTransaction());
    }

    @Override
    public void rollback() throws SQLException {
        blockingStub.rollbackTransaction(TransactionHolder.getDefaultInstance().getTransaction());
    }

    @Override
    public void close() throws SQLException {
        this.closed = true;
        blockingStub.closeConnection(Empty.getDefaultInstance());
//        try {
//            channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
//        } catch (InterruptedException e) {
//            throw new SQLException(e);
//        }
    }

    @Override
    public boolean isClosed() throws SQLException {
        return closed;
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        return null;
    }

    @Override
    public void setReadOnly(final boolean readOnly) throws SQLException {
        this.readOnly = readOnly;
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return readOnly;
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public String getCatalog() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        return java.sql.Connection.TRANSACTION_READ_COMMITTED;
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public PreparedStatement prepareStatement(
            final String sql,
            final int resultSetType,
            final int resultSetConcurrency) throws SQLException {
        return prepareStatement(sql);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public int getHoldability() throws SQLException {
        return java.sql.ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Clob createClob() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Blob createBlob() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public NClob createNClob() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        // FIXME
        return true;
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void setSchema(String schema) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public String getSchema() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void abort(Executor executor) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        return 0;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }
}
