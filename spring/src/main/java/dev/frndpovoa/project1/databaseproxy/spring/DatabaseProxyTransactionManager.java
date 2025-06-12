package dev.frndpovoa.project1.databaseproxy.spring;

import dev.frndpovoa.project1.databaseproxy.ConnectionHolder;
import dev.frndpovoa.project1.databaseproxy.config.DatabaseProxyDataSourceProperties;
import dev.frndpovoa.project1.databaseproxy.config.DatabaseProxyProperties;
import dev.frndpovoa.project1.databaseproxy.jdbc.Connection;
import dev.frndpovoa.project1.databaseproxy.proto.BeginTransactionConfig;
import dev.frndpovoa.project1.databaseproxy.proto.Transaction;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.support.AbstractPlatformTransactionManager;
import org.springframework.transaction.support.DefaultTransactionStatus;

import java.util.Optional;

@Slf4j
@RequiredArgsConstructor
public class DatabaseProxyTransactionManager extends AbstractPlatformTransactionManager {
    private final DatabaseProxyProperties databaseProxyProperties;
    private final DatabaseProxyDataSourceProperties databaseProxyDataSourceProperties;

    @Override
    protected Object doGetTransaction() throws TransactionException {
        log.debug("doGetTransaction()");
        return dev.frndpovoa.project1.databaseproxy.jta.Transaction.builder().build();
    }

    @Override
    protected void doBegin(
            final Object object,
            final TransactionDefinition definition
    ) throws TransactionException {
        log.debug("doBegin()");

        boolean createNewTransaction = false;

        switch (definition.getPropagationBehavior()) {
            case TransactionDefinition.PROPAGATION_REQUIRED: {
                createNewTransaction = Optional.ofNullable(ConnectionHolder.getDefaultInstance())
                        .map(ConnectionHolder::getConnection)
                        .map(Connection::getTransaction)
                        .isEmpty();
                break;
            }
            case TransactionDefinition.PROPAGATION_REQUIRES_NEW: {
                createNewTransaction = true;
                break;
            }
            case TransactionDefinition.PROPAGATION_SUPPORTS: {
                createNewTransaction = false;
                break;
            }
        }

        if (createNewTransaction) {
            final Connection connection = new Connection(databaseProxyProperties, databaseProxyDataSourceProperties);
            ConnectionHolder.getDefaultInstance().pushConnection(connection);

            final dev.frndpovoa.project1.databaseproxy.jta.Transaction jdbcTransaction = (dev.frndpovoa.project1.databaseproxy.jta.Transaction) object;
            connection.pushTransaction(jdbcTransaction);

            final Transaction protoTransaction = connection
                    .getBlockingStub()
                    .beginTransaction(BeginTransactionConfig.newBuilder()
                            .setConnectionString(databaseProxyDataSourceProperties.getUrl())
                            .setTimeout(definition.getTimeout())
                            .setReadOnly(definition.isReadOnly())
                            .build());

            jdbcTransaction.setConnection(connection);
            jdbcTransaction.setTransaction(protoTransaction);
        }
    }

    @Override
    protected void doCommit(
            final DefaultTransactionStatus status
    ) throws TransactionException {
        log.debug("doCommit()");
        final dev.frndpovoa.project1.databaseproxy.jta.Transaction jdbcTransaction = (dev.frndpovoa.project1.databaseproxy.jta.Transaction) status.getTransaction();
        ConnectionHolder.getDefaultInstance()
                .getConnection()
                .getBlockingStub()
                .commitTransaction(jdbcTransaction.getTransaction());
    }

    @Override
    protected void doRollback(
            final DefaultTransactionStatus status
    ) throws TransactionException {
        log.debug("doRollback()");
        final dev.frndpovoa.project1.databaseproxy.jta.Transaction jdbcTransaction = (dev.frndpovoa.project1.databaseproxy.jta.Transaction) status.getTransaction();
        ConnectionHolder.getDefaultInstance()
                .getConnection()
                .getBlockingStub()
                .rollbackTransaction(jdbcTransaction.getTransaction());
    }
}
