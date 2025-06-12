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
        dev.frndpovoa.project1.databaseproxy.jta.Transaction transaction = dev.frndpovoa.project1.databaseproxy.jta.Transaction.builder().build();
        log.debug("doGetTransaction({})", transaction.getId());
        return transaction;
    }

    @Override
    protected void doBegin(
            final Object object,
            final TransactionDefinition definition
    ) throws TransactionException {
        boolean createNewTransaction = false;

        switch (definition.getPropagationBehavior()) {
            case TransactionDefinition.PROPAGATION_REQUIRED: {
                createNewTransaction = Optional.ofNullable(ConnectionHolder.getConnection())
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

        final dev.frndpovoa.project1.databaseproxy.jta.Transaction jdbcTransaction = (dev.frndpovoa.project1.databaseproxy.jta.Transaction) object;
        log.debug("doBegin({})", jdbcTransaction.getId());

        if (createNewTransaction) {
            final Connection connection = new Connection(databaseProxyProperties, databaseProxyDataSourceProperties);
            connection.pushTransaction(jdbcTransaction);

            ConnectionHolder.pushConnection(connection);

            final Transaction protoTransaction = connection
                    .getBlockingStub()
                    .beginTransaction(BeginTransactionConfig.newBuilder()
                            .setConnectionString(databaseProxyDataSourceProperties.getUrl())
                            .setTimeout(definition.getTimeout())
                            .setReadOnly(definition.isReadOnly())
                            .build());

            jdbcTransaction.setConnection(connection);
            jdbcTransaction.setTransaction(protoTransaction);
        } else {
            final Connection connection = ConnectionHolder.getConnection();

            jdbcTransaction.setConnection(connection);
            jdbcTransaction.setTransaction(connection.getTransaction().getTransaction());
        }
    }

    @Override
    protected void doCommit(
            final DefaultTransactionStatus status
    ) throws TransactionException {
        final dev.frndpovoa.project1.databaseproxy.jta.Transaction jdbcTransaction = (dev.frndpovoa.project1.databaseproxy.jta.Transaction) status.getTransaction();
        log.debug("doCommit({})", jdbcTransaction.getId());
        jdbcTransaction.commit();
    }

    @Override
    protected void doRollback(
            final DefaultTransactionStatus status
    ) throws TransactionException {
        final dev.frndpovoa.project1.databaseproxy.jta.Transaction jdbcTransaction = (dev.frndpovoa.project1.databaseproxy.jta.Transaction) status.getTransaction();
        log.debug("doRollback({})", jdbcTransaction.getId());
        jdbcTransaction.rollback();
    }
}
