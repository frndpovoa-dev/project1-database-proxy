package dev.frndpovoa.project1.databaseproxy.spring;

import dev.frndpovoa.project1.databaseproxy.proto.BeginTransactionConfig;
import dev.frndpovoa.project1.databaseproxy.proto.Transaction;
import dev.frndpovoa.project1.databaseproxy.spring.config.DatabaseProxyDataSourceProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.support.AbstractPlatformTransactionManager;
import org.springframework.transaction.support.DefaultTransactionStatus;

@Slf4j
@RequiredArgsConstructor
public class DatabaseProxyTransactionManager extends AbstractPlatformTransactionManager {
    private final DatabaseProxyDataSourceProperties databaseProxyDataSourceProperties;

    @Override
    protected Object doGetTransaction() throws TransactionException {
        log.debug("doGetTransaction()");
        return TransactionHolder.builder().build();
    }

    @Override
    protected void doBegin(
            final Object object,
            final TransactionDefinition definition
    ) throws TransactionException {
        log.debug("doBegin()");
        final TransactionHolder transactionHolder = (TransactionHolder) object;
        final Transaction transaction = Connection.getDefaultInstance().getBlockingStub().beginTransaction(BeginTransactionConfig.newBuilder()
                .setConnectionString(databaseProxyDataSourceProperties.getUrl())
                .setTimeout(definition.getTimeout())
                .setReadOnly(definition.isReadOnly())
                .build());
        transactionHolder.setTransaction(transaction);
    }

    @Override
    protected void doCommit(
            final DefaultTransactionStatus status
    ) throws TransactionException {
        log.debug("doCommit()");
        final TransactionHolder transactionHolder = (TransactionHolder) status.getTransaction();
        transactionHolder.setTransaction(Connection.getDefaultInstance().getBlockingStub().commitTransaction(transactionHolder.getTransaction()));
    }

    @Override
    protected void doRollback(
            final DefaultTransactionStatus status
    ) throws TransactionException {
        log.debug("doRollback()");
        final TransactionHolder transactionHolder = (TransactionHolder) status.getTransaction();
        transactionHolder.setTransaction(Connection.getDefaultInstance().getBlockingStub().rollbackTransaction(transactionHolder.getTransaction()));
    }
}
