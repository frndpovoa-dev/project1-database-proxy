package dev.frndpovoa.project1.databaseproxy.spring;

import dev.frndpovoa.project1.databaseproxy.spring.config.DataSourceProperties;
import dev.frndpovoa.project1.databaseproxy.spring.config.DatabaseProxyProperties;
import dev.frndpovoa.project1.databaseproxy.proto.BeginTransactionConfig;
import dev.frndpovoa.project1.databaseproxy.proto.DatabaseProxyGrpc;
import dev.frndpovoa.project1.databaseproxy.proto.Empty;
import dev.frndpovoa.project1.databaseproxy.proto.Transaction;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.support.AbstractPlatformTransactionManager;
import org.springframework.transaction.support.DefaultTransactionStatus;

import java.util.concurrent.TimeUnit;

@RequiredArgsConstructor
public class DatabaseProxyTransactionManager extends AbstractPlatformTransactionManager {
    private final DatabaseProxyProperties databaseProxyProperties;
    private final DataSourceProperties dataSourceProperties;
    private ManagedChannel channel;
    private DatabaseProxyGrpc.DatabaseProxyBlockingStub blockingStub;

    @PreDestroy
    public void destroy() throws InterruptedException {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    @Override
    protected Object doGetTransaction() throws TransactionException {
        this.channel = ManagedChannelBuilder
                .forAddress(databaseProxyProperties.getHostname(), databaseProxyProperties.getPort())
                .usePlaintext()
                .build();
        this.blockingStub = DatabaseProxyGrpc
                .newBlockingStub(channel);
        return TransactionHolder.builder().build();
    }

    @Override
    protected void doBegin(
            final Object object,
            final TransactionDefinition definition
    ) throws TransactionException {
        final TransactionHolder transactionHolder = (TransactionHolder) object;
        final Transaction transaction = blockingStub.beginTransaction(BeginTransactionConfig.newBuilder()
                .setConnectionString(dataSourceProperties.getUrl())
                .setTimeout(definition.getTimeout())
                .setReadOnly(definition.isReadOnly())
                .build());
        transactionHolder.setTransaction(transaction);
    }

    @Override
    protected void doCommit(
            final DefaultTransactionStatus status
    ) throws TransactionException {
        try {
            final TransactionHolder transactionHolder = (TransactionHolder) status.getTransaction();
            transactionHolder.setTransaction(blockingStub.commitTransaction(transactionHolder.getTransaction()));
        } finally {
            blockingStub.closeConnection(Empty.newBuilder().build());
        }
    }

    @Override
    protected void doRollback(
            final DefaultTransactionStatus status
    ) throws TransactionException {
        try {
            final TransactionHolder transactionHolder = (TransactionHolder) status.getTransaction();
            transactionHolder.setTransaction(blockingStub.rollbackTransaction(transactionHolder.getTransaction()));
        } finally {
            blockingStub.closeConnection(Empty.newBuilder().build());
        }
    }
}
