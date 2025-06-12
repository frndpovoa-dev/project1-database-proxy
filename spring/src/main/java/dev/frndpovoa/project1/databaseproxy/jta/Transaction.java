package dev.frndpovoa.project1.databaseproxy.jta;

import jakarta.transaction.RollbackException;
import jakarta.transaction.Status;
import jakarta.transaction.Synchronization;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import javax.transaction.xa.XAResource;
import java.util.UUID;

@Slf4j
@Getter
@Setter
@Builder
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
public class Transaction implements jakarta.transaction.Transaction {
    @EqualsAndHashCode.Include
    private final String id = UUID.randomUUID().toString();
    private dev.frndpovoa.project1.databaseproxy.jdbc.Connection connection;
    private dev.frndpovoa.project1.databaseproxy.proto.Transaction transaction;
    private boolean rollbackOnly = false;
    private int status = Status.STATUS_ACTIVE;

    @Override
    public void commit() {
        log.debug("commit({})", id);
        this.status = Status.STATUS_COMMITTING;
        connection.popTransaction(this);
        connection.getBlockingStub().commitTransaction(transaction);
        this.status = Status.STATUS_COMMITTED;
    }

    @Override
    public boolean delistResource(XAResource xaResource, int i) {
        return false;
    }

    @Override
    public boolean enlistResource(XAResource xaResource) throws RollbackException {
        return false;
    }

    @Override
    public int getStatus() {
        return status;
    }

    @Override
    public void registerSynchronization(Synchronization synchronization) throws RollbackException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void rollback() {
        log.debug("rollback({})", id);
        this.status = Status.STATUS_ROLLING_BACK;
        connection.popTransaction(this);
        connection.getBlockingStub().rollbackTransaction(transaction);
        this.status = Status.STATUS_ROLLEDBACK;
    }

    @Override
    public void setRollbackOnly() {
        this.rollbackOnly = true;
    }
}
