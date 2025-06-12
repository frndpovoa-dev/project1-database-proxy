package dev.frndpovoa.project1.databaseproxy.jta;

import jakarta.transaction.*;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

import javax.transaction.xa.XAResource;

@Getter
@Setter
@Builder
public class Transaction implements jakarta.transaction.Transaction {
    private dev.frndpovoa.project1.databaseproxy.jdbc.Connection connection;
    private dev.frndpovoa.project1.databaseproxy.proto.Transaction transaction;
    private boolean rollbackOnly = false;

    @Override
    public void commit() throws RollbackException, HeuristicMixedException, HeuristicRollbackException, SecurityException, IllegalStateException, SystemException {
        connection.getBlockingStub().commitTransaction(transaction);
    }

    @Override
    public boolean delistResource(XAResource xaResource, int i) throws IllegalStateException, SystemException {
        return false;
    }

    @Override
    public boolean enlistResource(XAResource xaResource) throws RollbackException, IllegalStateException, SystemException {
        return false;
    }

    @Override
    public int getStatus() throws SystemException {
        // FIXME
        return Status.STATUS_ACTIVE;
    }

    @Override
    public void registerSynchronization(Synchronization synchronization) throws RollbackException, IllegalStateException, SystemException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void rollback() throws IllegalStateException, SystemException {
        connection.getBlockingStub().rollbackTransaction(transaction);
    }

    @Override
    public void setRollbackOnly() throws IllegalStateException, SystemException {
        this.rollbackOnly = true;
    }
}
