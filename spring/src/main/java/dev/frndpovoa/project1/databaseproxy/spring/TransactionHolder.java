package dev.frndpovoa.project1.databaseproxy.spring;

import dev.frndpovoa.project1.databaseproxy.proto.DatabaseProxyGrpc;
import dev.frndpovoa.project1.databaseproxy.proto.Transaction;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@Builder
public class TransactionHolder {
    private static ThreadLocal<TransactionHolder> defaultInstance = new InheritableThreadLocal<>();

    public static TransactionHolder getDefaultInstance() {
        return defaultInstance.get();
    }

    private Transaction transaction;
    private DatabaseProxyGrpc.DatabaseProxyBlockingStub blockingStub;

    public static TransactionHolderBuilder builder() {
        return new CustomTransactionHolderBuilder();
    }

    private static class CustomTransactionHolderBuilder extends TransactionHolderBuilder {
        @Override
        public TransactionHolder build() {
            final TransactionHolder thiz = super.build();
            defaultInstance.set(thiz);
            return thiz;
        }
    }
}
