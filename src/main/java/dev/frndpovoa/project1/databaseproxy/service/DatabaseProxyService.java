package dev.frndpovoa.project1.databaseproxy.service;

import dev.frndpovoa.project1.databaseproxy.proto.*;
import io.grpc.stub.StreamObserver;
import lombok.Builder;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Duration;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;

@Service
@RequiredArgsConstructor
public class DatabaseProxyService extends DatabaseProxyGrpc.DatabaseProxyImplBase {
    private final PlatformTransactionManager transactionManager;
    private final UniqueIdGenerator uniqueIdGenerator;
    private final Map<String, TransactionalOperation> transactionalOperationMap = new ConcurrentHashMap<>();

    @Override
    public void beginTransaction(BeginTransactionConfig request, StreamObserver<Transaction> responseObserver) {
        Transaction transaction = Transaction.newBuilder()
                .setId(uniqueIdGenerator.generate(DatabaseProxyService.class.getCanonicalName()))
                .build();

        TransactionalOperation transactionalOperation = TransactionalOperation.builder()
                .transactionManager(transactionManager)
                .transaction(transaction)
                .build()
                .beginTransaction(request);

        transactionalOperationMap.putIfAbsent(transaction.getId(), transactionalOperation);

        responseObserver.onNext(transaction);
        responseObserver.onCompleted();
    }

    @Override
    public void commitTransaction(Transaction request, StreamObserver<Transaction> responseObserver) {
        super.commitTransaction(request, responseObserver);
    }

    @Override
    public void rollbackTransaction(Transaction request, StreamObserver<Transaction> responseObserver) {
        super.rollbackTransaction(request, responseObserver);
    }

    @Override
    public void execute(ExecuteConfig request, StreamObserver<ExecuteResult> responseObserver) {
        super.execute(request, responseObserver);
    }

    @Override
    public void query(QueryConfig request, StreamObserver<QueryResult> responseObserver) {
        super.query(request, responseObserver);
    }

    @Override
    public void next(NextConfig request, StreamObserver<QueryResult> responseObserver) {
        super.next(request, responseObserver);
    }

    @Override
    public void closeConnection(Empty request, StreamObserver<Empty> responseObserver) {
        super.closeConnection(request, responseObserver);
    }

    @Override
    public void closeStatement(Empty request, StreamObserver<Empty> responseObserver) {
        super.closeStatement(request, responseObserver);
    }

    @Override
    public void closeRows(Empty request, StreamObserver<Empty> responseObserver) {
        super.closeRows(request, responseObserver);
    }
}

@Getter
@Builder
class CustomTransactionDefinition implements TransactionDefinition {
    @Builder.Default
    private int propagationBehavior = PROPAGATION_REQUIRED;
    @Builder.Default
    private int isolationLevel = ISOLATION_DEFAULT;
    private String name;
    private int timeout;
    @Builder.Default
    private boolean readOnly = false;
}

@Builder
class TransactionalOperation {
    private final AtomicBoolean shouldContinue = new AtomicBoolean(true);
    private final Queue<TransactionCallback<?>> taskQueue = new ConcurrentLinkedQueue<>();
    private PlatformTransactionManager transactionManager;
    private TransactionTemplate transactionTemplate;
    private Transaction transaction;

    TransactionalOperation beginTransaction(BeginTransactionConfig request) {
        CustomTransactionDefinition transactionDefinition = CustomTransactionDefinition.builder()
                .name(transaction.getId())
                .timeout(request.getTransactionTimeout())
                .build();
        this.transactionTemplate = new TransactionTemplate(transactionManager, transactionDefinition);
        this.transactionTemplate.execute(status -> {
            while (shouldContinue.get()) {
                TransactionCallback<?> callback;
                do {
                    callback = taskQueue.poll();
                    if (callback != null) {
                        callback.doInTransaction(status);
                    }
                } while (callback != null);
                sleepUninterruptibly(Duration.ofSeconds(1));
            }
            return null;
        });
        return this;
    }
}
