package dev.frndpovoa.project1.databaseproxy.service;

import com.google.protobuf.InvalidProtocolBufferException;
import dev.frndpovoa.project1.databaseproxy.config.IgniteProperties;
import dev.frndpovoa.project1.databaseproxy.proto.*;
import io.grpc.stub.StreamObserver;
import lombok.Builder;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.sql.*;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;

@Slf4j
@Service
@RequiredArgsConstructor
public class DatabaseProxyService extends DatabaseProxyGrpc.DatabaseProxyImplBase {
    private final ConcurrentHashMap<String, DatabaseOperation> transactionalOperationMap = new ConcurrentHashMap<>();
    private final UniqueIdGenerator uniqueIdGenerator;
    private final IgniteProperties igniteProperties;

    @Override
    public void beginTransaction(BeginTransactionConfig config, StreamObserver<Transaction> responseObserver) {
        Transaction transaction = Transaction.newBuilder()
                .setId(uniqueIdGenerator.generate(Transaction.class))
                .setStatus(Transaction.Status.ACTIVE)
                .build();

        DatabaseOperation databaseOperation = DatabaseOperation.builder()
                .igniteProperties(igniteProperties)
                .transaction(transaction)
                .build();

        try {
            databaseOperation
                    .openConnection()
                    .beginTransaction(config);

            responseObserver.onNext(transaction);
            responseObserver.onCompleted();
        } catch (Throwable t) {
            responseObserver.onError(t);
        } finally {
            transactionalOperationMap.put(transaction.getId(), databaseOperation);
        }
    }

    @Override
    public void commitTransaction(Transaction transaction, StreamObserver<Transaction> responseObserver) {
        try {
            DatabaseOperation databaseOperation = getTransactionalOperation(transaction);

            if (databaseOperation.getTransaction().getStatus() != Transaction.Status.ACTIVE) {
                responseObserver.onError(new IllegalArgumentException("Transaction is not active"));
                return;
            }

            boolean committed = databaseOperation
                    .commitTransaction();

            transaction = databaseOperation.getTransaction().toBuilder()
                    .setStatus(committed ? Transaction.Status.COMMITTED : Transaction.Status.UNKNOWN)
                    .build();

            databaseOperation.setTransaction(transaction);

            responseObserver.onNext(transaction);
            responseObserver.onCompleted();
        } catch (Throwable t) {
            responseObserver.onError(t);
        }
    }

    @Override
    public void rollbackTransaction(Transaction transaction, StreamObserver<Transaction> responseObserver) {
        try {
            DatabaseOperation databaseOperation = getTransactionalOperation(transaction);

            if (databaseOperation.getTransaction().getStatus() != Transaction.Status.ACTIVE) {
                responseObserver.onError(new IllegalArgumentException("Transaction is not active"));
                return;
            }

            boolean rolledBack = databaseOperation
                    .rollbackTransaction();

            transaction = databaseOperation.getTransaction().toBuilder()
                    .setStatus(rolledBack ? Transaction.Status.ROLLED_BACK : Transaction.Status.UNKNOWN)
                    .build();

            databaseOperation.setTransaction(transaction);

            responseObserver.onNext(transaction);
            responseObserver.onCompleted();
        } catch (Throwable t) {
            responseObserver.onError(t);
        }
    }

    @Override
    public void ddl(DdlConfig config, StreamObserver<DdlResult> responseObserver) {
        try {
            DatabaseOperation databaseOperation = DatabaseOperation.builder()
                    .igniteProperties(igniteProperties)
                    .transaction(null)
                    .build();

            ExecuteConfig executeConfig = ExecuteConfig.newBuilder()
                    .setQuery(config.getQuery())
                    .setTimeout(config.getTimeout())
                    .build();

            try {
                databaseOperation
                        .openConnection();

                ExecuteResult ignored = databaseOperation
                        .execute(executeConfig);
            } finally {
                databaseOperation
                        .closeConnection();
            }

            DdlResult result = DdlResult.newBuilder()
                    .build();

            responseObserver.onNext(result);
            responseObserver.onCompleted();
        } catch (Throwable t) {
            responseObserver.onError(t);
        }
    }

    @Override
    public void execute(ExecuteConfig config, StreamObserver<ExecuteResult> responseObserver) {
        try {
            DatabaseOperation databaseOperation = DatabaseOperation.builder()
                    .igniteProperties(igniteProperties)
                    .transaction(null)
                    .build();

            ExecuteResult result;
            try {
                databaseOperation
                        .openConnection();

                result = databaseOperation
                        .execute(config);
            } finally {
                databaseOperation
                        .closeConnection();
            }

            responseObserver.onNext(result);
            responseObserver.onCompleted();
        } catch (Throwable t) {
            responseObserver.onError(t);
        }
    }

    @Override
    public void query(QueryConfig config, StreamObserver<QueryResult> responseObserver) {
        try {
            DatabaseOperation databaseOperation = DatabaseOperation.builder()
                    .igniteProperties(igniteProperties)
                    .transaction(null)
                    .build();

            QueryResult result;
            try {
                databaseOperation
                        .openConnection();

                result = databaseOperation
                        .query(config);
            } finally {
                databaseOperation
                        .closeConnection();
            }

            responseObserver.onNext(result);
            responseObserver.onCompleted();
        } catch (Throwable t) {
            responseObserver.onError(t);
        }
    }

    @Override
    public void executeTx(ExecuteTxConfig config, StreamObserver<ExecuteResult> responseObserver) {
        try {
            ExecuteResult result = getTransactionalOperation(config.getTransaction())
                    .execute(config.getExecuteConfig());
            responseObserver.onNext(result);
            responseObserver.onCompleted();
        } catch (Throwable t) {
            responseObserver.onError(t);
        }
    }

    @Override
    public void queryTx(QueryTxConfig config, StreamObserver<QueryResult> responseObserver) {
        try {
            QueryResult result = getTransactionalOperation(config.getTransaction())
                    .query(config.getQueryConfig());
            responseObserver.onNext(result);
            responseObserver.onCompleted();
        } catch (Throwable t) {
            responseObserver.onError(t);
        }
    }

    @Override
    public void next(NextConfig request, StreamObserver<QueryResult> responseObserver) {
        responseObserver.onCompleted();
    }

    @Override
    public void closeConnection(Empty request, StreamObserver<Empty> responseObserver) {
        responseObserver.onCompleted();
    }

    @Override
    public void closeStatement(Empty request, StreamObserver<Empty> responseObserver) {
        responseObserver.onCompleted();
    }

    @Override
    public void closeRows(Empty request, StreamObserver<Empty> responseObserver) {
        responseObserver.onCompleted();
    }

    DatabaseOperation getTransactionalOperation(Transaction transaction) {
        return Optional.ofNullable(transactionalOperationMap.get(transaction.getId()))
                .orElseThrow(() -> new IllegalArgumentException("Transaction not found"));
    }
}

@FunctionalInterface
interface DoInTransaction {
    void doInTransaction(Params params);

    @Getter
    @Builder
    class Params {
        private final Connection connection;
        private final ExecutorService sideTaskExecutor;
        private final AtomicBoolean shouldContinue;
    }
}

@Slf4j
@Builder
class DatabaseOperation {
    private final AtomicBoolean shouldContinue = new AtomicBoolean(true);
    private final LinkedBlockingQueue<DoInTransaction> taskQueue = new LinkedBlockingQueue<>() {
        @Override
        public boolean add(DoInTransaction doInTransaction) {
            if (!shouldContinue.get()) {
                return false;
            }
            return super.add(doInTransaction);
        }
    };
    private IgniteProperties igniteProperties;
    @Getter
    @Setter
    private Transaction transaction;

    DatabaseOperation openConnection() {
        CompletableFuture<Boolean> connOpened = new CompletableFuture<>();
        CompletableFuture.runAsync(() -> {
            ExecutorService sideTaskExecutor = Executors.newFixedThreadPool(2);
            try (Connection connection = DriverManager.getConnection(igniteProperties.getUrl())) {
                connOpened.complete(true);

                DoInTransaction.Params params = DoInTransaction.Params.builder()
                        .connection(connection)
                        .sideTaskExecutor(sideTaskExecutor)
                        .shouldContinue(shouldContinue)
                        .build();

                while (shouldContinue.get()) {
                    DoInTransaction callback = taskQueue.poll(1, TimeUnit.SECONDS);
                    if (callback != null) {
                        callback.doInTransaction(params);
                    }
                }
            } catch (Throwable t) {
                log.error(t.getMessage(), t);
                connOpened.completeExceptionally(t);
            } finally {
                sideTaskExecutor.shutdownNow();
            }
        });
        connOpened.join();
        return this;
    }

    void closeConnection() {
        CompletableFuture<Void> future = new CompletableFuture<>();
        boolean accepted = taskQueue.add(params -> {
            try {
                shouldContinue.set(false);
                future.complete(null);
            } catch (Throwable t) {
                log.error(t.getMessage(), t);
                future.completeExceptionally(t);
            }
        });
        if (accepted) {
            future.join();
        }
    }

    boolean beginTransaction(BeginTransactionConfig config) {
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        boolean accepted = taskQueue.add(params -> {
            try {
                params.getConnection().setAutoCommit(false);
                params.getConnection().setReadOnly(config.getReadOnly());

                CompletableFuture
                        .runAsync(() -> {
                            sleepUninterruptibly(Duration.ofMillis(config.getTimeout()));
                            boolean ignored = Optional.ofNullable(params.getConnection())
                                    .filter(this::isActive)
                                    .flatMap(this::rollback)
                                    .orElse(false);
                            shouldContinue.set(false);
                        }, params.getSideTaskExecutor());

                future.complete(true);
            } catch (Throwable t) {
                log.error(t.getMessage(), t);
                future.completeExceptionally(t);
            }
        });
        if (accepted) {
            return future.join();
        } else {
            return false;
        }
    }

    boolean commitTransaction() {
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        boolean accepted = taskQueue.add(params -> {
            try {
                boolean result = Optional.ofNullable(params.getConnection())
                        .filter(this::isActive)
                        .flatMap(this::commit)
                        .orElse(false);
                shouldContinue.set(false);
                future.complete(result);
            } catch (Throwable t) {
                log.error(t.getMessage(), t);
                future.completeExceptionally(t);
            }
        });
        if (accepted) {
            return future.join();
        } else {
            return false;
        }
    }

    boolean rollbackTransaction() {
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        boolean accepted = taskQueue.add(params -> {
            try {
                boolean result = Optional.ofNullable(params.getConnection())
                        .filter(this::isActive)
                        .flatMap(this::rollback)
                        .orElse(false);
                shouldContinue.set(false);
                future.complete(result);
            } catch (Throwable t) {
                log.error(t.getMessage(), t);
                future.completeExceptionally(t);
            }
        });
        if (accepted) {
            return future.join();
        } else {
            return false;
        }
    }

    public ExecuteResult execute(ExecuteConfig config) {
        CompletableFuture<Integer> future = new CompletableFuture<>();
        boolean accepted = taskQueue.add(params -> {
            try (PreparedStatement stmt = params.getConnection().prepareStatement(config.getQuery())) {
                stmt.setQueryTimeout(config.getTimeout());

                IntStream.range(0, config.getArgsCount())
                        .forEach(i -> setSqlArg(stmt, i + 1, config.getArgs(i)));

                future.complete(stmt.executeUpdate());
            } catch (Throwable t) {
                log.error(t.getMessage(), t);
                future.completeExceptionally(t);
            }
        });
        if (accepted) {
            return ExecuteResult.newBuilder()
                    .setRowsAffected(future.join())
                    .build();
        } else {
            return ExecuteResult.newBuilder()
                    .build();
        }
    }

    public QueryResult query(QueryConfig config) {
        CompletableFuture<List<Row>> future = new CompletableFuture<>();
        boolean accepted = taskQueue.add(params -> {
            try (PreparedStatement stmt = params.getConnection().prepareStatement(config.getQuery())) {
                stmt.setQueryTimeout(config.getTimeout());

                IntStream.range(0, config.getArgsCount())
                        .forEach(i -> setSqlArg(stmt, i + 1, config.getArgs(i)));

                List<Row> results = new ArrayList<>();
                try (ResultSet rs = stmt.executeQuery()) {
                    while (rs.next()) {
                        results.add(Row.newBuilder()
                                .addAllCols(IntStream.range(1, rs.getMetaData().getColumnCount() + 1)
                                        .mapToObj(i -> getSqlArg(rs, i))
                                        .toList()
                                )
                                .build());
                    }
                }

                future.complete(results);
            } catch (Throwable t) {
                log.error(t.getMessage(), t);
                future.completeExceptionally(t);
            }
        });
        if (accepted) {
            return QueryResult.newBuilder()
                    .addAllRows(future.join())
                    .build();
        } else {
            return QueryResult.newBuilder()
                    .build();
        }
    }

    boolean isActive(Connection conn) {
        return Optional.ofNullable(conn).stream()
                .anyMatch(it -> {
                    try {
                        return !it.isClosed();
                    } catch (SQLException e) {
                        return false;
                    }
                });
    }

    Optional<Boolean> commit(Connection conn) {
        return Optional.ofNullable(conn).stream()
                .map(it -> {
                    try {
                        it.commit();
                        return true;
                    } catch (SQLException e) {
                        return false;
                    }
                })
                .findFirst();
    }

    Optional<Boolean> rollback(Connection conn) {
        return Optional.ofNullable(conn).stream()
                .map(it -> {
                    try {
                        it.rollback();
                        return true;
                    } catch (SQLException e) {
                        return false;
                    }
                })
                .findFirst();
    }

    protected Value getSqlArg(ResultSet rs, int i) {
        try {
            switch (JDBCType.valueOf(rs.getMetaData().getColumnType(i))) {
                case BIGINT -> {
                    return Value.newBuilder()
                            .setCode(ValueCode.INT64)
                            .setData(ValueInt64.newBuilder()
                                    .setValue(rs.getLong(i))
                                    .build()
                                    .toByteString()
                            )
                            .build();

                }
                case DOUBLE -> {
                    return Value.newBuilder()
                            .setCode(ValueCode.FLOAT64)
                            .setData(ValueFloat64.newBuilder()
                                    .setValue(rs.getDouble(i))
                                    .build()
                                    .toByteString()
                            )
                            .build();
                }
                case BOOLEAN -> {
                    return Value.newBuilder()
                            .setCode(ValueCode.BOOL)
                            .setData(ValueBool.newBuilder()
                                    .setValue(rs.getBoolean(i))
                                    .build()
                                    .toByteString()
                            )
                            .build();
                }
                case VARCHAR -> {
                    return Value.newBuilder()
                            .setCode(ValueCode.STRING)
                            .setData(ValueString.newBuilder()
                                    .setValue(rs.getString(i))
                                    .build()
                                    .toByteString()
                            )
                            .build();
                }
                case DATE -> {
                    return Value.newBuilder()
                            .setCode(ValueCode.TIME)
                            .setData(ValueTime.newBuilder()
                                    .setValue(rs.getDate(i).getTime())
                                    .build()
                                    .toByteString()
                            )
                            .build();
                }
                case NULL -> {
                    return Value.newBuilder()
                            .setCode(ValueCode.NULL)
                            .setData(ValueNull.newBuilder()
                                    .build()
                                    .toByteString()
                            )
                            .build();
                }
                default -> {
                    return null;
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    protected void setSqlArg(PreparedStatement stmt, int i, Value value) {
        try {
            switch (value.getCode()) {
                case INT64 -> {
                    stmt.setLong(i, ValueInt64.parseFrom(value.getData()).getValue());
                }
                case FLOAT64 -> {
                    stmt.setDouble(i, ValueFloat64.parseFrom(value.getData()).getValue());
                }
                case BOOL -> {
                    stmt.setBoolean(i, ValueBool.parseFrom(value.getData()).getValue());
                }
                case STRING -> {
                    stmt.setString(i, ValueString.parseFrom(value.getData()).getValue());
                }
                case TIME -> {
                    stmt.setDate(i, new Date(ValueTime.parseFrom(value.getData()).getValue()));
                }
                case NULL -> {
                    stmt.setNull(i, Types.NULL);
                }
                default -> {
                    stmt.setNull(i, Types.NULL);
                }
            }
        } catch (InvalidProtocolBufferException | SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
