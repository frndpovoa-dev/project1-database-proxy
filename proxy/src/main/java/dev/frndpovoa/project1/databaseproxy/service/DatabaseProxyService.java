package dev.frndpovoa.project1.databaseproxy.service;

/*-
 * #%L
 * database-proxy
 * %%
 * Copyright (C) 2025 Fernando Lemes Povoa
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import com.google.protobuf.InvalidProtocolBufferException;
import dev.frndpovoa.project1.databaseproxy.proto.*;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.openjpa.lib.jdbc.SQLFormatter;
import org.slf4j.MDC;
import org.springframework.stereotype.Service;

import java.sql.*;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;

@Slf4j
@Service
public class DatabaseProxyService extends DatabaseProxyGrpc.DatabaseProxyImplBase {
    private final ConcurrentHashMap<String, DatabaseOperation> transactionMap = new ConcurrentHashMap<>();
    private final UniqueIdGenerator uniqueIdGenerator;
    private final SQLFormatter defaultSqlFormatter;
    private final String node;

    public DatabaseProxyService(
            final UniqueIdGenerator uniqueIdGenerator,
            @org.springframework.beans.factory.annotation.Value("${app.node}") final String node
    ) {
        log.info("Database proxy at node {}", node);

        this.uniqueIdGenerator = uniqueIdGenerator;
        this.node = node;

        final SQLFormatter sqlFormatter = new SQLFormatter();
        sqlFormatter.setClauseIndent("");
        sqlFormatter.setDoubleSpace(false);
        sqlFormatter.setLineLength(Integer.MAX_VALUE);
        sqlFormatter.setMultiLine(false);
        sqlFormatter.setNewline("");
        sqlFormatter.setWrapIndent("");
        this.defaultSqlFormatter = sqlFormatter;
    }

    @Override
    public void beginTransaction(final BeginTransactionConfig config, final StreamObserver<Transaction> responseObserver) {
        final Transaction transaction = Transaction.newBuilder()
                .setId(uniqueIdGenerator.generate(Transaction.class))
                .setStatus(Transaction.Status.ACTIVE)
                .setNode(node)
                .build();

        log.debug("beginTransaction(timeout: {}) -> {}", config.getTimeout(), transaction.getStatus());

        final DatabaseOperation ops = DatabaseOperation.builder()
                .uniqueIdGenerator(uniqueIdGenerator)
                .connectionString(config.getConnectionString())
                .sqlFormatter(defaultSqlFormatter)
                .transaction(transaction)
                .build();

        transactionMap.put(transaction.getId(), ops);

        try {
            ops.openConnection();
            ops.beginTransaction(config);

            responseObserver.onNext(transaction);
            responseObserver.onCompleted();
        } catch (final Throwable t) {
            responseObserver.onError(Status.UNKNOWN
                    .withDescription(t.getMessage())
                    .withCause(t)
                    .asRuntimeException());
        }
    }

    @Override
    public void commitTransaction(final Transaction transaction, final StreamObserver<Transaction> responseObserver) {
        try {
            final DatabaseOperation ops = getDatabaseOperationByTransaction(transaction)
                    .orElseThrow(() -> new IllegalArgumentException("Transaction not found"));

            if (ops.getTransaction().getStatus() != Transaction.Status.ACTIVE) {
                responseObserver.onError(Status.INVALID_ARGUMENT
                        .withDescription("Transaction is not active")
                        .asRuntimeException());
                return;
            }

            final boolean committed = ops.commitTransaction();

            final Transaction result = ops.getTransaction().toBuilder()
                    .setStatus(committed ? Transaction.Status.COMMITTED : Transaction.Status.UNKNOWN)
                    .build();

            log.debug("commitTransaction() -> {}", result.getStatus());

            responseObserver.onNext(result);
            responseObserver.onCompleted();
        } catch (final Throwable t) {
            responseObserver.onError(Status.UNKNOWN
                    .withDescription(t.getMessage())
                    .withCause(t)
                    .asRuntimeException());
        } finally {
            deleteDatabaseOperationByTransaction(transaction);
        }
    }

    @Override
    public void rollbackTransaction(final Transaction transaction, final StreamObserver<Transaction> responseObserver) {
        try {
            Transaction result = null;

            final Optional<DatabaseOperation> opsOptional = getDatabaseOperationByTransaction(transaction);
            if (opsOptional.isPresent()) {


                final DatabaseOperation ops = opsOptional.get();

                if (ops.getTransaction().getStatus() != Transaction.Status.ACTIVE) {
                    responseObserver.onError(Status.INVALID_ARGUMENT
                            .withDescription("Transaction is not active")
                            .asRuntimeException());
                    return;
                }

                final boolean rolledBack = ops.rollbackTransaction();

                result = ops.getTransaction().toBuilder()
                        .setStatus(rolledBack ? Transaction.Status.ROLLED_BACK : Transaction.Status.UNKNOWN)
                        .build();
            } else {
                result = Transaction.newBuilder()
                        .setStatus(Transaction.Status.UNKNOWN)
                        .build();
            }

            log.debug("rollbackTransaction() -> {}", result.getStatus());

            responseObserver.onNext(result);
            responseObserver.onCompleted();
        } catch (final Throwable t) {
            responseObserver.onError(Status.UNKNOWN
                    .withDescription(t.getMessage())
                    .withCause(t)
                    .asRuntimeException());
        } finally {
            deleteDatabaseOperationByTransaction(transaction);
        }
    }

    @Override
    public void execute(final ExecuteConfig config, final StreamObserver<ExecuteResult> responseObserver) {
        try {
            final Transaction transaction = Transaction.newBuilder()
                    .setId(uniqueIdGenerator.generate(Transaction.class))
                    .setStatus(Transaction.Status.ACTIVE)
                    .setNode(node)
                    .build();

            final DatabaseOperation ops = DatabaseOperation.builder()
                    .uniqueIdGenerator(uniqueIdGenerator)
                    .connectionString(config.getConnectionString())
                    .sqlFormatter(defaultSqlFormatter)
                    .transaction(transaction)
                    .build();

            final boolean dml = config.getQuery()
                    .matches("(?i)^(insert|update|delete|merge)\\s+.*");

            ExecuteResult result;
            try {
                ops.openConnection();
                try {
                    if (dml) {
                        ops.beginTransaction(BeginTransactionConfig.newBuilder()
                                .setTimeout(config.getTimeout())
                                .setReadOnly(false)
                                .build());
                    }
                    result = ops.execute(config);
                } finally {
                    if (dml) {
                        ops.commitTransaction();
                    }
                }
            } finally {
                ops.closeConnection();
            }

            responseObserver.onNext(result);
            responseObserver.onCompleted();
        } catch (final Throwable t) {
            responseObserver.onError(Status.UNKNOWN
                    .withDescription(t.getMessage())
                    .withCause(t)
                    .asRuntimeException());
        }
    }

    @Override
    public void query(final QueryConfig config, final StreamObserver<QueryResult> responseObserver) {
        try {
            final Transaction transaction = Transaction.newBuilder()
                    .setId(uniqueIdGenerator.generate(Transaction.class))
                    .setStatus(Transaction.Status.ACTIVE)
                    .setNode(node)
                    .build();

            final DatabaseOperation ops = DatabaseOperation.builder()
                    .uniqueIdGenerator(uniqueIdGenerator)
                    .connectionString(config.getConnectionString())
                    .sqlFormatter(defaultSqlFormatter)
                    .transaction(transaction)
                    .build();

            QueryResult result;
            try {
                ops.openConnection();
                try {
                    ops.beginTransaction(BeginTransactionConfig.newBuilder()
                            .setTimeout(config.getTimeout())
                            .setReadOnly(true)
                            .build());
                    result = ops.query(config);
                    result = ops.next(NextConfig.newBuilder()
                            .setTransaction(transaction)
                            .setQueryResultId(result.getId())
                            .build());
                } finally {
                    ops.rollbackTransaction();
                }
            } finally {
                ops.closeConnection();
            }

            responseObserver.onNext(result);
            responseObserver.onCompleted();
        } catch (final Throwable t) {
            responseObserver.onError(Status.UNKNOWN
                    .withDescription(t.getMessage())
                    .withCause(t)
                    .asRuntimeException());
        }
    }

    @Override
    public void executeTx(final ExecuteTxConfig config, final StreamObserver<ExecuteResult> responseObserver) {
        try {
            final DatabaseOperation ops = getDatabaseOperationByTransaction(config.getTransaction())
                    .orElseThrow(() -> new IllegalArgumentException("Transaction not found"));
            final ExecuteResult result = ops.execute(config.getExecuteConfig());
            responseObserver.onNext(result);
            responseObserver.onCompleted();
        } catch (final Throwable t) {
            responseObserver.onError(Status.UNKNOWN
                    .withDescription(t.getMessage())
                    .withCause(t)
                    .asRuntimeException());
        }
    }

    @Override
    public void queryTx(final QueryTxConfig config, final StreamObserver<QueryResult> responseObserver) {
        try {
            final DatabaseOperation ops = getDatabaseOperationByTransaction(config.getTransaction())
                    .orElseThrow(() -> new IllegalArgumentException("Transaction not found"));
            QueryResult result = ops.query(config.getQueryConfig());
            result = ops.next(NextConfig.newBuilder()
                    .setTransaction(config.getTransaction())
                    .setQueryResultId(result.getId())
                    .build());
            responseObserver.onNext(result);
            responseObserver.onCompleted();
        } catch (final Throwable t) {
            responseObserver.onError(Status.UNKNOWN
                    .withDescription(t.getMessage())
                    .withCause(t)
                    .asRuntimeException());
        }
    }

    @Override
    public void next(final NextConfig config, final StreamObserver<QueryResult> responseObserver) {
        try {
            final DatabaseOperation ops = getDatabaseOperationByTransaction(config.getTransaction())
                    .orElseThrow(() -> new IllegalArgumentException("Transaction not found"));
            final QueryResult result = ops.next(config);
            responseObserver.onNext(result);
            responseObserver.onCompleted();
        } catch (final Throwable t) {
            responseObserver.onError(Status.UNKNOWN
                    .withDescription(t.getMessage())
                    .withCause(t)
                    .asRuntimeException());
        }
    }

    @Override
    public void closeConnection(final Empty empty, final StreamObserver<Empty> responseObserver) {
        responseObserver.onNext(empty);
        responseObserver.onCompleted();
    }

    @Override
    public void closeStatement(final Empty empty, final StreamObserver<Empty> responseObserver) {
        responseObserver.onNext(empty);
        responseObserver.onCompleted();
    }

    @Override
    public void closeResultSet(final NextConfig config, final StreamObserver<Empty> responseObserver) {
        try {
            final Optional<DatabaseOperation> opsOptional = getDatabaseOperationByTransaction(config.getTransaction());

            if (opsOptional.isPresent()) {
                final DatabaseOperation ops = opsOptional
                        .orElseThrow(() -> new IllegalArgumentException("Transaction not found"));
                ops.closeResultSet(config);
            }

            responseObserver.onNext(Empty.newBuilder().build());
            responseObserver.onCompleted();
        } catch (final Throwable t) {
            responseObserver.onError(Status.UNKNOWN
                    .withDescription(t.getMessage())
                    .withCause(t)
                    .asRuntimeException());
        }
    }

    private Optional<DatabaseOperation> getDatabaseOperationByTransaction(final Transaction transaction) {
        return Optional.ofNullable(transaction)
                .filter(it -> Objects.equals(it.getNode(), node))
                .map(it -> transactionMap.get(it.getId()));
    }

    private void deleteDatabaseOperationByTransaction(final Transaction transaction) {
        Optional.ofNullable(transactionMap.get(transaction.getId()))
                .ifPresent(it -> {
                    it.getShouldContinue().set(false);
                    transactionMap.remove(transaction.getId());
                });
    }
}

@FunctionalInterface
interface DoWithConnection {
    void doWithConnection(Params params);

    @Getter
    @Builder
    class Params {
        private final Connection connection;
        private final ExecutorService taskExecutor;
        private final AtomicBoolean shouldContinue;
    }
}

@FunctionalInterface
interface DoWithResultSet {
    void doWithResultSet(Params params);

    @Getter
    @Builder
    class Params {
        private final ResultSet rs;
        private final ExecutorService taskExecutor;
        private final AtomicBoolean shouldContinue;
    }
}

@Slf4j
@Builder
class DatabaseOperation {
    @Getter
    private final AtomicBoolean shouldContinue = new AtomicBoolean(true);
    private final ConcurrentHashMap<String, LinkedBlockingQueue<DoWithResultSet>> queryTaskMap = new ConcurrentHashMap<>();
    private final LinkedBlockingQueue<DoWithConnection> taskQueue = new LinkedBlockingQueue<>() {
        @Override
        public boolean add(DoWithConnection doWithConnection) {
            if (!shouldContinue.get()) {
                return false;
            }
            return super.add(doWithConnection);
        }
    };
    private final UniqueIdGenerator uniqueIdGenerator;
    private final String connectionString;
    private final SQLFormatter sqlFormatter;
    @Getter
    private Transaction transaction;

    boolean openConnection() {
        final ExecutorService taskExecutor = Executors.newFixedThreadPool(5);
        final CompletableFuture<Boolean> future = new CompletableFuture<>();
        CompletableFuture.runAsync(() -> {
            MDC.put("transaction.id", getMaskedId(transaction.getId()));
            try (Connection connection = DriverManager.getConnection(connectionString)) {
                final boolean opened = !connection.isClosed();
                log.debug("openConnection() -> {}", opened);
                future.complete(opened);

                final DoWithConnection.Params params = DoWithConnection.Params.builder()
                        .connection(connection)
                        .taskExecutor(taskExecutor)
                        .shouldContinue(shouldContinue)
                        .build();

                while (params.getShouldContinue().get()) {
                    final DoWithConnection callback = taskQueue.poll(30, TimeUnit.SECONDS);
                    if (callback != null) {
                        log.debug("before doWithConnection()");
                        callback.doWithConnection(params);
                        log.debug("after doWithConnection(), shouldContinue -> {}", params.getShouldContinue().get());
                    }
                }
            } catch (final Throwable t) {
                log.error(t.getMessage(), t);
                future.completeExceptionally(t);
            } finally {
                MDC.clear();
                taskExecutor.shutdownNow();
            }
        }, taskExecutor);
        return future.join();
    }

    boolean closeConnection() {
        final CompletableFuture<Boolean> future = new CompletableFuture<>();
        boolean accepted = taskQueue.add(params -> {
            try {
                log.debug("closeConnection()");
                params.getShouldContinue().set(false);
                future.complete(true);
            } catch (final Throwable t) {
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

    boolean beginTransaction(final BeginTransactionConfig config) {
        final CompletableFuture<Boolean> future = new CompletableFuture<>();
        boolean accepted = taskQueue.add(params -> {
            try {
                params.getConnection().setAutoCommit(false);
                params.getConnection().setReadOnly(config.getReadOnly());

                final CompletableFuture<Void> rollbackTask = CompletableFuture.runAsync(() -> {
                    try {
                        TimeUnit.MILLISECONDS.sleep(config.getTimeout());
                        log.debug("transaction timed out, rolling back");
                        taskQueue.add(params1 -> {
                            final boolean rolledBack = Optional.ofNullable(params1.getConnection())
                                    .filter(this::isActive)
                                    .flatMap(this::rollback)
                                    .orElse(false);
                            log.debug("rolledBack -> {}", rolledBack);
                            params1.getShouldContinue().set(false);
                        });
                    } catch (final InterruptedException e) {
                        log.debug("automatic rollback task interrupted");
                    }
                }, params.getTaskExecutor());

                CompletableFuture.runAsync(() -> {
                    while (params.getShouldContinue().get()) {
                        sleepUninterruptibly(Duration.ofMillis(500));
                    }
                    rollbackTask.cancel(true);
                }, params.getTaskExecutor());

                future.complete(true);
            } catch (final Throwable t) {
                log.error(t.getMessage(), t);
                future.completeExceptionally(t);
            }
        });
        log.debug("begin transaction task accepted -> {}", accepted);
        if (accepted) {
            return future.join();
        } else {
            return false;
        }
    }

    boolean commitTransaction() {
        final CompletableFuture<Boolean> future = new CompletableFuture<>();
        boolean accepted = taskQueue.add(params -> {
            try {
                final boolean committed = Optional.ofNullable(params.getConnection())
                        .filter(this::isActive)
                        .flatMap(this::commit)
                        .orElse(false);
                log.debug("committed -> {}", committed);
                params.getShouldContinue().set(false);
                future.complete(committed);
            } catch (final Throwable t) {
                log.error(t.getMessage(), t);
                future.completeExceptionally(t);
            }
        });
        log.debug("commit task accepted -> {}", accepted);
        if (accepted) {
            return future.join();
        } else {
            return false;
        }
    }

    boolean rollbackTransaction() {
        final CompletableFuture<Boolean> future = new CompletableFuture<>();
        boolean accepted = taskQueue.add(params -> {
            try {
                final boolean rolledBack = Optional.ofNullable(params.getConnection())
                        .filter(this::isActive)
                        .flatMap(this::rollback)
                        .orElse(false);
                log.debug("rolledBack -> {}", rolledBack);
                params.getShouldContinue().set(false);
                future.complete(rolledBack);
            } catch (final Throwable t) {
                log.error(t.getMessage(), t);
                future.completeExceptionally(t);
            }
        });
        log.debug("rollback task accepted -> {}", accepted);
        if (accepted) {
            return future.join();
        } else {
            return false;
        }
    }

    public ExecuteResult execute(final ExecuteConfig config) {
        final CompletableFuture<Integer> future = new CompletableFuture<>();
        boolean accepted = taskQueue.add(params -> {
            try (final PreparedStatement stmt = params.getConnection().prepareStatement(config.getQuery())) {
                stmt.setQueryTimeout(getQueryTimeout(config.getTimeout()));

                IntStream.range(0, config.getArgsCount())
                        .forEach(i -> setSqlArg(stmt, i + 1, config.getArgs(i)));

                logQuery(config.getQuery());

                future.complete(stmt.executeUpdate());
            } catch (final Throwable t) {
                log.error(t.getMessage(), t);
                future.completeExceptionally(t);
            }
        });
        log.debug("execute task accepted -> {}", accepted);
        if (accepted) {
            return ExecuteResult.newBuilder()
                    .setRowsAffected(future.join())
                    .build();
        } else {
            return ExecuteResult.newBuilder()
                    .build();
        }
    }

    public QueryResult query(final QueryConfig config) {
        final String queryResultId = uniqueIdGenerator.generate(QueryResult.class);
        final CompletableFuture<Boolean> future = new CompletableFuture<>();
        final boolean accepted = taskQueue.add(params -> {
            MDC.put("query.id", getMaskedId(queryResultId));
            log.debug("before prepare statement");
            try (final PreparedStatement stmt = params.getConnection().prepareStatement(config.getQuery())) {
                stmt.setFetchSize(getFetchSize(config.getFetchSize()));
                stmt.setQueryTimeout(getQueryTimeout(config.getTimeout()));

                IntStream.range(0, config.getArgsCount())
                        .forEach(i -> setSqlArg(stmt, i + 1, config.getArgs(i)));

                logQuery(config.getQuery());

                final AtomicBoolean shouldContinueResultSet = new AtomicBoolean(true);
                final LinkedBlockingQueue<DoWithResultSet> taskQueueResultSet = new LinkedBlockingQueue<>() {
                    @Override
                    public boolean add(DoWithResultSet doWithResultSet) {
                        if (!params.getShouldContinue().get() || !shouldContinueResultSet.get()) {
                            return false;
                        }
                        return super.add(doWithResultSet);
                    }
                };

                log.debug("before open result set");
                try (final ResultSet rs = stmt.executeQuery()) {
                    queryTaskMap.put(queryResultId, taskQueueResultSet);
                    future.complete(true);

                    final DoWithResultSet.Params resultSetParams = DoWithResultSet.Params.builder()
                            .rs(rs)
                            .taskExecutor(params.getTaskExecutor())
                            .shouldContinue(shouldContinueResultSet)
                            .build();

                    while (params.getShouldContinue().get() && shouldContinueResultSet.get()) {
                        final DoWithResultSet callback = taskQueueResultSet.poll(30, TimeUnit.SECONDS);
                        if (callback != null) {
                            log.debug("before doWithResultSet()");
                            callback.doWithResultSet(resultSetParams);
                            log.debug("after doWithResultSet(), shouldContinue -> {}, shouldContinueResultSet -> {}", params.getShouldContinue().get(), shouldContinueResultSet.get());
                        }
                    }
                }
                log.debug("after close result set");
            } catch (final Throwable t) {
                log.error(t.getMessage(), t);
                future.completeExceptionally(t);
            } finally {
                log.debug("after prepare statement");
                MDC.remove("query.id");
                queryTaskMap.remove(queryResultId);
            }
        });
        log.debug("query task accepted -> {}", accepted);
        if (accepted) {
            future.join();
        }
        return QueryResult.newBuilder()
                .setId(queryResultId)
                .build();
    }

    public QueryResult next(final NextConfig config) {
        final CompletableFuture<List<Row>> future = new CompletableFuture<>();
        final boolean accepted = queryTaskMap.get(config.getQueryResultId()).add(params -> {
            try {
                boolean next = true;
                int rowsFetched = 0;
                final List<Row> results = new ArrayList<>();
                while (params.getShouldContinue().get() && next && rowsFetched < params.getRs().getFetchSize()) {
                    next = params.getRs().next();
                    if (next) {
                        rowsFetched++;
                        results.add(Row.newBuilder()
                                .addAllCols(IntStream.range(1, params.getRs().getMetaData().getColumnCount() + 1)
                                        .mapToObj(i -> getSqlArg(params.getRs(), i))
                                        .toList()
                                )
                                .build());
                    }
                }
                if (!next) {
                    params.getShouldContinue().set(false);
                }
                future.complete(results);
            } catch (final Throwable t) {
                log.error(t.getMessage(), t);
                future.completeExceptionally(t);
            }
        });
        log.debug("next task accepted -> {}", accepted);
        if (accepted) {
            return QueryResult.newBuilder()
                    .setId(config.getQueryResultId())
                    .addAllRows(future.join())
                    .build();
        } else {
            return QueryResult.newBuilder()
                    .setId(config.getQueryResultId())
                    .build();
        }
    }

    public boolean closeResultSet(final NextConfig config) {
        final CompletableFuture<Boolean> future = new CompletableFuture<>();
        final boolean accepted = queryTaskMap.get(config.getQueryResultId()).add(params -> {
            try {
                params.getShouldContinue().set(false);
                future.complete(true);
            } catch (Throwable t) {
                log.error(t.getMessage(), t);
                future.completeExceptionally(t);
            }
        });
        log.debug("close result set task accepted -> {}", accepted);
        if (accepted) {
            return future.join();
        } else {
            return false;
        }
    }

    boolean isActive(final Connection conn) {
        return Optional.ofNullable(conn).stream()
                .anyMatch(it -> {
                    try {
                        return !it.isClosed();
                    } catch (final SQLException e) {
                        return false;
                    }
                });
    }

    Optional<Boolean> commit(final Connection conn) {
        return Optional.ofNullable(conn)
                .map(it -> {
                    try {
                        it.commit();
                        return true;
                    } catch (final SQLException e) {
                        log.error(e.getMessage(), e);
                        return false;
                    }
                });
    }

    Optional<Boolean> rollback(final Connection conn) {
        return Optional.ofNullable(conn)
                .map(it -> {
                    try {
                        it.rollback();
                        return true;
                    } catch (final SQLException e) {
                        log.error(e.getMessage(), e);
                        return false;
                    }
                });
    }

    private Value getSqlArg(final ResultSet rs, final int i) {
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
                case BOOLEAN, BIT -> {
                    return Value.newBuilder()
                            .setCode(ValueCode.BOOL)
                            .setData(ValueBool.newBuilder()
                                    .setValue(rs.getBoolean(i))
                                    .build()
                                    .toByteString()
                            )
                            .build();

                }
                case DATE -> {
                    return Value.newBuilder()
                            .setCode(ValueCode.TIME)
                            .setData(Optional.ofNullable(rs.getDate(i))
                                    .map(it -> OffsetDateTime.ofInstant(it.toInstant(), ZoneId.systemDefault()))
                                    .map(it -> ValueTime.newBuilder().setValue(it.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME)))
                                    .orElseGet(ValueTime::newBuilder)
                                    .build()
                                    .toByteString()
                            )
                            .build();
                }
                case NUMERIC, DOUBLE -> {
                    return Value.newBuilder()
                            .setCode(ValueCode.FLOAT64)
                            .setData(ValueFloat64.newBuilder()
                                    .setValue(rs.getDouble(i))
                                    .build()
                                    .toByteString()
                            )
                            .build();
                }
                case INTEGER -> {
                    return Value.newBuilder()
                            .setCode(ValueCode.INT32)
                            .setData(ValueInt32.newBuilder()
                                    .setValue(rs.getInt(i))
                                    .build()
                                    .toByteString()
                            )
                            .build();
                }
                case TIMESTAMP -> {
                    return Value.newBuilder()
                            .setCode(ValueCode.TIME)
                            .setData(Optional.ofNullable(rs.getTimestamp(i))
                                    .map(it -> OffsetDateTime.ofInstant(it.toInstant(), ZoneId.systemDefault()))
                                    .map(it -> ValueTime.newBuilder().setValue(it.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME)))
                                    .orElseGet(ValueTime::newBuilder)
                                    .build()
                                    .toByteString()
                            )
                            .build();
                }
                case VARCHAR -> {
                    return Value.newBuilder()
                            .setCode(ValueCode.STRING)
                            .setData(Optional.ofNullable(rs.getString(i))
                                    .map(it -> ValueString.newBuilder().setValue(it))
                                    .orElseGet(ValueString::newBuilder)
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
        } catch (final SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void setSqlArg(final PreparedStatement stmt, final int i, final Value value) {
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
                    final String s = ValueTime.parseFrom(value.getData()).getValue();
                    final OffsetDateTime odt = OffsetDateTime.parse(s, DateTimeFormatter.ISO_OFFSET_DATE_TIME);
                    stmt.setTimestamp(i, new Timestamp(odt.toInstant().toEpochMilli()));
                }
                case NULL -> {
                    stmt.setNull(i, Types.NULL);
                }
                default -> {
                    stmt.setNull(i, Types.NULL);
                }
            }
        } catch (final InvalidProtocolBufferException | SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private String getMaskedId(final String id) {
        return id.replaceFirst("^(.{32}).*$", "$1");
    }

    private int getFetchSize(final long fetchSize) {
        return (int) Math.min(Math.max(fetchSize, 25), Integer.MAX_VALUE);
    }

    private int getQueryTimeout(final long timeout) {
        return (int) Math.min(timeout, Integer.MAX_VALUE);
    }

    private void logQuery(final String query) {
        log.debug("{}", Objects.toString(sqlFormatter.prettyPrint(query)).trim());
    }
}
