package dev.frndpovoa.project1.databaseproxy.service;

import com.google.protobuf.InvalidProtocolBufferException;
import dev.frndpovoa.project1.databaseproxy.config.IgniteProperties;
import dev.frndpovoa.project1.databaseproxy.proto.*;
import io.grpc.stub.StreamObserver;
import lombok.Builder;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.sql.*;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;

@Slf4j
@Service
@RequiredArgsConstructor
public class DatabaseProxyService extends DatabaseProxyGrpc.DatabaseProxyImplBase {
    private final UniqueIdGenerator uniqueIdGenerator;
    private final IgniteProperties igniteProperties;
    private final Map<String, TransactionalOperation> transactionalOperationMap = new ConcurrentHashMap<>();

    @Override
    public void beginTransaction(BeginTransactionConfig request, StreamObserver<Transaction> responseObserver) {
        try {
            Transaction transaction = Transaction.newBuilder()
                    .setId(uniqueIdGenerator.generate(DatabaseProxyService.class.getName()))
                    .setStatus(Transaction.Status.ACTIVE)
                    .build();

            transactionalOperationMap.put(transaction.getId(), TransactionalOperation.builder()
                    .igniteProperties(igniteProperties)
                    .transaction(transaction)
                    .build()
                    .beginTransaction(request));

            responseObserver.onNext(transaction);
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void commitTransaction(Transaction transaction, StreamObserver<Transaction> responseObserver) {
        try {
            transactionalOperationMap.get(transaction.getId())
                    .commitTransaction();

            transactionalOperationMap.remove(transaction.getId());

            transaction = transaction.toBuilder()
                    .setStatus(Transaction.Status.COMMITTED)
                    .build();

            responseObserver.onNext(transaction);
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void rollbackTransaction(Transaction transaction, StreamObserver<Transaction> responseObserver) {
        try {
            transactionalOperationMap.get(transaction.getId())
                    .rollbackTransaction();

            transactionalOperationMap.remove(transaction.getId());

            transaction = transaction.toBuilder()
                    .setStatus(Transaction.Status.ROLLED_BACK)
                    .build();

            responseObserver.onNext(transaction);
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void execute(ExecuteConfig config, StreamObserver<ExecuteResult> responseObserver) {
        try {
            ExecuteResult result = transactionalOperationMap.get(config.getTransaction().getId())
                    .execute(config);
            responseObserver.onNext(result);
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void query(QueryConfig config, StreamObserver<QueryResult> responseObserver) {
        try {
            QueryResult result = transactionalOperationMap.get(config.getTransaction().getId())
                    .query(config);
            responseObserver.onNext(result);
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(e);
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
}

@FunctionalInterface
interface DoInTransaction {
    void doInTransaction(Connection conn, AtomicBoolean shouldContinue) throws SQLException;
}

@Slf4j
@Builder
class TransactionalOperation {
    private final AtomicBoolean shouldContinue = new AtomicBoolean(true);
    private final Queue<DoInTransaction> taskQueue = new ConcurrentLinkedQueue<>();
    private IgniteProperties igniteProperties;
    private Transaction transaction;

    TransactionalOperation beginTransaction(BeginTransactionConfig config) {
        CompletableFuture.runAsync(() -> {
            try (Connection conn = DriverManager.getConnection(igniteProperties.getUrl())) {
                while (shouldContinue.get()) {
                    while (!taskQueue.isEmpty()) {
                        DoInTransaction callback = taskQueue.poll();
                        callback.doInTransaction(conn, shouldContinue);
                    }
                    sleepUninterruptibly(Duration.ofSeconds(1));
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });
        return this;
    }

    void commitTransaction() {
        CompletableFuture<Void> future = new CompletableFuture<>();
        taskQueue.add((conn, shouldContinue) -> {
            conn.commit();
            shouldContinue.set(false);
            future.complete(null);
        });
        future.join();
    }

    void rollbackTransaction() {
        CompletableFuture<Void> future = new CompletableFuture<>();
        taskQueue.add((conn, shouldContinue) -> {
            conn.rollback();
            shouldContinue.set(false);
            future.complete(null);
        });
        future.join();
    }

    public ExecuteResult execute(ExecuteConfig config) {
        CompletableFuture<Integer> future = new CompletableFuture<>();
        taskQueue.add((conn, shouldContinue) -> {
            try (PreparedStatement stmt = conn.prepareStatement(config.getQuery())) {
                IntStream.range(0, config.getArgsCount())
                        .forEach(i -> setSqlArg(stmt, i + 1, config.getArgs(i)));
                future.complete(stmt.executeUpdate());
            }
        });
        return ExecuteResult.newBuilder()
                .setRowsAffected(future.join())
                .build();
    }

    public QueryResult query(QueryConfig config) {
        CompletableFuture<List<Row>> future = new CompletableFuture<>();
        taskQueue.add((conn, shouldContinue) -> {
            try (PreparedStatement stmt = conn.prepareStatement(config.getQuery())) {
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
            }
        });
        return QueryResult.newBuilder()
                .addAllRows(future.join())
                .build();
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
