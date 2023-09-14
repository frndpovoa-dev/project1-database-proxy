package dev.frndpovoa.project1.databaseproxy.service;

import dev.frndpovoa.project1.databaseproxy.BaseIntTest;
import dev.frndpovoa.project1.databaseproxy.config.GrpcProperties;
import dev.frndpovoa.project1.databaseproxy.proto.*;
import io.grpc.ManagedChannelBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.time.Duration;
import java.util.List;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static org.assertj.core.api.Assertions.assertThat;

class DatabaseProxyServiceIntTest extends BaseIntTest {
    private static final List<Value> ARGS_ID_1 = List.of(Value.newBuilder()
            .setCode(ValueCode.INT64)
            .setData(ValueInt64.newBuilder().setValue(1L).build().toByteString())
            .build());
    private static final List<Value> ARGS_ID_2 = List.of(Value.newBuilder()
            .setCode(ValueCode.INT64)
            .setData(ValueInt64.newBuilder().setValue(2L).build().toByteString())
            .build());
    private static final List<Row> RESULTS_NAME_DUMMY = List.of(Row.newBuilder()
            .addCols(Value.newBuilder()
                    .setCode(ValueCode.STRING)
                    .setData(ValueString.newBuilder().setValue("dummy").build().toByteString())
                    .build()
            )
            .build());
    private static final List<Row> RESULTS_NAME_FOOBAR = List.of(Row.newBuilder()
            .addCols(Value.newBuilder()
                    .setCode(ValueCode.STRING)
                    .setData(ValueString.newBuilder().setValue("foobar").build().toByteString())
                    .build()
            )
            .build());
    private static final String SELECT_NAME_FROM_TEST_WHERE_ID = """
            select name from test
            where id = ?;
            """;
    private static final String INSERT_INTO_TEST_ID_NAME_VALUES_1_DUMMY = """
            insert into test (
              id,
              name
            ) values (
              1,
              'dummy'
            );
            """;
    private static final String INSERT_INTO_TEST_ID_NAME_VALUES_2_FOOBAR = """
            insert into test (
              id,
              name
            ) values (
              2,
              'foobar'
            );
            """;
    private static final String CREATE_TABLE_TEST = """
            create table test (
              id int primary key,
              name varchar
            ) with "ATOMICITY=TRANSACTIONAL_SNAPSHOT";
            """;
    public static final String DROP_TABLE_IF_EXISTS_TEST = """
            drop table if exists test;
            """;

    @Autowired
    private GrpcProperties grpcProperties;
    private DatabaseProxyGrpc.DatabaseProxyBlockingStub databaseProxyServiceClient;

    @BeforeEach
    void setUp() throws Exception {
        this.databaseProxyServiceClient = DatabaseProxyGrpc.newBlockingStub(ManagedChannelBuilder
                .forAddress("localhost", grpcProperties.getPort())
                .usePlaintext()
                .build());

        ddl(DROP_TABLE_IF_EXISTS_TEST);
        ddl(CREATE_TABLE_TEST);
    }

    @Test
    void givenCreateTable_thenInsert_thenSelect() {
        Transaction tx1 = beginTransaction(1_000);
        Transaction tx2 = beginTransaction(1_000);

        executeTx(tx1, 1, INSERT_INTO_TEST_ID_NAME_VALUES_1_DUMMY);
        queryTx(tx1, 1, SELECT_NAME_FROM_TEST_WHERE_ID, ARGS_ID_1, RESULTS_NAME_DUMMY);
        queryTx(tx2, 0, SELECT_NAME_FROM_TEST_WHERE_ID, ARGS_ID_1, null);
        query(0, SELECT_NAME_FROM_TEST_WHERE_ID, ARGS_ID_1, null);

        commit(tx1, Transaction.Status.COMMITTED);

        executeTx(tx2, 1, INSERT_INTO_TEST_ID_NAME_VALUES_2_FOOBAR);
        queryTx(tx2, 1, SELECT_NAME_FROM_TEST_WHERE_ID, ARGS_ID_2, RESULTS_NAME_FOOBAR);
        queryTx(tx2, 0, SELECT_NAME_FROM_TEST_WHERE_ID, ARGS_ID_1, null);
        query(1, SELECT_NAME_FROM_TEST_WHERE_ID, ARGS_ID_1, RESULTS_NAME_DUMMY);

        rollback(tx2);

        Transaction tx3 = beginTransaction(1_000);
        queryTx(tx3, 1, SELECT_NAME_FROM_TEST_WHERE_ID, ARGS_ID_1, RESULTS_NAME_DUMMY);
        queryTx(tx3, 0, SELECT_NAME_FROM_TEST_WHERE_ID, ARGS_ID_2, null);

        commit(tx3, Transaction.Status.COMMITTED);
    }

    @Test
    void givenCreateTable_thenInsert_thenTransactionTimeout() {
        Transaction tx1 = beginTransaction(100);
        executeTx(tx1, 1, INSERT_INTO_TEST_ID_NAME_VALUES_1_DUMMY);

        sleepUninterruptibly(Duration.ofMillis(1_000));

        tx1 = commit(tx1, Transaction.Status.UNKNOWN);

        queryTx(tx1, 0, SELECT_NAME_FROM_TEST_WHERE_ID, ARGS_ID_1, null);
    }

    private Transaction beginTransaction(
            int timeout
    ) {
        Transaction transaction = databaseProxyServiceClient.beginTransaction(BeginTransactionConfig.newBuilder()
                .setTimeout(timeout)
                .build());
        assertThat(transaction)
                .isNotNull()
                .hasFieldOrProperty("id")
                .hasFieldOrPropertyWithValue("status", Transaction.Status.ACTIVE);
        return transaction;
    }

    private void ddl(
            String sql
    ) {
        DdlResult ddlResult = databaseProxyServiceClient.ddl(DdlConfig.newBuilder()
                .setTimeout(100)
                .setQuery(sql)
                .build());
        assertThat(ddlResult)
                .isNotNull();
    }

    private void executeTx(
            Transaction transaction,
            int rowsAffected,
            String sql
    ) {
        ExecuteResult insertResult = databaseProxyServiceClient.executeTx(ExecuteTxConfig.newBuilder()
                .setTransaction(transaction)
                .setExecuteConfig(ExecuteConfig.newBuilder()
                        .setTimeout(100)
                        .setQuery(sql)
                        .build())
                .build());
        assertThat(insertResult)
                .isNotNull()
                .hasFieldOrPropertyWithValue("rowsAffected", rowsAffected);
    }

    private Transaction commit(
            Transaction transaction,
            Transaction.Status expectedStatus
    ) {
        transaction = databaseProxyServiceClient.commitTransaction(transaction);
        assertThat(transaction)
                .isNotNull()
                .hasFieldOrPropertyWithValue("status", expectedStatus);
        return transaction;
    }

    private Transaction rollback(
            Transaction transaction
    ) {
        transaction = databaseProxyServiceClient.rollbackTransaction(transaction);
        assertThat(transaction)
                .isNotNull()
                .hasFieldOrPropertyWithValue("status", Transaction.Status.ROLLED_BACK);
        return transaction;
    }

    private void queryTx(
            Transaction transaction,
            int rowsReturned,
            String sql,
            List<Value> args,
            List<Row> expectedResults
    ) {
        QueryResult queryResult = databaseProxyServiceClient.queryTx(QueryTxConfig.newBuilder()
                .setTransaction(transaction)
                .setQueryConfig(QueryConfig.newBuilder()
                        .setTimeout(1_000)
                        .setQuery(sql)
                        .addAllArgs(args)
                        .build())
                .build());
        assertThat(queryResult)
                .isNotNull();
        if (rowsReturned > 0) {
            assertThat(queryResult.getRowsList())
                    .isNotNull()
                    .hasSize(rowsReturned)
                    .hasToString(expectedResults.toString());
        } else {
            assertThat(queryResult.getRowsList())
                    .isEmpty();
        }
    }

    private void query(
            int rowsReturned,
            String sql,
            List<Value> args,
            List<Row> expectedResults
    ) {
        QueryResult queryResult = databaseProxyServiceClient.query(QueryConfig.newBuilder()
                .setTimeout(1_000)
                .setQuery(sql)
                .addAllArgs(args)
                .build());
        assertThat(queryResult)
                .isNotNull();
        if (rowsReturned > 0) {
            assertThat(queryResult.getRowsList())
                    .isNotNull()
                    .hasSize(rowsReturned)
                    .hasToString(expectedResults.toString());
        } else {
            assertThat(queryResult.getRowsList())
                    .isEmpty();
        }
    }
}
