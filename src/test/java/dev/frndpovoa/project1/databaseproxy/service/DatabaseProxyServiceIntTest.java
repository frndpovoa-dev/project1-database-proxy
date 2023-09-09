package dev.frndpovoa.project1.databaseproxy.service;

import dev.frndpovoa.project1.databaseproxy.BaseIntTest;
import dev.frndpovoa.project1.databaseproxy.config.GrpcProperties;
import dev.frndpovoa.project1.databaseproxy.proto.*;
import io.grpc.ManagedChannelBuilder;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;

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

    @Autowired
    private GrpcProperties grpcProperties;
    private DatabaseProxyGrpc.DatabaseProxyBlockingStub databaseProxyServiceClient;

    @BeforeEach
    void setUp() throws Exception {
        this.databaseProxyServiceClient = DatabaseProxyGrpc.newBlockingStub(ManagedChannelBuilder
                .forAddress("localhost", grpcProperties.getPort())
                .usePlaintext()
                .build());
    }

    @Test
    void createTable() {
        ddl(CREATE_TABLE_TEST);

        Transaction tx1 = beginTransaction();
        Transaction tx2 = beginTransaction();

        execute(tx1, 1, INSERT_INTO_TEST_ID_NAME_VALUES_1_DUMMY);
        query(tx1, 1, SELECT_NAME_FROM_TEST_WHERE_ID, ARGS_ID_1, RESULTS_NAME_DUMMY);
        query(tx2, 0, SELECT_NAME_FROM_TEST_WHERE_ID, ARGS_ID_1, null);

        commit(tx1);

        execute(tx2, 1, INSERT_INTO_TEST_ID_NAME_VALUES_2_FOOBAR);
        query(tx2, 1, SELECT_NAME_FROM_TEST_WHERE_ID, ARGS_ID_2, RESULTS_NAME_FOOBAR);
        query(tx2, 0, SELECT_NAME_FROM_TEST_WHERE_ID, ARGS_ID_1, null);

        rollback(tx2);

        Transaction tx3 = beginTransaction();
        query(tx3, 1, SELECT_NAME_FROM_TEST_WHERE_ID, ARGS_ID_1, RESULTS_NAME_DUMMY);
        query(tx3, 0, SELECT_NAME_FROM_TEST_WHERE_ID, ARGS_ID_2, null);

        commit(tx3);
    }

    private Transaction beginTransaction() {
        Transaction transaction = databaseProxyServiceClient.beginTransaction(BeginTransactionConfig.newBuilder()
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

    private void execute(
            Transaction transaction,
            int rowsAffected,
            String sql
    ) {
        ExecuteResult insertResult = databaseProxyServiceClient.execute(ExecuteConfig.newBuilder()
                .setTimeout(100)
                .setTransaction(transaction)
                .setQuery(sql)
                .build());
        assertThat(insertResult)
                .isNotNull()
                .hasFieldOrPropertyWithValue("rowsAffected", rowsAffected);
    }

    private void commit(
            Transaction transaction
    ) {
        transaction = databaseProxyServiceClient.commitTransaction(transaction);
        assertThat(transaction)
                .isNotNull()
                .hasFieldOrPropertyWithValue("status", Transaction.Status.COMMITTED);
    }

    private void rollback(
            Transaction transaction
    ) {
        transaction = databaseProxyServiceClient.rollbackTransaction(transaction);
        assertThat(transaction)
                .isNotNull()
                .hasFieldOrPropertyWithValue("status", Transaction.Status.ROLLED_BACK);
    }

    private void query(
            Transaction transaction,
            int rowsReturned,
            String sql,
            List<Value> args,
            List<Row> expectedResults
    ) {
        QueryResult queryResult = databaseProxyServiceClient.query(QueryConfig.newBuilder()
                .setTimeout(100)
                .setTransaction(transaction)
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
