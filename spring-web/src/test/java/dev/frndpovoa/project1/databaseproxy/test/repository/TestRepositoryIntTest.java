package dev.frndpovoa.project1.databaseproxy.test.repository;

import dev.frndpovoa.project1.databaseproxy.test.BaseIntTest;
import dev.frndpovoa.project1.databaseproxy.test.bo.TestBo;
import jakarta.transaction.Transactional;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.jdbc.Sql;
import org.springframework.test.context.jdbc.SqlConfig;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
@Transactional
class TestRepositoryIntTest extends BaseIntTest {
    @Autowired
    private TestRepository repository;

    public static final TestBo TEST_1 = TestBo.builder()
            .id(1L)
            .name("test1")
            .build();
    public static final TestBo TEST_2 = TestBo.builder()
            .id(2L)
            .name("test2")
            .build();

    @Test
    @Sql(value = "classpath:dev.frndpovoa.project1.databaseproxy.test.repository/TestRepositoryIntTest.sql", config = @SqlConfig(dataSource = "dataSource"))
    void testCrud() {
        repository.save(TEST_2);
        repository.flush();

        repository.findAll().forEach(it -> log.info("{}", it));

        Optional<TestBo> test1Bo = repository.findById(TEST_1.getId());
        assertThat(test1Bo.isPresent())
                .isTrue();
        assertThat(test1Bo.get())
                .isNotNull()
                .isEqualTo(TEST_1);

        Optional<TestBo> test2Bo = repository.findById(TEST_2.getId());
        assertThat(test2Bo.isPresent())
                .isTrue();
        assertThat(test2Bo.get())
                .isNotNull()
                .isEqualTo(TEST_2);
    }
}
