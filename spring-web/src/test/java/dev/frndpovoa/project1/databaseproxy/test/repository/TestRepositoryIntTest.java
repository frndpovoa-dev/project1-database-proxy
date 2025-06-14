package dev.frndpovoa.project1.databaseproxy.test.repository;

import dev.frndpovoa.project1.databaseproxy.ConnectionHolder;
import dev.frndpovoa.project1.databaseproxy.proto.Transaction;
import dev.frndpovoa.project1.databaseproxy.test.BaseIntTest;
import dev.frndpovoa.project1.databaseproxy.test.bo.TestBo;
import jakarta.transaction.Transactional;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.test.context.jdbc.Sql;
import org.springframework.test.context.jdbc.SqlConfig;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestTemplate;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@Transactional
class TestRepositoryIntTest extends BaseIntTest {
    @Autowired
    private TestRepository repository;
    @Autowired
    private RestTemplate restTemplate;

    public static final TestBo TEST_1 = TestBo.builder()
            .id(1L)
            .name("Hello World! 1")
            .build();
    public static final TestBo TEST_2 = TestBo.builder()
            .id(2L)
            .name("Hello World! 2")
            .build();

    @Test
    @Sql(value = "classpath:dev.frndpovoa.project1.databaseproxy.test.repository/TestRepositoryIntTest.sql", config = @SqlConfig(dataSource = "dataSource"))
    void testJpaUsingSharedTransaction() {
        final Transaction transaction = ConnectionHolder.getConnection().getTransaction();
        final String transactionId = transaction.getId() + "@" + transaction.getNode();
        log.debug("Tx transactionId({})", transactionId);

        log.debug("Read before insert using JPA");
        log.debug("{}", repository.findAll());

        log.debug("Insert");
        log.debug("{}", repository.saveAndFlush(TEST_2));

        log.debug("Read after insert using JPA");
        log.debug("{}", repository.findAll());

        log.debug("List after insert using API");
        log.debug("{}", restTemplate.exchange("http://localhost:8080/api/v1/test/list", HttpMethod.GET, new HttpEntity<>(
                        MultiValueMap.fromSingleValue(Map.of("X-Transaction-Id", transactionId))), new ParameterizedTypeReference<List<TestBo>>() {
                }).getBody());

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
