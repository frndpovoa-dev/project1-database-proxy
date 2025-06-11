package dev.frndpovoa.project1.databaseproxy.spring.repository;

import dev.frndpovoa.project1.databaseproxy.spring.BaseIntTest;
import dev.frndpovoa.project1.databaseproxy.spring.bo.TestBo;
import jakarta.transaction.Transactional;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

@Transactional
class TestRepositoryIntTest extends BaseIntTest {
    @Autowired
    private TestRepository repository;

    public static final TestBo TEST_1 = TestBo.builder()
            .id(1L)
            .name("test1")
            .build();

    @Test
    void testCrud() {
        repository.saveAndFlush(TEST_1);
        Optional<TestBo> testBo = repository.findById(TEST_1.getId());
        assertThat(testBo.isPresent())
                .isTrue();
        assertThat(testBo.get())
                .isNotNull()
                .isEqualTo(TEST_1);
    }
}
