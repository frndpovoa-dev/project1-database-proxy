package dev.frndpovoa.project1.databaseproxy.spring.repository;

import dev.frndpovoa.project1.databaseproxy.spring.BaseIntTest;
import dev.frndpovoa.project1.databaseproxy.spring.repository.test.TestBo;
import dev.frndpovoa.project1.databaseproxy.spring.repository.test.TestRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.annotation.Transactional;

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

    @BeforeEach
    void setUp() {
        repository.save(TEST_1);
    }

    @Test
    void testCrud() {
        Optional<TestBo> testBo = repository.findById(1L);
        assertThat(testBo.isPresent())
                .isTrue();
        assertThat(testBo.get())
                .isNotNull()
                .isEqualTo(TEST_1);
    }
}
