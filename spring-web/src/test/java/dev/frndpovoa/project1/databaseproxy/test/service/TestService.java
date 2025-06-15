package dev.frndpovoa.project1.databaseproxy.test.service;

import dev.frndpovoa.project1.databaseproxy.test.bo.TestBo;
import dev.frndpovoa.project1.databaseproxy.test.repository.TestRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

@Service
@RequiredArgsConstructor
public class TestService {
    private final TestRepository repository;

    @Transactional(timeout = 60_000, propagation = Propagation.REQUIRES_NEW)
    public TestBo save(final TestBo testBo) {
        return repository.save(TestBo.builder()
                .id(testBo.getId() + 1)
                .name(testBo.getName() + " from server side")
                .build());
    }
}
