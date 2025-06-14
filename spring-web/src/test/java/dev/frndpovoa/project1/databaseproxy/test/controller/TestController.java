package dev.frndpovoa.project1.databaseproxy.test.controller;

import dev.frndpovoa.project1.databaseproxy.ConnectionHolder;
import dev.frndpovoa.project1.databaseproxy.test.bo.TestBo;
import dev.frndpovoa.project1.databaseproxy.test.repository.TestRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@Slf4j
@RestController
@RequestMapping(path = "/api/v1/test")
@Transactional
@RequiredArgsConstructor
public class TestController {
    private final TestRepository repository;

    @GetMapping(path = "/list")
    public List<TestBo> list(
            @RequestHeader("X-Transaction-Id") final String transactionId
    ) throws Exception {
        log.debug("List using transactionId({})", transactionId);
        ConnectionHolder.getConnection().joinSharedTransaction(transactionId);
        return repository.findAll();
    }

    @PostMapping(path = "/insert")
    public TestBo step2(
            @RequestHeader("X-Transaction-Id") final String transactionId,
            @RequestBody final TestBo test
    ) throws Exception {
        log.debug("Insert using transactionId({})", transactionId);
        ConnectionHolder.getConnection().joinSharedTransaction(transactionId);
        repository.save(test);
        return test;
    }
}
