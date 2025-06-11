package dev.frndpovoa.project1.databaseproxy.spring;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

@Slf4j
@SpringBootTest
@ActiveProfiles({"integration"})
public abstract class BaseIntTest {
}
