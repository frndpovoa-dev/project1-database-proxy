package dev.frndpovoa.project1.databaseproxy.spring;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

@Slf4j
@SpringBootTest
@EnableConfigurationProperties
@ActiveProfiles({"integration"})
public abstract class BaseIntTest {
}
