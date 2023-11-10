package dev.frndpovoa.project1.databaseproxy;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

@Slf4j
@SpringBootTest
@ExtendWith({PostgresExtension.class})
@ActiveProfiles({"integration"})
public abstract class BaseIntTest {
}
