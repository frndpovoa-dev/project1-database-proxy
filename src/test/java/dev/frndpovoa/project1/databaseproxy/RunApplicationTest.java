package dev.frndpovoa.project1.databaseproxy;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import java.time.Duration;

import static org.awaitility.Awaitility.await;

@Slf4j
@SpringBootTest
@ExtendWith({IgniteExtension.class})
@ActiveProfiles({"integration"})
class RunApplicationTest {
    @Test
    @EnabledIfEnvironmentVariable(named = "running.from.local.environment", matches = ".+")
    void run() {
        log.info("App is running in testing mode");
        await()
                .pollInterval(Duration.ofHours(1))
                .atMost(Duration.ofDays(1))
                .until(() -> false);
    }
}
