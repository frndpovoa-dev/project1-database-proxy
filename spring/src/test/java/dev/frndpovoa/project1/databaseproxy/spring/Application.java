package dev.frndpovoa.project1.databaseproxy.spring;

import dev.frndpovoa.project1.databaseproxy.spring.config.DatabaseProxyDataSourceProperties;
import dev.frndpovoa.project1.databaseproxy.spring.config.DatabaseProxyProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.info.BuildProperties;
import org.springframework.context.event.EventListener;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import java.util.Optional;

@Slf4j
@RequiredArgsConstructor
@EnableTransactionManagement
@EntityScan(
        basePackages = "dev.frndpovoa.project1.databaseproxy.spring.bo"
)
@EnableConfigurationProperties({
        DatabaseProxyProperties.class,
        DatabaseProxyDataSourceProperties.class
})
@EnableJpaRepositories(
        basePackages = "dev.frndpovoa.project1.databaseproxy.spring.repository"
)
@SpringBootApplication(
        scanBasePackages = "dev.frndpovoa.project1.databaseproxy.spring",
        exclude = {
                DataSourceAutoConfiguration.class
        }
)
public class Application {
    private final Optional<BuildProperties> buildProperties;

    public static void main(String[] args) {
        new SpringApplicationBuilder(Application.class)
                .run(args);
    }

    @EventListener
    public void onReady(ApplicationReadyEvent event) {
        buildProperties.ifPresent(it ->
                log.info("APP_INFO=[name={}, version={}, buildTime={}]", it.getName(), it.getVersion(), it.getTime())
        );
    }
}
