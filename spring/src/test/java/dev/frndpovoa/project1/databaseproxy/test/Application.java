package dev.frndpovoa.project1.databaseproxy.test;

import dev.frndpovoa.project1.databaseproxy.config.DatabaseProxyDataSourceProperties;
import dev.frndpovoa.project1.databaseproxy.config.DatabaseProxyProperties;
import dev.frndpovoa.project1.databaseproxy.test.config.TestConfig;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.info.BuildProperties;
import org.springframework.context.annotation.Import;
import org.springframework.context.event.EventListener;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import java.util.Optional;

@Slf4j
@RequiredArgsConstructor
@EnableTransactionManagement
@EnableConfigurationProperties({
        DatabaseProxyProperties.class,
        DatabaseProxyDataSourceProperties.class
})
@EnableJpaRepositories(
        basePackageClasses = {
                dev.frndpovoa.project1.databaseproxy.test.repository.Package.class,
        }
)
@SpringBootApplication(
        scanBasePackageClasses = {
                dev.frndpovoa.project1.databaseproxy.test.Package.class,
                dev.frndpovoa.project1.databaseproxy.test.config.Package.class,
        },
        exclude = {
                DataSourceAutoConfiguration.class,
        }
)
@Import(TestConfig.class)
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
