package dev.frndpovoa.project1.databaseproxy;


import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.boot.info.BuildProperties;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.event.EventListener;

import java.util.Optional;

@Slf4j
@RequiredArgsConstructor
@SpringBootApplication
@ComponentScan(basePackages = {"dev.frndpovoa.project1.databaseproxy"})
@ConfigurationPropertiesScan(basePackages = {"dev.frndpovoa.project1.databaseproxy"})
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
