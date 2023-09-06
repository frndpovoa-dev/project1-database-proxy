package dev.frndpovoa.project1.databaseproxy.config;

import org.apache.ignite.configuration.ClientConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration(proxyBeanMethods = false)
public class DatabaseConfiguration {
    @Bean
    public ClientConfiguration clientConfiguration(
            IgniteProperties igniteProperties
    ) {
        ClientConfiguration cfg = new ClientConfiguration();
        cfg.setAddresses(igniteProperties.getAddresses().toArray(new String[0]));
        cfg.setTimeout(igniteProperties.getTimeout());
        return cfg;
    }
}
