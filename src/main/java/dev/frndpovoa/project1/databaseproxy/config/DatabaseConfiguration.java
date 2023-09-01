package dev.frndpovoa.project1.databaseproxy.config;

import org.apache.ignite.configuration.ClientConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration(proxyBeanMethods = false)
public class DatabaseConfiguration {
    @Bean
    public ClientConfiguration igniteClientConfiguration(
            IgniteProperties igniteProperties
    ) {
        return new ClientConfiguration()
                .setAddresses(igniteProperties.getAddresses().toArray(new String[0]));
    }
}
