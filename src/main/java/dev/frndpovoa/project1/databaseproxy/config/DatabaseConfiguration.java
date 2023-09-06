package dev.frndpovoa.project1.databaseproxy.config;

import org.springframework.context.annotation.Configuration;

@Configuration(proxyBeanMethods = false)
public class DatabaseConfiguration {
    public DatabaseConfiguration() throws Exception {
        Class.forName("org.apache.ignite.IgniteJdbcThinDriver");
    }
}
