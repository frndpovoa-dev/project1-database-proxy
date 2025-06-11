package dev.frndpovoa.project1.databaseproxy.spring.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Getter
@Setter
@ConfigurationProperties(prefix = "app.db-proxy-datasource")
public class DatabaseProxyDataSourceProperties {
    private String url;
}
