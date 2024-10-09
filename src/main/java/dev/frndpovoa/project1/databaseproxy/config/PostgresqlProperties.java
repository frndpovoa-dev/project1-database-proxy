package dev.frndpovoa.project1.databaseproxy.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Getter
@Setter
@ConfigurationProperties(prefix = "app.postgresql")
public class PostgresqlProperties {
    private String database;
    private String username;
    private String password;
    private String hostname;
    private int port;
    private String url;
    private boolean showSql;
}
