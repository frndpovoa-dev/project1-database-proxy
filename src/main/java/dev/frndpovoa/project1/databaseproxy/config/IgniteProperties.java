package dev.frndpovoa.project1.databaseproxy.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.List;

@Getter
@Setter
@ConfigurationProperties(prefix = "app.ignite")
public class IgniteProperties {
    private List<String> addresses;
}
