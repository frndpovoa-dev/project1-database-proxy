package dev.frndpovoa.project1.databaseproxy.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Getter
@Setter
@ConfigurationProperties(prefix = "app.grpc")
public class GrpcProperties {
    private Integer port;
}
