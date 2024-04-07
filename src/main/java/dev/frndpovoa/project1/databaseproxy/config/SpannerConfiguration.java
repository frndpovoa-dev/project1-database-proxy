package dev.frndpovoa.project1.databaseproxy.config;

import com.google.cloud.spanner.pgadapter.ProxyServer;
import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata;
import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.Optional;

//@Component
//@Configuration(proxyBeanMethods = false)
//@Profile({"!integration"})
@RequiredArgsConstructor
public class SpannerConfiguration {
    private final PostgresqlProperties postgresqlProperties;
    private final SpannerProperties spannerProperties;
    private ProxyServer server;

    @PostConstruct
    public void postConstruct() {
        final OptionsMetadata.Builder builder = OptionsMetadata.newBuilder()
                .setProject(spannerProperties.getProject())
                .setInstance(spannerProperties.getInstance())
                .setDatabase(postgresqlProperties.getDatabase())
                .setPort(postgresqlProperties.getPort());
        server = new ProxyServer(builder.build());
        server.startServer();
        server.awaitRunning();
    }

    @PreDestroy
    public void preDestroy() {
        Optional.ofNullable(server)
                .ifPresent(it -> {
                    it.stopServer();
                    it.awaitTerminated();
                });
    }
}
