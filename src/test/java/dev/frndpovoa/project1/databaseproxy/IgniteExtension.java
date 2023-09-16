package dev.frndpovoa.project1.databaseproxy;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.List;

@Slf4j
public class IgniteExtension implements BeforeAllCallback {
    private GenericContainer<?> ignite;

    @Override
    public void beforeAll(final ExtensionContext context) throws Exception {
        final int thinClientPort = 10800;
        final int restPort = 8080;
        ignite = new GenericContainer(DockerImageName
                .parse("apacheignite/ignite")
                .withTag(context.getConfigurationParameter("ignite.version").orElse("latest"))
        ) {
        }
                .withSharedMemorySize(1024 * 1024 * 512L)
                .withEnv("OPTION_LIBS", "ignite-rest-http");

        ignite.setPortBindings(List.of(thinClientPort + ":" + thinClientPort,  "9080:" + restPort));
        ignite.start();

        System.setProperty("IGNITE_HOST", ignite.getHost());
        System.setProperty("IGNITE_PORT", "" + ignite.getMappedPort(thinClientPort));

        log.info("Ignite client started on {}:{}", ignite.getHost(), ignite.getMappedPort(thinClientPort));
        log.info("Ignite REST API started on {}:{}", ignite.getHost(), ignite.getMappedPort(restPort));
    }
}
