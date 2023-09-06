package dev.frndpovoa.project1.databaseproxy;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

@Slf4j
public class IgniteExtension implements BeforeAllCallback {
    private GenericContainer ignite;

    @Override
    public void beforeAll(ExtensionContext context) throws Exception {
        int port = 10800;
        ignite = new GenericContainer(DockerImageName
                .parse("apacheignite/ignite")
                .withTag(context.getConfigurationParameter("ignite.version").orElse("latest"))
        ) {
        }
                .withSharedMemorySize(1024 * 1024 * 512L)
                .withExposedPorts(port)
        ;

        ignite.start();

        System.setProperty("IGNITE_HOST", ignite.getHost());
        System.setProperty("IGNITE_PORT", "" + ignite.getMappedPort(port));
    }
}
