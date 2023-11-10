package dev.frndpovoa.project1.databaseproxy;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

@Slf4j
public class PostgresExtension implements BeforeAllCallback {
    private GenericContainer<?> postgresql;

    @Override
    public void beforeAll(final ExtensionContext context) throws Exception {
        final String database = "test";
        final int port = 5432;
        final String username = "postgres";
        final String password = "postgres";
        postgresql = new GenericContainer(DockerImageName
                .parse("bitnami/postgresql")
                .withTag(context.getConfigurationParameter("postgresql.version").orElse("latest"))
        ) {
        }
                .withSharedMemorySize(1024 * 1024 * 512L)
                .withEnv("POSTGRESQL_DATABASE", database)
                .withEnv("POSTGRESQL_PASSWORD", password)
                .withExposedPorts(port)
        ;

        postgresql.start();

        System.setProperty("POSTGRESQL_HOSTNAME", postgresql.getHost());
        System.setProperty("POSTGRESQL_PORT", "" + postgresql.getMappedPort(port));
        System.setProperty("POSTGRESQL_DATABASE", database);
        System.setProperty("POSTGRESQL_USERNAME", username);
        System.setProperty("POSTGRESQL_PASSWORD", password);

        log.info("Postgresql server started on {}:{}", postgresql.getHost(), postgresql.getMappedPort(port));
    }
}
