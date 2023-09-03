package dev.frndpovoa.project1.databaseproxy.grpc;

import dev.frndpovoa.project1.databaseproxy.config.GrpcProperties;
import dev.frndpovoa.project1.databaseproxy.service.DatabaseProxyService;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class GrpcServer implements InitializingBean, DisposableBean {
    private final GrpcProperties grpcProperties;
    private final DatabaseProxyService databaseProxyService;
    private Server server;

    @Override
    public void destroy() throws Exception {
        server
                .shutdown()
                .awaitTermination();
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        this.server = ServerBuilder
                .forPort(grpcProperties.getPort())
                .addService(databaseProxyService)
                .build()
                .start();
    }
}
