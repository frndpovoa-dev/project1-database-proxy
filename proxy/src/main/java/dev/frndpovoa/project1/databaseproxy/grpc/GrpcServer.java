package dev.frndpovoa.project1.databaseproxy.grpc;

/*-
 * #%L
 * database-proxy
 * %%
 * Copyright (C) 2025 Fernando Lemes Povoa
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import dev.frndpovoa.project1.databaseproxy.config.GrpcProperties;
import dev.frndpovoa.project1.databaseproxy.service.DatabaseProxyService;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

@Component
@RequiredArgsConstructor
public class GrpcServer implements InitializingBean, DisposableBean {
    private final GrpcProperties grpcProperties;
    private final DatabaseProxyService databaseProxyService;
    private Server server;

    @Override
    public void destroy() throws Exception {
        if (!server.shutdown().awaitTermination(20, TimeUnit.SECONDS)) {
            server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        }
        this.server = null;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        if (server == null) {
            this.server = ServerBuilder
                    .forPort(grpcProperties.getPort())
                    .addService(databaseProxyService)
                    .build()
                    .start();
        }
    }
}
