package dev.frndpovoa.project1.databaseproxy.test.controller;

/*-
 * #%L
 * database-proxy-spring-test
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

import dev.frndpovoa.project1.databaseproxy.config.DatabaseProxyDataSourceProperties;
import dev.frndpovoa.project1.databaseproxy.config.DatabaseProxyProperties;
import dev.frndpovoa.project1.databaseproxy.proto.BeginTransactionConfig;
import dev.frndpovoa.project1.databaseproxy.proto.DatabaseProxyGrpc;
import dev.frndpovoa.project1.databaseproxy.proto.Empty;
import dev.frndpovoa.project1.databaseproxy.proto.Transaction;
import dev.frndpovoa.project1.databaseproxy.test.BaseIntTest;
import dev.frndpovoa.project1.databaseproxy.test.dto.TestDto;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestTemplate;

import java.util.List;
import java.util.Map;

@Slf4j
class TestControllerIntTest extends BaseIntTest {
    @Autowired
    private RestTemplate restTemplate;
    @Autowired
    private DatabaseProxyProperties databaseProxyProperties;
    @Autowired
    private DatabaseProxyDataSourceProperties databaseProxyDataSourceProperties;
    private ManagedChannel channel;
    private DatabaseProxyGrpc.DatabaseProxyBlockingStub blockingStub;
    private Transaction tx1Transaction;
    private String tx1TransactionId;
    private Transaction tx2Transaction;
    private String tx2TransactionId;

    @BeforeEach
    void setUp() throws Exception {
        this.channel = ManagedChannelBuilder
                .forAddress(databaseProxyProperties.getHostname(), databaseProxyProperties.getPort())
                .usePlaintext()
                .build();
        this.blockingStub = DatabaseProxyGrpc
                .newBlockingStub(channel);

        this.tx1Transaction = blockingStub
                .beginTransaction(BeginTransactionConfig.newBuilder()
                        .setConnectionString(databaseProxyDataSourceProperties.getUrl())
                        .setTimeout(60_000 * 10)
                        .setReadOnly(false)
                        .build());
        this.tx2Transaction = blockingStub
                .beginTransaction(BeginTransactionConfig.newBuilder()
                        .setConnectionString(databaseProxyDataSourceProperties.getUrl())
                        .setTimeout(60_000 * 10)
                        .setReadOnly(false)
                        .build());

        this.tx1TransactionId = tx1Transaction.getId() + "@" + tx1Transaction.getNode();
        log.debug("Tx 1 transactionId({})", tx1TransactionId);

        this.tx2TransactionId = tx2Transaction.getId() + "@" + tx2Transaction.getNode();
        log.debug("Tx 2 transactionId({})", tx2TransactionId);
    }

    @AfterEach
    void tearDown() throws Exception {
        blockingStub.rollbackTransaction(tx1Transaction);
        blockingStub.rollbackTransaction(tx2Transaction);
        blockingStub.closeConnection(Empty.getDefaultInstance());
        channel.shutdownNow();
    }

    @Test
    @SuppressWarnings({"unchecked"})
    void testApiUsingSharedTransaction() {
        final ParameterizedTypeReference<List<TestDto>> listTypeReference = new ParameterizedTypeReference<>() {
        };

        log.debug("Read before insert using tx 1");
        log.debug("{}", restTemplate.exchange("http://localhost:8080/api/v1/test/list", HttpMethod.GET, new HttpEntity<>(
                MultiValueMap.fromSingleValue(Map.of("X-Transaction-Id", tx1TransactionId))), listTypeReference).getBody());

        log.debug("Read before insert using tx 2");
        log.debug("{}", restTemplate.exchange("http://localhost:8080/api/v1/test/list", HttpMethod.GET, new HttpEntity<>(
                MultiValueMap.fromSingleValue(Map.of("X-Transaction-Id", tx2TransactionId))), listTypeReference).getBody());

        log.debug("Insert using tx 1");
        log.debug("{}", restTemplate.exchange("http://localhost:8080/api/v1/test/insert", HttpMethod.POST, new HttpEntity<>(
                TestDto.builder().id(2025L).name("Hello World!").build(),
                MultiValueMap.fromSingleValue(Map.of("X-Transaction-Id", tx1TransactionId))), TestDto.class).getBody());

        log.debug("Read after insert using tx 1");
        log.debug("{}", restTemplate.exchange("http://localhost:8080/api/v1/test/list", HttpMethod.GET, new HttpEntity<>(
                MultiValueMap.fromSingleValue(Map.of("X-Transaction-Id", tx1TransactionId))), listTypeReference).getBody());

        log.debug("Read after insert using tx 2");
        log.debug("{}", restTemplate.exchange("http://localhost:8080/api/v1/test/list", HttpMethod.GET, new HttpEntity<>(
                MultiValueMap.fromSingleValue(Map.of("X-Transaction-Id", tx2TransactionId))), listTypeReference).getBody());


    }
}
