package dev.frndpovoa.project1.databaseproxy.test.config;

import org.springframework.context.annotation.Bean;
import org.springframework.web.client.RestTemplate;

public class TestConfig {

    @Bean
    public RestTemplate restTemplate() {
        return new RestTemplate();
    }
}
