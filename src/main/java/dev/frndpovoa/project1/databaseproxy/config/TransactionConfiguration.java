package dev.frndpovoa.project1.databaseproxy.config;

import com.atomikos.icatch.jta.UserTransactionManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.jta.JtaTransactionManager;

@Configuration(proxyBeanMethods = false)
public class TransactionConfiguration {
    @Bean
    public PlatformTransactionManager platformTransactionManager() {
        JtaTransactionManager transactionManager = new JtaTransactionManager();
        transactionManager.setUserTransaction(new UserTransactionManager());
        return transactionManager;
    }
}
