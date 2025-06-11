package dev.frndpovoa.project1.databaseproxy.spring.config;

import dev.frndpovoa.project1.databaseproxy.spring.DatabaseProxyConnectionProvider;
import dev.frndpovoa.project1.databaseproxy.spring.DatabaseProxyTransactionManager;
import org.hibernate.cfg.Environment;
import org.hibernate.engine.jdbc.connections.spi.ConnectionProvider;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.orm.jpa.LocalContainerEntityManagerFactoryBean;
import org.springframework.orm.jpa.vendor.HibernateJpaVendorAdapter;
import org.springframework.transaction.PlatformTransactionManager;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class JpaConfig {

    @Bean
    @Primary
    public ConnectionProvider connectionProvider(
            final DatabaseProxyDataSourceProperties databaseProxyDataSourceProperties,
            final DatabaseProxyProperties databaseProxyProperties
    ) {
        return new DatabaseProxyConnectionProvider(databaseProxyProperties, databaseProxyDataSourceProperties);
    }

    @Bean
    @Primary
    public LocalContainerEntityManagerFactoryBean entityManagerFactory(
            final ConnectionProvider connectionProvider
    ) {
        final HibernateJpaVendorAdapter vendorAdapter = new HibernateJpaVendorAdapter();
        vendorAdapter.setShowSql(true);
        vendorAdapter.setGenerateDdl(true);

        final Map<String, Object> jpaProperties = new HashMap<>();
        jpaProperties.put(Environment.CONNECTION_PROVIDER, connectionProvider);
        jpaProperties.put(Environment.HBM2DDL_AUTO, "create-drop");
        jpaProperties.put(Environment.DIALECT, "org.hibernate.dialect.PostgreSQLDialect");
        jpaProperties.put(Environment.SHOW_SQL, "true");
        jpaProperties.put(Environment.FORMAT_SQL, "true");

        final LocalContainerEntityManagerFactoryBean em = new LocalContainerEntityManagerFactoryBean();
//        em.setDataSource(dataSource);
        em.setPackagesToScan("dev.frndpovoa.project1.databaseproxy.spring");
        em.setJpaVendorAdapter(vendorAdapter);
        em.setJpaPropertyMap(jpaProperties);
        em.afterPropertiesSet();
        return em;
    }

    @Bean
    @Primary
    public PlatformTransactionManager transactionManager(
            final DatabaseProxyDataSourceProperties databaseProxyDataSourceProperties
    ) {
        return new DatabaseProxyTransactionManager(databaseProxyDataSourceProperties);
    }
}
