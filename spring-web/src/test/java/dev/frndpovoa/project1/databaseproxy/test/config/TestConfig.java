package dev.frndpovoa.project1.databaseproxy.test.config;

import dev.frndpovoa.project1.databaseproxy.config.DatabaseProxyDataSourceProperties;
import dev.frndpovoa.project1.databaseproxy.config.DatabaseProxyProperties;
import dev.frndpovoa.project1.databaseproxy.jdbc.DataSource;
import jakarta.persistence.EntityManagerFactory;
import org.hibernate.cfg.Environment;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.orm.jpa.LocalContainerEntityManagerFactoryBean;
import org.springframework.orm.jpa.vendor.HibernateJpaVendorAdapter;

import java.util.HashMap;
import java.util.Map;

public class TestConfig {

    @Bean("dataSource")
    @Primary
    public DataSource dataSource(
            final DatabaseProxyDataSourceProperties databaseProxyDataSourceProperties,
            final DatabaseProxyProperties databaseProxyProperties
    ) {
        return new DataSource(databaseProxyDataSourceProperties, databaseProxyProperties);
    }

    @Bean("entityManagerFactory")
    @Primary
    public LocalContainerEntityManagerFactoryBean entityManagerFactory(
            final DataSource dataSource
    ) {
        final HibernateJpaVendorAdapter vendorAdapter = new HibernateJpaVendorAdapter();
        vendorAdapter.setShowSql(true);
        vendorAdapter.setGenerateDdl(true);

        final Map<String, Object> jpaProperties = new HashMap<>();
        jpaProperties.put(Environment.HBM2DDL_AUTO, "create-drop");
        jpaProperties.put(Environment.DIALECT, "org.hibernate.dialect.PostgreSQLDialect");
        jpaProperties.put(Environment.SHOW_SQL, true);
        jpaProperties.put(Environment.FORMAT_SQL, true);

        final LocalContainerEntityManagerFactoryBean em = new LocalContainerEntityManagerFactoryBean();
        em.setDataSource(dataSource);
        em.setPackagesToScan(dev.frndpovoa.project1.databaseproxy.test.bo.Package.class.getPackageName());
        em.setJpaVendorAdapter(vendorAdapter);
        em.setJpaPropertyMap(jpaProperties);
        em.afterPropertiesSet();
        return em;
    }

    @Bean("transactionManager")
    @Primary
    public JpaTransactionManager transactionManager(
            final EntityManagerFactory entityManagerFactory,
            final DataSource dataSource
    ) {
        JpaTransactionManager transactionManager = new JpaTransactionManager();
        transactionManager.setDataSource(dataSource);
        transactionManager.setEntityManagerFactory(entityManagerFactory);
        transactionManager.afterPropertiesSet();
        return transactionManager;
    }
}
