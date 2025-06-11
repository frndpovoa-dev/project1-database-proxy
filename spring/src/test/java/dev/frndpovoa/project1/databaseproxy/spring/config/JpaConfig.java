package dev.frndpovoa.project1.databaseproxy.spring.config;

import dev.frndpovoa.project1.databaseproxy.spring.DatabaseProxyTransactionManager;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.orm.jpa.LocalContainerEntityManagerFactoryBean;
import org.springframework.orm.jpa.vendor.HibernateJpaVendorAdapter;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import java.util.HashMap;
import java.util.Map;

@TestConfiguration
@EnableTransactionManagement
@EnableJpaRepositories(
        basePackages = "dev.frndpovoa.project1.databaseproxy.spring",
        entityManagerFactoryRef = "postgresqlEM",
        transactionManagerRef = "postgresqlTM"
)
public class JpaConfig {

    @Bean
    @Primary
    @Qualifier("postgresqlEM")
    public LocalContainerEntityManagerFactoryBean entityManagerFactory() {
        final HibernateJpaVendorAdapter vendorAdapter = new HibernateJpaVendorAdapter();
        vendorAdapter.setShowSql(true);
        vendorAdapter.setGenerateDdl(true);

        final Map<String, Object> jpaProperties = new HashMap<>();
        jpaProperties.put("hibernate.hbm2ddl.auto", "none");
        jpaProperties.put("hibernate.dialect", "org.hibernate.dialect.PostgreSQLDialect");
        jpaProperties.put("hibernate.show_sql", "true");
        jpaProperties.put("hibernate.format_sql", "true");

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
    @Qualifier("postgresqlTM")
    public PlatformTransactionManager transactionManager(
            final DatabaseProxyProperties databaseProxyProperties,
            final DataSourceProperties dataSourceProperties
    ) {
        return new DatabaseProxyTransactionManager(databaseProxyProperties, dataSourceProperties);
    }
}
