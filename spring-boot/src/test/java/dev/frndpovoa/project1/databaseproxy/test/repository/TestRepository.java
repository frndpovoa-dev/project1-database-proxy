package dev.frndpovoa.project1.databaseproxy.test.repository;

import dev.frndpovoa.project1.databaseproxy.test.bo.TestBo;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface TestRepository extends JpaRepository<TestBo, Long> {
}
