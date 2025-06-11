package dev.frndpovoa.project1.databaseproxy.spring.bo;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.*;

@Getter
@Setter
@Builder
@AllArgsConstructor
@NoArgsConstructor
@EqualsAndHashCode
@Entity
@Table(name = "test")
public class TestBo {
    @Id
    @EqualsAndHashCode.Include
    private Long id;
    @EqualsAndHashCode.Include
    private String name;
}
