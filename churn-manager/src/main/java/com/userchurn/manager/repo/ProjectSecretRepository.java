package com.userchurn.manager.repo;

import com.userchurn.manager.domain.ProjectSecret;
import org.springframework.data.jpa.repository.JpaRepository;

public interface ProjectSecretRepository extends JpaRepository<ProjectSecret, Long> {
}
