package com.churn.manager.project;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

public interface ProjectSecretRepository extends JpaRepository<ProjectSecret, Long> {
    Optional<ProjectSecret> findByProjectId(Long projectId);
}
