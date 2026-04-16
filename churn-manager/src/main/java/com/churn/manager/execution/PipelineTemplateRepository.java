package com.churn.manager.execution;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;

import java.util.List;

public interface PipelineTemplateRepository extends JpaRepository<PipelineTemplate, Long> {
    List<PipelineTemplate> findByProjectIdOrderByUpdatedAtDesc(Long projectId);
}
