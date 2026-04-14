package com.userchurn.manager.repo;

import com.userchurn.manager.domain.PipelineTemplate;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface PipelineTemplateRepository extends JpaRepository<PipelineTemplate, Long> {

    List<PipelineTemplate> findByProjectIdOrderByUpdatedAtDesc(Long projectId);
}
