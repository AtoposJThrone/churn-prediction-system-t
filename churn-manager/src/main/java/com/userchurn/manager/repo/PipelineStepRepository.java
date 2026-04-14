package com.userchurn.manager.repo;

import com.userchurn.manager.domain.PipelineStep;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface PipelineStepRepository extends JpaRepository<PipelineStep, Long> {

    List<PipelineStep> findByPipelineIdOrderByStepOrderAsc(Long pipelineId);
}
