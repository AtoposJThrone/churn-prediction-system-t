package com.churn.manager.execution;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface JobRecordRepository extends JpaRepository<JobRecord, Long> {
    Page<JobRecord> findByProjectIdOrderByCreatedAtDesc(Long projectId, Pageable pageable);
    List<JobRecord> findByProjectIdAndStatus(Long projectId, JobStatus status);
}
