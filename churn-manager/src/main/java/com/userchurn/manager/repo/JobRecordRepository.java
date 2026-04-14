package com.userchurn.manager.repo;

import com.userchurn.manager.domain.JobRecord;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface JobRecordRepository extends JpaRepository<JobRecord, Long> {

    List<JobRecord> findByProjectIdOrderByCreatedAtDesc(Long projectId);
}
