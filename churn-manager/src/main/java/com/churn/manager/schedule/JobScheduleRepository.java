package com.churn.manager.schedule;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface JobScheduleRepository extends JpaRepository<JobSchedule, Long> {
    List<JobSchedule> findByProjectIdOrderByCreatedAtDesc(Long projectId);
    List<JobSchedule> findByEnabled(Boolean enabled);
}
