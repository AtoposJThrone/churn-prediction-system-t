package com.userchurn.manager.repo;

import com.userchurn.manager.domain.JobSchedule;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface JobScheduleRepository extends JpaRepository<JobSchedule, Long> {

    List<JobSchedule> findByProjectIdOrderByUpdatedAtDesc(Long projectId);

    List<JobSchedule> findByEnabledTrue();
}