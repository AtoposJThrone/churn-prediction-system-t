package com.userchurn.manager.repo;

import com.userchurn.manager.domain.ManagedProject;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface ManagedProjectRepository extends JpaRepository<ManagedProject, Long> {

    boolean existsByNameIgnoreCase(String name);

    List<ManagedProject> findAllByOrderByUpdatedAtDesc();
}
