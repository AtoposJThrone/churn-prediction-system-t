package com.churn.manager.project;

import org.springframework.data.jpa.repository.JpaRepository;

public interface ManagedProjectRepository extends JpaRepository<ManagedProject, Long> {}
