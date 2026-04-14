package com.userchurn.manager.repo;

import com.userchurn.manager.domain.AppUser;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

public interface AppUserRepository extends JpaRepository<AppUser, Long> {

    boolean existsByUsernameIgnoreCase(String username);

    Optional<AppUser> findByUsernameIgnoreCase(String username);
}
