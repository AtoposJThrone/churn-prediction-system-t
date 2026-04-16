package com.churn.manager.auth;

import com.churn.manager.common.ApiException;
import com.churn.manager.config.AppProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpStatus;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Map;
import java.util.Optional;

@Service
public class AuthService {

    private static final Logger log = LoggerFactory.getLogger(AuthService.class);

    private final AppUserRepository userRepository;
    private final TokenService tokenService;
    private final AppProperties appProperties;
    private final BCryptPasswordEncoder passwordEncoder = new BCryptPasswordEncoder();

    public AuthService(AppUserRepository userRepository,
                       TokenService tokenService,
                       AppProperties appProperties) {
        this.userRepository = userRepository;
        this.tokenService = tokenService;
        this.appProperties = appProperties;
    }

    // Auto-create admin on first startup.  
    @EventListener(ApplicationReadyEvent.class)
    @Order(1)
    @Transactional
    public void initDefaultAdmin() {
        if (userRepository.count() > 0) return;
        String username = appProperties.getInit().getAdminUsername();
        String password = appProperties.getInit().getAdminPassword();
        if (username == null || username.isBlank() || password == null || password.isBlank()) {
            log.warn("INIT_ADMIN_USERNAME / INIT_ADMIN_PASSWORD not configured — skipping auto-init.");
            return;
        }
        AppUser admin = new AppUser();
        admin.setUsername(username.trim());
        admin.setPasswordHash(passwordEncoder.encode(password));
        userRepository.save(admin);
        log.info("Default admin user created: {}", username);
    }

    @Transactional(readOnly = true)
    public Map<String, Object> login(String username, String password) {
        AppUser user = userRepository.findByUsernameIgnoreCase(username == null ? "" : username.trim())
                .orElseThrow(() -> new ApiException(HttpStatus.UNAUTHORIZED, "用户名或密码错误。"));
        if (!passwordEncoder.matches(password, user.getPasswordHash())) {
            throw new ApiException(HttpStatus.UNAUTHORIZED, "用户名或密码错误。");
        }
        return Map.of(
                "token", tokenService.issue(user.getUsername()),
                "username", user.getUsername()
        );
    }

    public Optional<String> verifyToken(String token) {
        return tokenService.verify(token);
    }
}
