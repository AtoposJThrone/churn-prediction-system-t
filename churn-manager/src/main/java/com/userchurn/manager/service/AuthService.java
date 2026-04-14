package com.userchurn.manager.service;

import com.userchurn.manager.domain.AppUser;
import com.userchurn.manager.repo.AppUserRepository;
import com.userchurn.manager.web.ApiException;
import org.springframework.core.env.Environment;
import org.springframework.http.HttpStatus;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.LinkedHashMap;
import java.util.Map;

@Service
public class AuthService {

    private final AppUserRepository appUserRepository;
    private final TokenService tokenService;
    private final SecretCryptoService secretCryptoService;
    private final Environment environment;
    private final BCryptPasswordEncoder passwordEncoder = new BCryptPasswordEncoder();

    public AuthService(AppUserRepository appUserRepository,
                       TokenService tokenService,
                       SecretCryptoService secretCryptoService,
                       Environment environment) {
        this.appUserRepository = appUserRepository;
        this.tokenService = tokenService;
        this.secretCryptoService = secretCryptoService;
        this.environment = environment;
    }

    public Map<String, Object> getStatus() {
        boolean initialized = appUserRepository.count() > 0;
        Map<String, Object> status = new LinkedHashMap<>();
        status.put("initialized", initialized);
        status.put("persistentSecretConfigured", secretCryptoService.hasPersistentKey());
        status.put("recommendedAdminUsername", environment.getProperty("INIT_ADMIN_USERNAME_HINT"));
        status.put("recommendedAdminPassword", environment.getProperty("INIT_ADMIN_PASSWORD_HINT"));
        return status;
    }

    @Transactional
    public void initialize(String username, String password) {
        if (appUserRepository.count() > 0) {
            throw new ApiException(HttpStatus.CONFLICT, "系统已经初始化过管理员账号。请直接登录。" );
        }
        if (username == null || username.isBlank() || password == null || password.isBlank()) {
            throw new ApiException(HttpStatus.BAD_REQUEST, "初始化管理员账号时必须填写用户名和密码。" );
        }

        AppUser user = new AppUser();
        user.setUsername(username.trim());
        user.setPasswordHash(passwordEncoder.encode(password));
        appUserRepository.save(user);
    }

    public Map<String, Object> login(String username, String password) {
        AppUser user = appUserRepository.findByUsernameIgnoreCase(username == null ? "" : username.trim())
            .orElseThrow(() -> new ApiException(HttpStatus.UNAUTHORIZED, "用户名或密码错误。"));
        if (!passwordEncoder.matches(password, user.getPasswordHash())) {
            throw new ApiException(HttpStatus.UNAUTHORIZED, "用户名或密码错误。" );
        }
        return Map.of(
            "token", tokenService.issueToken(user.getUsername()),
            "username", user.getUsername()
        );
    }

    public AppUser authenticate(String token) {
        String username = tokenService.verifyAndExtractUsername(token)
            .orElseThrow(() -> new ApiException(HttpStatus.UNAUTHORIZED, "登录已失效，请重新登录。"));
        return appUserRepository.findByUsernameIgnoreCase(username)
            .orElseThrow(() -> new ApiException(HttpStatus.UNAUTHORIZED, "用户不存在或已被删除。"));
    }
}
