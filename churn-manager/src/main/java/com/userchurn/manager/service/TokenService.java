package com.userchurn.manager.service;

import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Service;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.time.Instant;
import java.util.Base64;
import java.util.Optional;
import java.util.UUID;

@Service
public class TokenService {

    private static final Logger log = LoggerFactory.getLogger(TokenService.class);

    private final Environment environment;
    private final long ttlHours;
    private final SecureRandom secureRandom = new SecureRandom();

    private byte[] secretBytes;

    public TokenService(Environment environment, @Value("${app.auth.token-ttl-hours:12}") long ttlHours) {
        this.environment = environment;
        this.ttlHours = ttlHours;
    }

    @PostConstruct
    public void init() {
        String configured = environment.getProperty("MANAGER_TOKEN_SECRET");
        if (configured == null || configured.isBlank()) {
            secretBytes = new byte[32];
            secureRandom.nextBytes(secretBytes);
            log.warn("MANAGER_TOKEN_SECRET 未配置，当前使用进程内随机 token 密钥。重启后所有 token 将失效。");
        } else {
            secretBytes = configured.getBytes(StandardCharsets.UTF_8);
        }
    }

    public String issueToken(String username) {
        long expiresAt = Instant.now().plusSeconds(ttlHours * 3600).getEpochSecond();
        String payload = username + "|" + expiresAt + "|" + UUID.randomUUID();
        String encoded = Base64.getUrlEncoder().withoutPadding().encodeToString(payload.getBytes(StandardCharsets.UTF_8));
        return encoded + "." + sign(encoded);
    }

    public Optional<String> verifyAndExtractUsername(String token) {
        if (token == null || token.isBlank() || !token.contains(".")) {
            return Optional.empty();
        }
        String[] parts = token.split("\\.", 2);
        if (parts.length != 2 || !slowEquals(sign(parts[0]), parts[1])) {
            return Optional.empty();
        }

        String payload = new String(Base64.getUrlDecoder().decode(parts[0]), StandardCharsets.UTF_8);
        String[] values = payload.split("\\|", 3);
        if (values.length < 2) {
            return Optional.empty();
        }
        long expiresAt = Long.parseLong(values[1]);
        if (Instant.now().getEpochSecond() > expiresAt) {
            return Optional.empty();
        }
        return Optional.of(values[0]);
    }

    private String sign(String encodedPayload) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(secretBytes, "HmacSHA256"));
            return Base64.getUrlEncoder().withoutPadding()
                .encodeToString(mac.doFinal(encodedPayload.getBytes(StandardCharsets.UTF_8)));
        } catch (GeneralSecurityException ex) {
            throw new IllegalStateException("Unable to sign auth token", ex);
        }
    }

    private boolean slowEquals(String left, String right) {
        if (left.length() != right.length()) {
            return false;
        }
        int result = 0;
        for (int i = 0; i < left.length(); i++) {
            result |= left.charAt(i) ^ right.charAt(i);
        }
        return result == 0;
    }
}
