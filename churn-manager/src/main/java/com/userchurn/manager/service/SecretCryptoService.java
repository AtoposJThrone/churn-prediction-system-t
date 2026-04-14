package com.userchurn.manager.service;

import com.userchurn.manager.web.ApiException;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.env.Environment;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.util.Base64;

@Service
public class SecretCryptoService {

    private static final Logger log = LoggerFactory.getLogger(SecretCryptoService.class);
    private static final int GCM_TAG_BITS = 128;
    private static final int IV_LENGTH = 12;

    private final Environment environment;
    private final SecureRandom secureRandom = new SecureRandom();

    private SecretKeySpec secretKey;
    private boolean persistentKey;

    public SecretCryptoService(Environment environment) {
        this.environment = environment;
    }

    @PostConstruct
    public void init() {
        String configured = environment.getProperty("MANAGER_SECRET_KEY");
        byte[] raw;
        if (configured == null || configured.isBlank()) {
            raw = new byte[32];
            secureRandom.nextBytes(raw);
            persistentKey = false;
            log.warn("MANAGER_SECRET_KEY 未配置，当前将使用进程内随机密钥。重启后项目敏感配置需要重新录入。{}");
        } else {
            raw = normalizeKey(configured);
            persistentKey = true;
        }
        secretKey = new SecretKeySpec(raw, "AES");
    }

    public String encrypt(String plainText) {
        if (plainText == null || plainText.isBlank()) {
            return null;
        }
        try {
            byte[] iv = new byte[IV_LENGTH];
            secureRandom.nextBytes(iv);
            Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
            cipher.init(Cipher.ENCRYPT_MODE, secretKey, new GCMParameterSpec(GCM_TAG_BITS, iv));
            byte[] encrypted = cipher.doFinal(plainText.getBytes(StandardCharsets.UTF_8));
            ByteBuffer buffer = ByteBuffer.allocate(iv.length + encrypted.length);
            buffer.put(iv);
            buffer.put(encrypted);
            return Base64.getEncoder().encodeToString(buffer.array());
        } catch (GeneralSecurityException ex) {
            throw new ApiException(HttpStatus.INTERNAL_SERVER_ERROR, "敏感字段加密失败。");
        }
    }

    public String decrypt(String encryptedText) {
        if (encryptedText == null || encryptedText.isBlank()) {
            return null;
        }
        try {
            byte[] allBytes = Base64.getDecoder().decode(encryptedText);
            ByteBuffer buffer = ByteBuffer.wrap(allBytes);
            byte[] iv = new byte[IV_LENGTH];
            buffer.get(iv);
            byte[] encrypted = new byte[buffer.remaining()];
            buffer.get(encrypted);

            Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
            cipher.init(Cipher.DECRYPT_MODE, secretKey, new GCMParameterSpec(GCM_TAG_BITS, iv));
            return new String(cipher.doFinal(encrypted), StandardCharsets.UTF_8);
        } catch (Exception ex) {
            throw new ApiException(HttpStatus.INTERNAL_SERVER_ERROR, "敏感字段解密失败，请检查 MANAGER_SECRET_KEY 是否与历史一致。");
        }
    }

    public boolean hasPersistentKey() {
        return persistentKey;
    }

    private byte[] normalizeKey(String configured) {
        try {
            byte[] decoded = Base64.getDecoder().decode(configured);
            if (decoded.length == 16 || decoded.length == 24 || decoded.length == 32) {
                return decoded;
            }
        } catch (IllegalArgumentException ignored) {
            // Fall through to raw bytes handling.
        }

        byte[] bytes = configured.getBytes(StandardCharsets.UTF_8);
        byte[] normalized = new byte[32];
        for (int i = 0; i < normalized.length; i++) {
            normalized[i] = i < bytes.length ? bytes[i] : 0;
        }
        return normalized;
    }
}
