package com.churn.manager.project;

import com.churn.manager.config.AppProperties;
import org.springframework.stereotype.Service;

import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.security.SecureRandom;
import java.util.Base64;

/**
 * AES-256-GCM encryption service for sensitive project credentials.
 * The key is loaded from app.crypto.secret-key (Base64-encoded 32 bytes).
 * If not configured, a random key is generated per process (restart invalidates ciphertext).
 */
@Service
public class CryptoService {

    private static final String CIPHER = "AES/GCM/NoPadding";
    private static final int GCM_IV_LENGTH = 12;
    private static final int GCM_TAG_BITS = 128;

    private final SecretKey aesKey;
    private final SecureRandom rng = new SecureRandom();

    public CryptoService(AppProperties props) {
        String b64Key = props.getCrypto().getSecretKey();
        if (b64Key != null && !b64Key.isBlank()) {
            byte[] keyBytes = Base64.getDecoder().decode(b64Key.trim());
            this.aesKey = new SecretKeySpec(keyBytes, "AES");
        } else {
            this.aesKey = generateRandomKey();
        }
    }

    /** Encrypt plaintext → Base64(IV || Ciphertext) */
    public String encrypt(String plaintext) {
        if (plaintext == null || plaintext.isBlank()) return null;
        try {
            byte[] iv = new byte[GCM_IV_LENGTH];
            rng.nextBytes(iv);
            Cipher cipher = Cipher.getInstance(CIPHER);
            cipher.init(Cipher.ENCRYPT_MODE, aesKey, new GCMParameterSpec(GCM_TAG_BITS, iv));
            byte[] encrypted = cipher.doFinal(plaintext.getBytes("UTF-8"));
            byte[] combined = new byte[iv.length + encrypted.length];
            System.arraycopy(iv, 0, combined, 0, iv.length);
            System.arraycopy(encrypted, 0, combined, iv.length, encrypted.length);
            return Base64.getEncoder().encodeToString(combined);
        } catch (Exception e) {
            throw new IllegalStateException("Encryption failed", e);
        }
    }

    /** Decrypt Base64(IV || Ciphertext) → plaintext */
    public String decrypt(String ciphertext) {
        if (ciphertext == null || ciphertext.isBlank()) return null;
        try {
            byte[] combined = Base64.getDecoder().decode(ciphertext);
            byte[] iv = new byte[GCM_IV_LENGTH];
            System.arraycopy(combined, 0, iv, 0, GCM_IV_LENGTH);
            byte[] data = new byte[combined.length - GCM_IV_LENGTH];
            System.arraycopy(combined, GCM_IV_LENGTH, data, 0, data.length);
            Cipher cipher = Cipher.getInstance(CIPHER);
            cipher.init(Cipher.DECRYPT_MODE, aesKey, new GCMParameterSpec(GCM_TAG_BITS, iv));
            return new String(cipher.doFinal(data), "UTF-8");
        } catch (Exception e) {
            throw new IllegalStateException("Decryption failed — check that MANAGER_SECRET_KEY matches the key used during encryption.", e);
        }
    }

    private static SecretKey generateRandomKey() {
        try {
            KeyGenerator gen = KeyGenerator.getInstance("AES");
            gen.init(256);
            return gen.generateKey();
        } catch (Exception e) {
            throw new IllegalStateException("AES key generation failed", e);
        }
    }
}
