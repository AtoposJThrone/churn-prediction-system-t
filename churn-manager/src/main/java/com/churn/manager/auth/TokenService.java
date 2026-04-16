package com.churn.manager.auth;

import com.churn.manager.config.AppProperties;
import org.springframework.stereotype.Service;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;
import java.util.Optional;
import java.util.UUID;

/**
  Lightweight token Use HMAC-SHA256.
  Token format: base64(header).base64(payload).base64(signature)
  Payload: username|expireEpochSeconds
**/
@Service
public class TokenService {

    private static final String ALGORITHM = "HmacSHA256";
    private final byte[] secretBytes;
    private final long ttlSeconds;

    public TokenService(AppProperties props) {
        String raw = props.getAuth().getTokenSecret();
        this.secretBytes = (raw != null && !raw.isBlank())
                ? raw.getBytes(StandardCharsets.UTF_8)
                : UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8);
        this.ttlSeconds = (long) props.getAuth().getTokenTtlHours() * 3600;
    }

    public String issue(String username) {
        long exp = Instant.now().getEpochSecond() + ttlSeconds;
        String payload = username + "|" + exp;
        String header = "churn-v1";
        String b64Header = b64(header.getBytes(StandardCharsets.UTF_8));
        String b64Payload = b64(payload.getBytes(StandardCharsets.UTF_8));
        String sig = b64(hmac(b64Header + "." + b64Payload));
        return b64Header + "." + b64Payload + "." + sig;
    }

    public Optional<String> verify(String token) {
        if (token == null) return Optional.empty();
        String[] parts = token.split("\\.");
        if (parts.length != 3) return Optional.empty();
        String expectedSig = b64(hmac(parts[0] + "." + parts[1]));
        if (!constantTimeEquals(expectedSig, parts[2])) return Optional.empty();
        try {
            String payload = new String(Base64.getUrlDecoder().decode(parts[1]), StandardCharsets.UTF_8);
            String[] fields = payload.split("\\|", 2);
            if (fields.length != 2) return Optional.empty();
            long exp = Long.parseLong(fields[1]);
            if (Instant.now().getEpochSecond() > exp) return Optional.empty();
            return Optional.of(fields[0]);
        } catch (Exception e) {
            return Optional.empty();
        }
    }

    private byte[] hmac(String data) {
        try {
            Mac mac = Mac.getInstance(ALGORITHM);
            mac.init(new SecretKeySpec(secretBytes, ALGORITHM));
            return mac.doFinal(data.getBytes(StandardCharsets.UTF_8));
        } catch (Exception e) {
            throw new IllegalStateException("HMAC init failed", e);
        }
    }

    private static String b64(byte[] data) {
        return Base64.getUrlEncoder().withoutPadding().encodeToString(data);
    }

    // Timing-safe string comparison to prevent timing attacks.
    private static boolean constantTimeEquals(String a, String b) {
        if (a.length() != b.length()) return false;
        int diff = 0;
        for (int i = 0; i < a.length(); i++) {
            diff |= a.charAt(i) ^ b.charAt(i);
        }
        return diff == 0;
    }
}
