package com.churn.manager.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix = "app")
public class AppProperties {

    private final Auth auth = new Auth();
    private final Crypto crypto = new Crypto();
    private final Jobs jobs = new Jobs();
    private final Init init = new Init();

    public Auth getAuth() { return auth; }
    public Crypto getCrypto() { return crypto; }
    public Jobs getJobs() { return jobs; }
    public Init getInit() { return init; }

    public static class Auth {
        private String tokenSecret;
        private int tokenTtlHours = 12;

        public String getTokenSecret() { return tokenSecret; }
        public void setTokenSecret(String tokenSecret) { this.tokenSecret = tokenSecret; }
        public int getTokenTtlHours() { return tokenTtlHours; }
        public void setTokenTtlHours(int tokenTtlHours) { this.tokenTtlHours = tokenTtlHours; }
    }

    public static class Crypto {
        private String secretKey;

        public String getSecretKey() { return secretKey; }
        public void setSecretKey(String secretKey) { this.secretKey = secretKey; }
    }

    public static class Jobs {
        private String logSubdir = "manager-jobs";
        private int previewLines = 200;

        public String getLogSubdir() { return logSubdir; }
        public void setLogSubdir(String logSubdir) { this.logSubdir = logSubdir; }
        public int getPreviewLines() { return previewLines; }
        public void setPreviewLines(int previewLines) { this.previewLines = previewLines; }
    }

    public static class Init {
        private String adminUsername;
        private String adminPassword;

        public String getAdminUsername() { return adminUsername; }
        public void setAdminUsername(String adminUsername) { this.adminUsername = adminUsername; }
        public String getAdminPassword() { return adminPassword; }
        public void setAdminPassword(String adminPassword) { this.adminPassword = adminPassword; }
    }
}
