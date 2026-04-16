package com.churn.manager.project;

/**
 * Decrypted credentials for one project — used at runtime only, never persisted.
 */
public record DecryptedSecrets(
        String sshUsername,
        String sshPassword,
        String sshPrivateKey,
        String mysqlUrl,
        String mysqlUsername,
        String mysqlPassword,
        String hiveJdbcUrl,
        String hiveUsername,
        String hivePassword
) {}
