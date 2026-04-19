package com.churn.manager.project;

/**
 * Unified request body for both project creation and update.
 * All fields are nullable; null means "do not change" during update.
 * Password fields are ignored (not updated) if blank.
 */
public record ProjectRequest(
        // Basic
        String name,
        String description,
        String host,
        Integer sshPort,
        // Directories
        String projectRoot,
        String scriptsDir,
        String alertOutputDir,
        String experimentResultsDir,
        String plotsDir,
        String logsDir,
        String originDataDir,
        String transformedDataDir,
        String hiveDb,
        String hdfsLandingDir,
        // Commands
        String pythonCommand,
        String sparkSubmitCommand,
        String beelineCommand,
        String hadoopConfDir,
        String yarnConfDir,
        // SSH credentials (sensitive)
        String sshUsername,
        String sshPassword,
        String sshPrivateKey,
        // MySQL credentials (sensitive)
        String mysqlUrl,
        String mysqlUsername,
        String mysqlPassword,
        // Hive credentials (sensitive)
        String hiveJdbcUrl,
        String hiveUsername,
        String hivePassword
) {}
