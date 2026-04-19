package com.churn.manager.project;

import com.churn.manager.common.ApiException;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class ProjectService {

    private final ManagedProjectRepository projectRepo;
    private final ProjectSecretRepository secretRepo;
    private final CryptoService crypto;

    public ProjectService(ManagedProjectRepository projectRepo,
                          ProjectSecretRepository secretRepo,
                          CryptoService crypto) {
        this.projectRepo = projectRepo;
        this.secretRepo = secretRepo;
        this.crypto = crypto;
    }

    @Transactional(readOnly = true)
    public List<ManagedProject> listAll() {
        return projectRepo.findAll();
    }

    @Transactional(readOnly = true)
    public ManagedProject getOrThrow(Long id) {
        return projectRepo.findById(id)
                .orElseThrow(() -> new ApiException(HttpStatus.NOT_FOUND, "项目不存在：" + id));
    }

    @Transactional
    public ManagedProject create(ProjectRequest req) {
        if (req.name() == null || req.name().isBlank()) {
            throw new ApiException(HttpStatus.BAD_REQUEST, "项目名称不能为空。");
        }
        if (req.host() == null || req.host().isBlank()) {
            throw new ApiException(HttpStatus.BAD_REQUEST, "主机地址不能为空。");
        }
        ManagedProject p = new ManagedProject();
        applyRequest(p, req);
        projectRepo.save(p);
        upsertSecret(p, req, true);
        return p;
    }

    @Transactional
    public ManagedProject update(Long id, ProjectRequest req) {
        ManagedProject p = getOrThrow(id);
        applyRequest(p, req);
        projectRepo.save(p);
        upsertSecret(p, req, false);
        return p;
    }

    @Transactional
    public void delete(Long id) {
        ManagedProject p = getOrThrow(id);
        projectRepo.delete(p);
    }

    @Transactional(readOnly = true)
    public DecryptedSecrets decryptSecrets(Long projectId) {
        ProjectSecret s = secretRepo.findByProjectId(projectId).orElse(null);
        if (s == null) return new DecryptedSecrets(null, null, null, null, null, null, null, null, null);
        return new DecryptedSecrets(
                s.getSshUsername(),
                crypto.decrypt(s.getSshPasswordEnc()),
                crypto.decrypt(s.getSshPrivateKeyEnc()),
                s.getMysqlUrl(),
                s.getMysqlUsername(),
                crypto.decrypt(s.getMysqlPasswordEnc()),
                s.getHiveJdbcUrl(),
                s.getHiveUsername(),
                crypto.decrypt(s.getHivePasswordEnc())
        );
    }

    /**
     * Build the environment variable map injected into each remote script execution.
     * Overrides from the caller (runtimeOverrides) take highest priority.
     */
    public Map<String, String> buildEnvMap(ManagedProject p, DecryptedSecrets s,
                                           Map<String, String> runtimeOverrides) {
        Map<String, String> env = new HashMap<>();
        String root = p.getProjectRoot();
        boolean hasRoot = root != null && !root.isBlank();

        putIfNotNull(env, "TD_CHURN_PROJECT_DIR", root);
        putIfNotNull(env, "TD_CHURN_SCRIPTS_DIR",
                fallback(p.getScriptsDir(), hasRoot ? root + "/scripts" : null));
        putIfNotNull(env, "TD_CHURN_ORIGIN_DIR", p.getOriginDataDir());
        putIfNotNull(env, "TD_CHURN_TRANSFORMED_DIR", p.getTransformedDataDir());
        putIfNotNull(env, "TD_CHURN_ETL_OUTPUT_DIR",
                fallback(null, hasRoot ? root + "/etl_output_v2" : null));
        putIfNotNull(env, "TD_CHURN_MODEL_DIR",
                fallback(null, hasRoot ? root + "/models" : null));
        putIfNotNull(env, "TD_CHURN_ALERT_OUTPUT_DIR",
                fallback(p.getAlertOutputDir(), hasRoot ? root + "/alert_output" : null));
        putIfNotNull(env, "TD_CHURN_EXPERIMENT_DIR",
                fallback(p.getExperimentResultsDir(), hasRoot ? root + "/experiment_results" : null));
        putIfNotNull(env, "TD_CHURN_PLOT_DIR",
                fallback(p.getPlotsDir(), hasRoot ? root + "/plots" : null));
        putIfNotNull(env, "TD_CHURN_LOGS_DIR",
                fallback(p.getLogsDir(), hasRoot ? root + "/logs" : null));
        putIfNotNull(env, "TD_CHURN_HDFS_LANDING_DIR", p.getHdfsLandingDir());
        putIfNotNull(env, "TD_CHURN_HIVE_DB", p.getHiveDb());
        // Hadoop/YARN 环境变量（spark-submit --master yarn 必需）
        putIfNotNull(env, "HADOOP_CONF_DIR", p.getHadoopConfDir());
        putIfNotNull(env, "YARN_CONF_DIR",
                p.getYarnConfDir() != null && !p.getYarnConfDir().isBlank()
                        ? p.getYarnConfDir() : p.getHadoopConfDir()); // 未单独配置时用同一目录兼容
        // 从 sparkSubmitCommand 推导 SPARK_HOME（Python 脚本的 MySQL JAR 自动探测需要）
        String sparkCmd = p.getSparkSubmitCommand();
        if (sparkCmd != null) {
            int idx = sparkCmd.indexOf("/bin/spark-submit");
            if (idx > 0) {
                putIfNotNull(env, "SPARK_HOME", sparkCmd.substring(0, idx));
            }
        }
        // Database credentials
        putIfNotNull(env, "TD_CHURN_DB_URL", s.mysqlUrl());
        putIfNotNull(env, "TD_CHURN_DB_USER", s.mysqlUsername());
        putIfNotNull(env, "TD_CHURN_DB_PASSWORD", s.mysqlPassword());
        // Runtime overrides win
        if (runtimeOverrides != null) env.putAll(runtimeOverrides);
        return env;
    }

    // --- private helpers ---

    /** Return the first non-blank value, or null. */
    private static String fallback(String primary, String secondary) {
        if (primary != null && !primary.isBlank()) return primary;
        return secondary;
    }

    private void applyRequest(ManagedProject p, ProjectRequest req) {
        if (req.name() != null && !req.name().isBlank()) p.setName(req.name().trim());
        if (req.description() != null) p.setDescription(req.description());
        if (req.host() != null && !req.host().isBlank()) p.setHost(req.host().trim());
        if (req.sshPort() != null) p.setSshPort(req.sshPort());
        
        if (req.projectRoot() != null) p.setProjectRoot(req.projectRoot());
        if (req.scriptsDir() != null) p.setScriptsDir(req.scriptsDir());
        if (req.alertOutputDir() != null) p.setAlertOutputDir(req.alertOutputDir());
        if (req.experimentResultsDir() != null) p.setExperimentResultsDir(req.experimentResultsDir());
        if (req.plotsDir() != null) p.setPlotsDir(req.plotsDir());
        if (req.logsDir() != null) p.setLogsDir(req.logsDir());
        if (req.originDataDir() != null) p.setOriginDataDir(req.originDataDir());
        if (req.transformedDataDir() != null) p.setTransformedDataDir(req.transformedDataDir());
        
        if (req.hiveDb() != null) p.setHiveDb(req.hiveDb());
        if (req.hdfsLandingDir() != null) p.setHdfsLandingDir(req.hdfsLandingDir());
        if (req.pythonCommand() != null) p.setPythonCommand(req.pythonCommand());
        if (req.sparkSubmitCommand() != null) p.setSparkSubmitCommand(req.sparkSubmitCommand());
        if (req.beelineCommand() != null) p.setBeelineCommand(req.beelineCommand());
        if (req.hadoopConfDir() != null) p.setHadoopConfDir(req.hadoopConfDir());
        if (req.yarnConfDir() != null) p.setYarnConfDir(req.yarnConfDir());
        if (req.hadoopConfDir() != null) p.setHadoopConfDir(req.hadoopConfDir());
        if (req.yarnConfDir() != null) p.setYarnConfDir(req.yarnConfDir());
    }

    private void upsertSecret(ManagedProject p, ProjectRequest req, boolean isCreate) {
        ProjectSecret s = secretRepo.findByProjectId(p.getId()).orElse(null);
        if (s == null) {
            s = new ProjectSecret();
            s.setProject(p);
        }
        if (req.sshUsername() != null) s.setSshUsername(req.sshUsername());
        // For password fields: only update if a non-blank value is provided
        if (req.sshPassword() != null && !req.sshPassword().isBlank())
            s.setSshPasswordEnc(crypto.encrypt(req.sshPassword()));
        if (req.sshPrivateKey() != null && !req.sshPrivateKey().isBlank())
            s.setSshPrivateKeyEnc(crypto.encrypt(req.sshPrivateKey()));
        if (req.mysqlUrl() != null) s.setMysqlUrl(req.mysqlUrl());
        if (req.mysqlUsername() != null) s.setMysqlUsername(req.mysqlUsername());
        if (req.mysqlPassword() != null && !req.mysqlPassword().isBlank())
            s.setMysqlPasswordEnc(crypto.encrypt(req.mysqlPassword()));
        if (req.hiveJdbcUrl() != null) s.setHiveJdbcUrl(req.hiveJdbcUrl());
        if (req.hiveUsername() != null) s.setHiveUsername(req.hiveUsername());
        if (req.hivePassword() != null && !req.hivePassword().isBlank())
            s.setHivePasswordEnc(crypto.encrypt(req.hivePassword()));
        secretRepo.save(s);
    }

    private static void putIfNotNull(Map<String, String> map, String key, String value) {
        if (value != null && !value.isBlank()) map.put(key, value);
    }
}
