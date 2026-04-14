package com.userchurn.manager.service;

import com.userchurn.manager.domain.ManagedProject;
import com.userchurn.manager.domain.ProjectSecret;
import com.userchurn.manager.repo.ManagedProjectRepository;
import com.userchurn.manager.web.ApiException;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Service
public class ProjectService {

    public record ProjectCommand(
        String name,
        String description,
        String host,
        Integer sshPort,
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
        String pythonCommand,
        String sparkSubmitCommand,
        String beelineCommand,
        String sshUsername,
        String sshPassword,
        String sshPrivateKey,
        String mysqlUrl,
        String mysqlUsername,
        String mysqlPassword,
        String hiveJdbcUrl,
        String hiveUsername,
        String hivePassword
    ) {
    }

    private final ManagedProjectRepository managedProjectRepository;
    private final SecretCryptoService secretCryptoService;
    private final PipelineCatalogService pipelineCatalogService;

    public ProjectService(ManagedProjectRepository managedProjectRepository,
                          SecretCryptoService secretCryptoService,
                          PipelineCatalogService pipelineCatalogService) {
        this.managedProjectRepository = managedProjectRepository;
        this.secretCryptoService = secretCryptoService;
        this.pipelineCatalogService = pipelineCatalogService;
    }

    @Transactional(readOnly = true)
    public List<Map<String, Object>> listProjects() {
        return managedProjectRepository.findAllByOrderByUpdatedAtDesc().stream()
            .map(this::toProjectView)
            .toList();
    }

    @Transactional(readOnly = true)
    public Map<String, Object> getProjectView(Long projectId) {
        return toProjectView(getProject(projectId));
    }

    @Transactional
    public Map<String, Object> createProject(ProjectCommand command) {
        String normalizedName = required(command.name(), "项目名称不能为空。");
        if (managedProjectRepository.existsByNameIgnoreCase(normalizedName)) {
            throw new ApiException(HttpStatus.CONFLICT, "项目名称已存在，请使用其他名称。" );
        }

        ManagedProject project = new ManagedProject();
        applyCommand(project, command, true);
        managedProjectRepository.save(project);
        pipelineCatalogService.ensureDefaultPipeline(project);
        return toProjectView(project);
    }

    @Transactional
    public Map<String, Object> updateProject(Long projectId, ProjectCommand command) {
        ManagedProject project = getProject(projectId);
        String normalizedName = required(command.name(), "项目名称不能为空。");
        if (!project.getName().equalsIgnoreCase(normalizedName) && managedProjectRepository.existsByNameIgnoreCase(normalizedName)) {
            throw new ApiException(HttpStatus.CONFLICT, "项目名称已存在，请使用其他名称。" );
        }

        applyCommand(project, command, false);
        managedProjectRepository.save(project);
        return toProjectView(project);
    }

    @Transactional(readOnly = true)
    public ManagedProject getProject(Long projectId) {
        ManagedProject project = managedProjectRepository.findById(projectId)
            .orElseThrow(() -> new ApiException(HttpStatus.NOT_FOUND, "项目不存在。"));
        if (project.getSecret() == null) {
            project.attachSecret(new ProjectSecret());
        }
        return project;
    }

    @Transactional(readOnly = true)
    public Map<String, String> buildRuntimeEnv(ManagedProject project, Map<String, String> overrides) {
        ManagedProject actualProject = getProject(project.getId());
        ProjectSecret secret = Optional.ofNullable(actualProject.getSecret()).orElse(new ProjectSecret());
        Map<String, String> env = new LinkedHashMap<>();
        env.put("TD_CHURN_PROJECT_DIR", fallback(actualProject.getProjectRoot(), "/home/hadoop/td_churn_project"));
        env.put("TD_CHURN_ORIGIN_DIR", fallback(actualProject.getOriginDataDir(), "/DataSet_Origin"));
        env.put("TD_CHURN_TRANSFORMED_DIR", fallback(actualProject.getTransformedDataDir(), "/DataSet_Transformed"));
        env.put("TD_CHURN_BATTLE_LOG_BY_DT_DIR", fallback(actualProject.getTransformedDataDir(), "/DataSet_Transformed") + "/battle_log_by_dt");
        env.put("TD_CHURN_ETL_OUTPUT_DIR", fallback(actualProject.getProjectRoot(), "/home/hadoop/td_churn_project") + "/etl_output_v2");
        env.put("TD_CHURN_MODEL_DIR", fallback(actualProject.getProjectRoot(), "/home/hadoop/td_churn_project") + "/models");
        env.put("TD_CHURN_ALERT_OUTPUT_DIR", fallback(actualProject.getAlertOutputDir(), fallback(actualProject.getProjectRoot(), "/home/hadoop/td_churn_project") + "/alert_output"));
        env.put("TD_CHURN_EXPERIMENT_DIR", fallback(actualProject.getExperimentResultsDir(), fallback(actualProject.getProjectRoot(), "/home/hadoop/td_churn_project") + "/experiment_results"));
        env.put("TD_CHURN_PLOT_DIR", fallback(actualProject.getPlotsDir(), fallback(actualProject.getProjectRoot(), "/home/hadoop/td_churn_project") + "/plots"));
        env.put("TD_CHURN_HDFS_LANDING_DIR", fallback(actualProject.getHdfsLandingDir(), "/data/td_churn/landing"));
        env.put("TD_CHURN_HIVE_DB", fallback(actualProject.getHiveDb(), "td_churn"));
        env.put("TD_CHURN_DB_URL", decrypt(secret.getMysqlUrlEncrypted()));
        env.put("TD_CHURN_DB_USER", decrypt(secret.getMysqlUsernameEncrypted()));
        env.put("TD_CHURN_DB_PASSWORD", decrypt(secret.getMysqlPasswordEncrypted()));

        if (overrides != null) {
            overrides.forEach((key, value) -> {
                if (value != null && !value.isBlank()) {
                    env.put(key, value);
                }
            });
        }
        env.entrySet().removeIf(entry -> entry.getValue() == null || entry.getValue().isBlank());
        return env;
    }

    private void applyCommand(ManagedProject project, ProjectCommand command, boolean creating) {
        String projectRoot = fallback(command.projectRoot(), "/home/hadoop/td_churn_project");

        project.setName(required(command.name(), "项目名称不能为空。"));
        project.setDescription(trimToNull(command.description()));
        project.setHost(required(command.host(), "虚拟机主机 IP 或域名不能为空。"));
        project.setSshPort(command.sshPort() == null ? 22 : command.sshPort());
        project.setProjectRoot(projectRoot);
        project.setScriptsDir(fallback(command.scriptsDir(), projectRoot + "/scripts"));
        project.setAlertOutputDir(fallback(command.alertOutputDir(), projectRoot + "/alert_output"));
        project.setExperimentResultsDir(fallback(command.experimentResultsDir(), projectRoot + "/experiment_results"));
        project.setPlotsDir(fallback(command.plotsDir(), projectRoot + "/plots"));
        project.setLogsDir(fallback(command.logsDir(), projectRoot + "/logs"));
        project.setOriginDataDir(fallback(command.originDataDir(), "/DataSet_Origin"));
        project.setTransformedDataDir(fallback(command.transformedDataDir(), "/DataSet_Transformed"));
        project.setHiveDb(fallback(command.hiveDb(), "td_churn"));
        project.setHdfsLandingDir(fallback(command.hdfsLandingDir(), "/data/td_churn/landing"));
        project.setPythonCommand(fallback(command.pythonCommand(), "python3"));
        project.setSparkSubmitCommand(fallback(command.sparkSubmitCommand(), "spark-submit --master yarn --deploy-mode client"));
        project.setBeelineCommand(fallback(command.beelineCommand(), "beeline"));

        ProjectSecret secret = Optional.ofNullable(project.getSecret()).orElseGet(ProjectSecret::new);
        if (creating) {
            project.attachSecret(secret);
        }

        secret.setSshUsernameEncrypted(updateSecret(secret.getSshUsernameEncrypted(), command.sshUsername()));
        secret.setSshPasswordEncrypted(updateSecret(secret.getSshPasswordEncrypted(), command.sshPassword()));
        secret.setSshPrivateKeyEncrypted(updateSecret(secret.getSshPrivateKeyEncrypted(), command.sshPrivateKey()));
        secret.setMysqlUrlEncrypted(updateSecret(secret.getMysqlUrlEncrypted(), command.mysqlUrl()));
        secret.setMysqlUsernameEncrypted(updateSecret(secret.getMysqlUsernameEncrypted(), command.mysqlUsername()));
        secret.setMysqlPasswordEncrypted(updateSecret(secret.getMysqlPasswordEncrypted(), command.mysqlPassword()));
        secret.setHiveJdbcUrlEncrypted(updateSecret(secret.getHiveJdbcUrlEncrypted(), command.hiveJdbcUrl()));
        secret.setHiveUsernameEncrypted(updateSecret(secret.getHiveUsernameEncrypted(), command.hiveUsername()));
        secret.setHivePasswordEncrypted(updateSecret(secret.getHivePasswordEncrypted(), command.hivePassword()));
        project.attachSecret(secret);
    }

    private Map<String, Object> toProjectView(ManagedProject project) {
        ProjectSecret secret = Optional.ofNullable(project.getSecret()).orElse(new ProjectSecret());
        Map<String, Object> view = new LinkedHashMap<>();
        view.put("id", project.getId());
        view.put("name", project.getName());
        view.put("description", project.getDescription());
        view.put("host", project.getHost());
        view.put("sshPort", project.getSshPort());
        view.put("projectRoot", project.getProjectRoot());
        view.put("scriptsDir", project.getScriptsDir());
        view.put("alertOutputDir", project.getAlertOutputDir());
        view.put("experimentResultsDir", project.getExperimentResultsDir());
        view.put("plotsDir", project.getPlotsDir());
        view.put("logsDir", project.getLogsDir());
        view.put("originDataDir", project.getOriginDataDir());
        view.put("transformedDataDir", project.getTransformedDataDir());
        view.put("hiveDb", project.getHiveDb());
        view.put("hdfsLandingDir", project.getHdfsLandingDir());
        view.put("pythonCommand", project.getPythonCommand());
        view.put("sparkSubmitCommand", project.getSparkSubmitCommand());
        view.put("beelineCommand", project.getBeelineCommand());
        view.put("lastStatDate", project.getLastStatDate());
        view.put("createdAt", project.getCreatedAt());
        view.put("updatedAt", project.getUpdatedAt());
        view.put("sshUsernameConfigured", secret.getSshUsernameEncrypted() != null);
        view.put("sshPasswordConfigured", secret.getSshPasswordEncrypted() != null);
        view.put("sshPrivateKeyConfigured", secret.getSshPrivateKeyEncrypted() != null);
        view.put("mysqlConfigured", secret.getMysqlUrlEncrypted() != null && secret.getMysqlUsernameEncrypted() != null);
        view.put("hiveConfigured", secret.getHiveJdbcUrlEncrypted() != null);
        return view;
    }

    private String updateSecret(String existingEncrypted, String newPlainText) {
        if (newPlainText == null) {
            return existingEncrypted;
        }
        String trimmed = trimToNull(newPlainText);
        if (trimmed == null) {
            return existingEncrypted;
        }
        return secretCryptoService.encrypt(trimmed);
    }

    private String decrypt(String encrypted) {
        return encrypted == null ? null : secretCryptoService.decrypt(encrypted);
    }

    private String required(String value, String message) {
        String trimmed = trimToNull(value);
        if (trimmed == null) {
            throw new ApiException(HttpStatus.BAD_REQUEST, message);
        }
        return trimmed;
    }

    private String fallback(String value, String fallback) {
        String trimmed = trimToNull(value);
        return trimmed == null ? fallback : trimmed;
    }

    private String trimToNull(String value) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }
}
