package com.churn.manager.execution;

import com.churn.manager.common.ApiException;
import com.churn.manager.config.AppProperties;
import com.churn.manager.project.DecryptedSecrets;
import com.churn.manager.project.ManagedProject;
import com.churn.manager.project.ProjectService;
import com.churn.manager.ssh.SshClient;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.*;

@Service
public class ExecutionService {

    private final ProjectService projectService;
    private final StepCatalog stepCatalog;
    private final PipelineTemplateRepository pipelineRepo;
    private final JobRecordRepository jobRepo;
    private final SshClient sshClient;
    private final AppProperties appProperties;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public ExecutionService(ProjectService projectService,
                            StepCatalog stepCatalog,
                            PipelineTemplateRepository pipelineRepo,
                            JobRecordRepository jobRepo,
                            SshClient sshClient,
                            AppProperties appProperties) {
        this.projectService = projectService;
        this.stepCatalog = stepCatalog;
        this.pipelineRepo = pipelineRepo;
        this.jobRepo = jobRepo;
        this.sshClient = sshClient;
        this.appProperties = appProperties;
    }

    // -------------------------------------------------------------------------
    // Pipeline CRUD
    // -------------------------------------------------------------------------

    @Transactional(readOnly = true)
    public List<PipelineTemplate> listPipelines(Long projectId) {
        projectService.getOrThrow(projectId);
        return pipelineRepo.findByProjectIdOrderByUpdatedAtDesc(projectId);
    }

    @Transactional
    public PipelineTemplate createPipeline(Long projectId, String name, String description, List<String> stepKeys) {
        projectService.getOrThrow(projectId);
        if (stepKeys == null || stepKeys.isEmpty()) {
            throw new ApiException(HttpStatus.BAD_REQUEST, "至少选择一个步骤。");
        }
        // Validate all keys exist
        stepKeys.forEach(k -> stepCatalog.getOrThrow(k));

        PipelineTemplate pt = new PipelineTemplate();
        pt.setProjectId(projectId);
        pt.setName(name);
        pt.setDescription(description);
        pt.setStepKeysJson(toJson(stepKeys));
        return pipelineRepo.save(pt);
    }

    @Transactional
    public void deletePipeline(Long projectId, Long pipelineId) {
        PipelineTemplate pt = pipelineRepo.findById(pipelineId)
                .orElseThrow(() -> new ApiException(HttpStatus.NOT_FOUND, "Pipeline 不存在。"));
        if (!pt.getProjectId().equals(projectId)) {
            throw new ApiException(HttpStatus.FORBIDDEN, "Pipeline 不属于该项目。");
        }
        pipelineRepo.delete(pt);
    }

    // -------------------------------------------------------------------------
    // Execution
    // -------------------------------------------------------------------------

    @Transactional
    public JobRecord runStep(Long projectId, String stepKey, Map<String, String> overrides, String triggerSource) {
        ManagedProject p = projectService.getOrThrow(projectId);
        StepCatalog.StepDef step = stepCatalog.getOrThrow(stepKey);
        DecryptedSecrets s = projectService.decryptSecrets(projectId);
        Map<String, String> env = projectService.buildEnvMap(p, s, overrides);

        String jobId = UUID.randomUUID().toString().replace("-", "").substring(0, 12);
        String jobDir = resolveLogsDir(p) + "/" + appProperties.getJobs().getLogSubdir() + "/" + jobId;
        String command = buildCommand(p, step);
        String envContent = buildEnvScript(env);
        String runContent = buildRunScript(step.displayName(), command);

        JobRecord job = new JobRecord();
        job.setProjectId(projectId);
        job.setStepKey(stepKey);
        job.setName(step.displayName());
        job.setStatus(JobStatus.PENDING);
        job.setTriggerSource(triggerSource != null ? triggerSource : "manual");
        job.setRemoteJobDir(jobDir);
        job.setLogPath(jobDir + "/job.log");
        job.setExitPath(jobDir + "/job.exit");
        jobRepo.save(job);

        try {
            String pid = sshClient.launchBackground(
                    p.getHost(), p.getSshPort(), s.sshUsername(), s.sshPassword(), s.sshPrivateKey(),
                    jobDir, envContent, runContent);
            job.setStatus(JobStatus.RUNNING);
            job.setStartedAt(Instant.now());
            job.setPid(pid);
        } catch (Exception e) {
            job.setStatus(JobStatus.FAILED);
            job.setFinishedAt(Instant.now());
        }
        return jobRepo.save(job);
    }

    @Transactional
    public JobRecord runPipeline(Long projectId, Long pipelineId, Map<String, String> overrides, String triggerSource) {
        ManagedProject p = projectService.getOrThrow(projectId);
        PipelineTemplate pt = pipelineRepo.findById(pipelineId)
                .orElseThrow(() -> new ApiException(HttpStatus.NOT_FOUND, "Pipeline 不存在。"));
        DecryptedSecrets s = projectService.decryptSecrets(projectId);
        Map<String, String> env = projectService.buildEnvMap(p, s, overrides);

        List<String> stepKeys = fromJson(pt.getStepKeysJson());
        String jobId = UUID.randomUUID().toString().replace("-", "").substring(0, 12);
        String jobDir = resolveLogsDir(p) + "/" + appProperties.getJobs().getLogSubdir() + "/" + jobId;

        StringBuilder runScript = new StringBuilder("set -e\n");
        for (String key : stepKeys) {
            StepCatalog.StepDef step = stepCatalog.getOrThrow(key);
            runScript.append("echo '[STEP ").append(key).append("] ").append(step.displayName()).append("'\n");
            runScript.append(buildCommand(p, step)).append("\n");
        }
        String envContent = buildEnvScript(env);
        String runContent = buildRunScript(pt.getName(), runScript.toString());

        JobRecord job = new JobRecord();
        job.setProjectId(projectId);
        job.setPipelineId(pipelineId);
        job.setName("[Pipeline] " + pt.getName());
        job.setStatus(JobStatus.PENDING);
        job.setTriggerSource(triggerSource != null ? triggerSource : "manual");
        job.setRemoteJobDir(jobDir);
        job.setLogPath(jobDir + "/job.log");
        job.setExitPath(jobDir + "/job.exit");
        jobRepo.save(job);

        try {
            String pid = sshClient.launchBackground(
                    p.getHost(), p.getSshPort(), s.sshUsername(), s.sshPassword(), s.sshPrivateKey(),
                    jobDir, envContent, runContent);
            job.setStatus(JobStatus.RUNNING);
            job.setStartedAt(Instant.now());
            job.setPid(pid);
        } catch (Exception e) {
            job.setStatus(JobStatus.FAILED);
            job.setFinishedAt(Instant.now());
        }
        return jobRepo.save(job);
    }

    // -------------------------------------------------------------------------
    // Job status & log
    // -------------------------------------------------------------------------

    @Transactional(readOnly = true)
    public org.springframework.data.domain.Page<JobRecord> listJobs(Long projectId,
                                                                     org.springframework.data.domain.Pageable pageable) {
        projectService.getOrThrow(projectId);
        return jobRepo.findByProjectIdOrderByCreatedAtDesc(projectId, pageable);
    }

    @Transactional
    public JobRecord refreshAndGet(Long jobId) {
        JobRecord job = jobRepo.findById(jobId)
                .orElseThrow(() -> new ApiException(HttpStatus.NOT_FOUND, "任务不存在。"));
        if (job.getStatus() == JobStatus.RUNNING) {
            refreshStatus(job);
            jobRepo.save(job);
        }
        return job;
    }

    @Transactional(readOnly = true)
    public String getLog(Long jobId) {
        JobRecord job = jobRepo.findById(jobId)
                .orElseThrow(() -> new ApiException(HttpStatus.NOT_FOUND, "任务不存在。"));
        if (job.getLogPath() == null) return "(暂无日志)";
        ManagedProject p = projectService.getOrThrow(job.getProjectId());
        DecryptedSecrets s = projectService.decryptSecrets(job.getProjectId());
        String content = sshClient.readFileContent(
                p.getHost(), p.getSshPort(), s.sshUsername(), s.sshPassword(), s.sshPrivateKey(),
                job.getLogPath(), appProperties.getJobs().getPreviewLines());
        return content != null ? content : "(日志文件为空或尚未生成)";
    }

    // -------------------------------------------------------------------------
    // Private helpers
    // -------------------------------------------------------------------------

    private void refreshStatus(JobRecord job) {
        ManagedProject p = projectService.getOrThrow(job.getProjectId());
        DecryptedSecrets s = projectService.decryptSecrets(job.getProjectId());
        // First check exit file
        String exitContent = sshClient.readRemoteFile(
                p.getHost(), p.getSshPort(), s.sshUsername(), s.sshPassword(), s.sshPrivateKey(),
                job.getExitPath());
        if (exitContent != null) {
            int code;
            try { code = Integer.parseInt(exitContent.trim()); } catch (NumberFormatException e) { code = 1; }
            job.setStatus(code == 0 ? JobStatus.SUCCESS : JobStatus.FAILED);
            job.setFinishedAt(Instant.now());
            return;
        }
        // Fall back to process check
        if (job.getPid() != null) {
            boolean alive = sshClient.isProcessAlive(
                    p.getHost(), p.getSshPort(), s.sshUsername(), s.sshPassword(), s.sshPrivateKey(),
                    job.getPid());
            if (!alive) {
                job.setStatus(JobStatus.FAILED);
                job.setFinishedAt(Instant.now());
            }
        }
    }

    private String buildCommand(ManagedProject p, StepCatalog.StepDef step) {
        String scripts = p.getScriptsDir() != null ? p.getScriptsDir() : "$TD_CHURN_PROJECT_DIR/scripts";
        String script = scripts + "/" + step.scriptFile();
        if ("spark".equals(step.executor())) {
            String sparkCmd = p.getSparkSubmitCommand() != null
                    ? p.getSparkSubmitCommand()
                    : "spark-submit --master yarn --deploy-mode client";
            return sparkCmd + " " + script;
        } else {
            String pythonCmd = p.getPythonCommand() != null ? p.getPythonCommand() : "python3";
            return pythonCmd + " " + script;
        }
    }

    private String buildEnvScript(Map<String, String> env) {
        StringBuilder sb = new StringBuilder("#!/usr/bin/env bash\n");
        for (Map.Entry<String, String> entry : env.entrySet()) {
            sb.append("export ").append(entry.getKey()).append("=")
              .append(singleQuote(entry.getValue())).append("\n");
        }
        return sb.toString();
    }

    private String buildRunScript(String jobName, String commands) {
        return "#!/usr/bin/env bash\n"
                + "set -e\n"
                + ". \"$(dirname \"$0\")/env.sh\"\n"
                + "echo '[START] " + jobName + " @ $(date)'\n"
                + commands + "\n"
                + "EXIT_CODE=$?\n"
                + "echo \"$EXIT_CODE\" > \"$(dirname \"$0\")/job.exit\"\n"
                + "echo '[END] exit=$EXIT_CODE @ $(date)'\n"
                + "exit $EXIT_CODE\n";
    }

    private String resolveLogsDir(ManagedProject p) {
        if (p.getLogsDir() != null && !p.getLogsDir().isBlank()) return p.getLogsDir();
        if (p.getProjectRoot() != null && !p.getProjectRoot().isBlank()) return p.getProjectRoot() + "/logs";
        return "/tmp/churn-logs";
    }

    private static String singleQuote(String v) {
        if (v == null) return "''";
        return "'" + v.replace("'", "'\\''") + "'";
    }

    @SuppressWarnings("unchecked")
    private List<String> fromJson(String json) {
        try {
            return objectMapper.readValue(json, List.class);
        } catch (Exception e) {
            return List.of();
        }
    }

    private String toJson(List<String> list) {
        try {
            return objectMapper.writeValueAsString(list);
        } catch (Exception e) {
            return "[]";
        }
    }
}
