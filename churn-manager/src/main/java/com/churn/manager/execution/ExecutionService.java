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
    public PipelineTemplate createPipeline(Long projectId, String name, String description,
                                           List<Map<String, Object>> steps) {
        projectService.getOrThrow(projectId);
        validateSteps(steps);
        PipelineTemplate pt = new PipelineTemplate();
        pt.setProjectId(projectId);
        pt.setName(name);
        pt.setDescription(description);
        pt.setStepKeysJson(toJson(steps));
        return pipelineRepo.save(pt);
    }

    @Transactional
    public PipelineTemplate updatePipeline(Long projectId, Long pipelineId, String name,
                                           String description, List<Map<String, Object>> steps) {
        PipelineTemplate pt = pipelineRepo.findById(pipelineId)
                .orElseThrow(() -> new ApiException(HttpStatus.NOT_FOUND, "流水线不存在。"));
        if (!pt.getProjectId().equals(projectId)) {
            throw new ApiException(HttpStatus.FORBIDDEN, "流水线不属于该项目。");
        }
        validateSteps(steps);
        if (name != null && !name.isBlank()) pt.setName(name);
        if (description != null) pt.setDescription(description);
        pt.setStepKeysJson(toJson(steps));
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

        List<Map<String, Object>> steps = parseSteps(pt.getStepKeysJson());
        String jobId = UUID.randomUUID().toString().replace("-", "").substring(0, 12);
        String jobDir = resolveLogsDir(p) + "/" + appProperties.getJobs().getLogSubdir() + "/" + jobId;

        StringBuilder runScript = new StringBuilder("set -e\n");
        int seq = 1;
        for (Map<String, Object> step : steps) {
            String label = String.valueOf(step.getOrDefault("label", "Step " + seq));
            String startCmd = String.valueOf(step.getOrDefault("startCmd", ""));
            String postCmd  = String.valueOf(step.getOrDefault("postCmd", ""));
            runScript.append("echo '[STEP ").append(seq).append("] ").append(label).append("'\n");
            if (!startCmd.isBlank()) runScript.append(startCmd).append("\n");
            if (postCmd != null && !postCmd.isBlank()) {
                runScript.append("echo '[POST ").append(seq).append("] ").append(label).append("'\n");
                runScript.append(postCmd).append("\n");
            }
            seq++;
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
        // Resolve scripts directory: explicit > projectRoot/scripts > fail with helpful message
        String scripts;
        if (p.getScriptsDir() != null && !p.getScriptsDir().isBlank()) {
            scripts = p.getScriptsDir().stripTrailing();
        } else if (p.getProjectRoot() != null && !p.getProjectRoot().isBlank()) {
            scripts = p.getProjectRoot().stripTrailing() + "/scripts";
        } else {
            throw new ApiException(HttpStatus.BAD_REQUEST,
                    "请先在项目设置中配置「脚本目录」或「项目根目录」，否则无法定位脚本文件。");
        }
        String script = scripts + "/" + step.scriptFile();

        if ("spark".equals(step.executor())) {
            if (p.getSparkSubmitCommand() == null || p.getSparkSubmitCommand().isBlank()) {
                throw new ApiException(HttpStatus.BAD_REQUEST,
                        "Spark 类步骤需要配置「spark-submit 命令路径」（如 /opt/spark/bin/spark-submit --master yarn --deploy-mode client），请在项目设置中填写。");
            }
            return p.getSparkSubmitCommand().strip() + " " + script;
        } else {
            String pythonCmd = (p.getPythonCommand() != null && !p.getPythonCommand().isBlank())
                    ? p.getPythonCommand().strip() : "python3";
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
        // Use trap to reliably write exit code even when set -e triggers early exit
        return "#!/usr/bin/env bash\n"
                + "set -e\n"
                + "_jd=\"$(cd \"$(dirname \"$0\")\" && pwd)\"\n"
                + "trap 'echo $? > \"${_jd}/job.exit\"' EXIT\n"
                + ". \"${_jd}/env.sh\"\n"
                + "echo '[START] " + jobName + " @ $(date)'\n"
                + commands + "\n"
                + "echo '[END] " + jobName + " @ $(date)'\n";
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

    private void validateSteps(List<Map<String, Object>> steps) {
        if (steps == null || steps.isEmpty()) {
            throw new ApiException(HttpStatus.BAD_REQUEST, "流水线至少需要一个组件。");
        }
        for (Map<String, Object> step : steps) {
            Object cmd = step.get("startCmd");
            if (cmd == null || cmd.toString().isBlank()) {
                throw new ApiException(HttpStatus.BAD_REQUEST,
                        "组件「" + step.getOrDefault("label", "?") + "」缺少启动命令。");
            }
        }
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> parseSteps(String json) {
        try {
            List<?> list = objectMapper.readValue(json, List.class);
            if (list.isEmpty()) return List.of();
            if (list.get(0) instanceof String) {
                // Old format: ["s11","s20"] → convert via StepCatalog for backward compat
                return list.stream().map(key -> {
                    Map<String, Object> m = new LinkedHashMap<>();
                    try {
                        StepCatalog.StepDef def = stepCatalog.getOrThrow((String) key);
                        m.put("uid", key); m.put("type", "script");
                        m.put("label", def.displayName());
                        m.put("scriptFile", def.scriptFile());
                        m.put("startCmd", ""); m.put("postCmd", "");
                    } catch (Exception e) {
                        m.put("uid", key); m.put("type", "script");
                        m.put("label", key); m.put("startCmd", ""); m.put("postCmd", "");
                    }
                    return m;
                }).toList();
            }
            return (List<Map<String, Object>>) (List<?>) list;
        } catch (Exception e) {
            return List.of();
        }
    }

    private String toJson(Object obj) {
        try {
            return objectMapper.writeValueAsString(obj);
        } catch (Exception e) {
            return "[]";
        }
    }
}
