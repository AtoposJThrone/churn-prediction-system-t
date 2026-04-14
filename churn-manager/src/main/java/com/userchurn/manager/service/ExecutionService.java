package com.userchurn.manager.service;

import com.userchurn.manager.domain.JobRecord;
import com.userchurn.manager.domain.JobStatus;
import com.userchurn.manager.domain.ManagedProject;
import com.userchurn.manager.domain.PipelineStep;
import com.userchurn.manager.domain.PipelineTemplate;
import com.userchurn.manager.repo.JobRecordRepository;
import com.userchurn.manager.repo.PipelineTemplateRepository;
import com.userchurn.manager.web.ApiException;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
public class ExecutionService {

    private final ProjectService projectService;
    private final PipelineCatalogService pipelineCatalogService;
    private final PipelineTemplateRepository pipelineTemplateRepository;
    private final JobRecordRepository jobRecordRepository;
    private final RemoteAccessService remoteAccessService;

    public ExecutionService(ProjectService projectService,
                            PipelineCatalogService pipelineCatalogService,
                            PipelineTemplateRepository pipelineTemplateRepository,
                            JobRecordRepository jobRecordRepository,
                            RemoteAccessService remoteAccessService) {
        this.projectService = projectService;
        this.pipelineCatalogService = pipelineCatalogService;
        this.pipelineTemplateRepository = pipelineTemplateRepository;
        this.jobRecordRepository = jobRecordRepository;
        this.remoteAccessService = remoteAccessService;
    }

    @Transactional(readOnly = true)
    public List<Map<String, Object>> listCatalog() {
        return pipelineCatalogService.listCatalog().stream().map(step -> {
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("key", step.key());
            row.put("displayName", step.displayName());
            row.put("description", step.description());
            row.put("commandTemplate", step.commandTemplate());
            return row;
        }).toList();
    }

    @Transactional(readOnly = true)
    public List<Map<String, Object>> listPipelines(Long projectId) {
        projectService.getProject(projectId);
        return pipelineTemplateRepository.findByProjectIdOrderByUpdatedAtDesc(projectId).stream()
            .map(this::toPipelineView)
            .toList();
    }

    @Transactional
    public Map<String, Object> createPipeline(Long projectId, String name, String description, List<String> stepKeys) {
        ManagedProject project = projectService.getProject(projectId);
        if (stepKeys == null || stepKeys.isEmpty()) {
            throw new ApiException(HttpStatus.BAD_REQUEST, "至少需要选择一个脚本步骤。" );
        }
        PipelineTemplate pipeline = pipelineCatalogService.createPipeline(project, name, description, stepKeys, false);
        return toPipelineView(pipeline);
    }

    @Transactional
    public Map<String, Object> runSingleStep(Long projectId, String stepKey, Map<String, String> runtimeOverrides, String triggerSource) {
        ManagedProject project = projectService.getProject(projectId);
        PipelineCatalogService.StepDefinition step = pipelineCatalogService.getRequiredStep(stepKey);

        String renderedCommand = pipelineCatalogService.renderCommand(project, step);
        String shellBody = "echo '[STEP] " + step.displayName() + "'\n" + renderedCommand;
        Map<String, String> envVars = projectService.buildRuntimeEnv(project, runtimeOverrides);
        RemoteAccessService.LaunchedJob launchedJob = remoteAccessService.launchBackgroundJob(project, shellBody, envVars);

        JobRecord job = new JobRecord();
        job.setProject(project);
        job.setStepKey(step.key());
        job.setDisplayName(step.displayName());
        job.setCommandTemplate(step.commandTemplate());
        job.setResolvedCommandMasked(renderedCommand);
        job.setRemoteJobDir(launchedJob.remoteJobDir());
        job.setLogPath(launchedJob.logPath());
        job.setExitPath(launchedJob.exitPath());
        job.setPidPath(launchedJob.pidPath());
        job.setStatus(JobStatus.RUNNING);
        job.setTriggerSource(triggerSource);
        job.setStartedAt(Instant.now());
        jobRecordRepository.save(job);

        return toJobView(job);
    }

    @Transactional
    public Map<String, Object> runPipeline(Long projectId, Long pipelineId, Map<String, String> runtimeOverrides, String triggerSource) {
        ManagedProject project = projectService.getProject(projectId);
        PipelineTemplate pipeline = getPipeline(projectId, pipelineId);

        String shellBody = pipeline.getSteps().stream()
            .sorted((left, right) -> Integer.compare(left.getStepOrder(), right.getStepOrder()))
            .map(step -> {
                PipelineCatalogService.StepDefinition definition = pipelineCatalogService.getRequiredStep(step.getStepKey());
                return "echo '[STEP] " + definition.displayName() + "'\n" + pipelineCatalogService.renderCommand(project, definition);
            })
            .collect(Collectors.joining("\n\n"));

        String masked = pipeline.getSteps().stream()
            .map(PipelineStep::getDisplayName)
            .collect(Collectors.joining(" -> "));

        Map<String, String> envVars = projectService.buildRuntimeEnv(project, runtimeOverrides);
        RemoteAccessService.LaunchedJob launchedJob = remoteAccessService.launchBackgroundJob(project, shellBody, envVars);

        JobRecord job = new JobRecord();
        job.setProject(project);
        job.setPipeline(pipeline);
        job.setStepKey("PIPELINE");
        job.setDisplayName(pipeline.getName());
        job.setCommandTemplate(pipeline.getSteps().stream().map(PipelineStep::getCommandTemplate).collect(Collectors.joining("\n")));
        job.setResolvedCommandMasked(masked);
        job.setRemoteJobDir(launchedJob.remoteJobDir());
        job.setLogPath(launchedJob.logPath());
        job.setExitPath(launchedJob.exitPath());
        job.setPidPath(launchedJob.pidPath());
        job.setStatus(JobStatus.RUNNING);
        job.setTriggerSource(triggerSource);
        job.setStartedAt(Instant.now());
        jobRecordRepository.save(job);

        return toJobView(job);
    }

    @Transactional
    public List<Map<String, Object>> listJobs(Long projectId) {
        projectService.getProject(projectId);
        List<JobRecord> jobs = jobRecordRepository.findByProjectIdOrderByCreatedAtDesc(projectId);
        List<Map<String, Object>> result = new ArrayList<>();
        for (JobRecord job : jobs) {
            result.add(refreshAndBuildJobView(job));
        }
        return result;
    }

    @Transactional
    public Map<String, Object> getJobLog(Long jobId) {
        JobRecord job = getJob(jobId);
        Map<String, Object> refreshed = refreshAndBuildJobView(job);
        String logPreview = job.getLogPath() == null ? "" : remoteAccessService.tailRemoteFile(job.getProject(), job.getLogPath());
        Map<String, Object> result = new LinkedHashMap<>(refreshed);
        result.put("logPreview", logPreview);
        return result;
    }

    @Transactional
    public Map<String, Object> getJobView(Long jobId) {
        return refreshAndBuildJobView(getJob(jobId));
    }

    @Transactional(readOnly = true)
    public PipelineTemplate getPipeline(Long projectId, Long pipelineId) {
        PipelineTemplate pipeline = pipelineTemplateRepository.findById(pipelineId)
            .orElseThrow(() -> new ApiException(HttpStatus.NOT_FOUND, "Pipeline 不存在。"));
        if (!pipeline.getProject().getId().equals(projectId)) {
            throw new ApiException(HttpStatus.BAD_REQUEST, "Pipeline 不属于当前项目。" );
        }
        return pipeline;
    }

    @Transactional(readOnly = true)
    public JobRecord getJob(Long jobId) {
        return jobRecordRepository.findById(jobId)
            .orElseThrow(() -> new ApiException(HttpStatus.NOT_FOUND, "任务不存在。"));
    }

    private Map<String, Object> refreshAndBuildJobView(JobRecord job) {
        if (job.getStatus() == JobStatus.RUNNING || job.getStatus() == JobStatus.QUEUED) {
            Map<String, Object> remoteStatus = remoteAccessService.resolveRemoteJobStatus(job.getProject(), job.getPidPath(), job.getExitPath());
            String status = remoteStatus.get("status").toString();
            if (!status.equals(job.getStatus().name())) {
                job.setStatus(JobStatus.valueOf(status));
                if (job.getStatus() == JobStatus.SUCCESS || job.getStatus() == JobStatus.FAILED) {
                    job.setEndedAt(Instant.now());
                    Object exitCode = remoteStatus.get("exitCode");
                    if (exitCode != null && !"0".equals(exitCode.toString())) {
                        job.setErrorSummary("远程任务退出码=" + exitCode);
                    }
                }
                jobRecordRepository.save(job);
            }
        }
        return toJobView(job);
    }

    private Map<String, Object> toPipelineView(PipelineTemplate pipeline) {
        Map<String, Object> view = new LinkedHashMap<>();
        view.put("id", pipeline.getId());
        view.put("name", pipeline.getName());
        view.put("description", pipeline.getDescription());
        view.put("builtIn", pipeline.getBuiltIn());
        view.put("createdAt", pipeline.getCreatedAt());
        view.put("updatedAt", pipeline.getUpdatedAt());
        view.put("steps", pipeline.getSteps().stream().map(step -> {
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("id", step.getId());
            row.put("stepOrder", step.getStepOrder());
            row.put("stepKey", step.getStepKey());
            row.put("displayName", step.getDisplayName());
            row.put("commandTemplate", step.getCommandTemplate());
            return row;
        }).toList());
        return view;
    }

    private Map<String, Object> toJobView(JobRecord job) {
        Map<String, Object> view = new LinkedHashMap<>();
        view.put("id", job.getId());
        view.put("projectId", job.getProject().getId());
        view.put("pipelineId", job.getPipeline() == null ? null : job.getPipeline().getId());
        view.put("stepKey", job.getStepKey());
        view.put("displayName", job.getDisplayName());
        view.put("commandTemplate", job.getCommandTemplate());
        view.put("resolvedCommandMasked", job.getResolvedCommandMasked());
        view.put("status", job.getStatus());
        view.put("triggerSource", job.getTriggerSource());
        view.put("createdAt", job.getCreatedAt());
        view.put("startedAt", job.getStartedAt());
        view.put("endedAt", job.getEndedAt());
        view.put("logPath", job.getLogPath());
        view.put("pidPath", job.getPidPath());
        view.put("exitPath", job.getExitPath());
        view.put("errorSummary", job.getErrorSummary());
        return view;
    }
}
