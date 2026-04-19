package com.churn.manager.execution;

import com.churn.manager.common.ApiResponse;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.web.bind.annotation.*;

import java.util.*;

@RestController
public class ExecutionController {

    private final ExecutionService executionService;
    private final StepCatalog stepCatalog;

    public ExecutionController(ExecutionService executionService, StepCatalog stepCatalog) {
        this.executionService = executionService;
        this.stepCatalog = stepCatalog;
    }

    /** Global step catalog */
    @GetMapping("/api/steps")
    public ApiResponse<List<Map<String, Object>>> listSteps() {
        List<Map<String, Object>> result = stepCatalog.all().stream().map(s -> {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("key", s.key());
            m.put("displayName", s.displayName());
            m.put("description", s.description());
            m.put("executor", s.executor());
            m.put("scriptFile", s.scriptFile());
            return m;
        }).toList();
        return ApiResponse.ok(result);
    }

    @GetMapping("/api/projects/{projectId}/pipelines")
    public ApiResponse<List<Map<String, Object>>> listPipelines(@PathVariable Long projectId) {
        return ApiResponse.ok(executionService.listPipelines(projectId).stream()
                .map(this::toPipelineView).toList());
    }

    @PostMapping("/api/projects/{projectId}/pipelines")
    public ApiResponse<Map<String, Object>> createPipeline(@PathVariable Long projectId,
                                                            @RequestBody Map<String, Object> body) {
        String name = (String) body.get("name");
        String desc = (String) body.getOrDefault("description", "");
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> steps = (List<Map<String, Object>>) body.get("steps");
        PipelineTemplate pt = executionService.createPipeline(projectId, name, desc, steps);
        return ApiResponse.ok(toPipelineView(pt));
    }

    @PutMapping("/api/projects/{projectId}/pipelines/{pipelineId}")
    public ApiResponse<Map<String, Object>> updatePipeline(@PathVariable Long projectId,
                                                            @PathVariable Long pipelineId,
                                                            @RequestBody Map<String, Object> body) {
        String name = (String) body.get("name");
        String desc = (String) body.getOrDefault("description", "");
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> steps = (List<Map<String, Object>>) body.get("steps");
        PipelineTemplate pt = executionService.updatePipeline(projectId, pipelineId, name, desc, steps);
        return ApiResponse.ok(toPipelineView(pt));
    }

    @DeleteMapping("/api/projects/{projectId}/pipelines/{pipelineId}")
    public ApiResponse<Void> deletePipeline(@PathVariable Long projectId, @PathVariable Long pipelineId) {
        executionService.deletePipeline(projectId, pipelineId);
        return ApiResponse.ok();
    }

    @PostMapping("/api/projects/{projectId}/steps/{stepKey}/run")
    public ApiResponse<Map<String, Object>> runStep(@PathVariable Long projectId,
                                                     @PathVariable String stepKey,
                                                     @RequestBody(required = false) Map<String, Object> body) {
        Map<String, String> overrides = extractOverrides(body);
        JobRecord job = executionService.runStep(projectId, stepKey, overrides, "manual");
        return ApiResponse.ok(toJobView(job));
    }

    @PostMapping("/api/projects/{projectId}/pipelines/{pipelineId}/run")
    public ApiResponse<Map<String, Object>> runPipeline(@PathVariable Long projectId,
                                                         @PathVariable Long pipelineId,
                                                         @RequestBody(required = false) Map<String, Object> body) {
        Map<String, String> overrides = extractOverrides(body);
        JobRecord job = executionService.runPipeline(projectId, pipelineId, overrides, "manual");
        return ApiResponse.ok(toJobView(job));
    }

    @GetMapping("/api/projects/{projectId}/jobs")
    public ApiResponse<Map<String, Object>> listJobs(@PathVariable Long projectId,
                                                      @RequestParam(defaultValue = "0") int page,
                                                      @RequestParam(defaultValue = "20") int size) {
        Page<JobRecord> result = executionService.listJobs(projectId, PageRequest.of(page, size));
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("total", result.getTotalElements());
        data.put("page", page);
        data.put("size", size);
        data.put("items", result.getContent().stream().map(this::toJobView).toList());
        return ApiResponse.ok(data);
    }

    @GetMapping("/api/jobs/{jobId}")
    public ApiResponse<Map<String, Object>> getJob(@PathVariable Long jobId) {
        return ApiResponse.ok(toJobView(executionService.refreshAndGet(jobId)));
    }

    @PostMapping("/api/jobs/{jobId}/cancel")
    public ApiResponse<Map<String, Object>> cancelJob(@PathVariable Long jobId) {
        return ApiResponse.ok(toJobView(executionService.cancelJob(jobId)));
    }

    @GetMapping("/api/jobs/{jobId}/log")
    public ApiResponse<Map<String, Object>> getLog(@PathVariable Long jobId) {
        // Refresh status first
        JobRecord job = executionService.refreshAndGet(jobId);
        String log = executionService.getLog(jobId);
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("jobId", jobId);
        data.put("status", job.getStatus());
        data.put("log", log);
        return ApiResponse.ok(data);
    }

    // --- helpers ---

    private Map<String, Object> toPipelineView(PipelineTemplate pt) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("id", pt.getId());
        m.put("projectId", pt.getProjectId());
        m.put("name", pt.getName());
        m.put("description", pt.getDescription());
        m.put("stepKeysJson", pt.getStepKeysJson());
        m.put("createdAt", pt.getCreatedAt());
        m.put("updatedAt", pt.getUpdatedAt());
        return m;
    }

    private Map<String, Object> toJobView(JobRecord j) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("id", j.getId());
        m.put("projectId", j.getProjectId());
        m.put("pipelineId", j.getPipelineId());
        m.put("stepKey", j.getStepKey());
        m.put("name", j.getName());
        m.put("status", j.getStatus());
        m.put("triggerSource", j.getTriggerSource());
        m.put("createdAt", j.getCreatedAt());
        m.put("startedAt", j.getStartedAt());
        m.put("finishedAt", j.getFinishedAt());
        return m;
    }

    @SuppressWarnings("unchecked")
    private Map<String, String> extractOverrides(Map<String, Object> body) {
        if (body == null) return Map.of();
        Object ov = body.get("overrides");
        if (ov instanceof Map<?,?> map) {
            Map<String, String> result = new HashMap<>();
            map.forEach((k, v) -> { if (k != null && v != null) result.put(k.toString(), v.toString()); });
            return result;
        }
        return Map.of();
    }
}
