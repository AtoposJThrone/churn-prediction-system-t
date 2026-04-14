package com.userchurn.manager.controller;

import com.userchurn.manager.service.ExecutionService;
import com.userchurn.manager.service.ScheduleService;
import com.userchurn.manager.web.ApiResponse;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api")
public class ExecutionController {

    public record PipelineRequest(@NotBlank(message = "Pipeline 名称不能为空。") String name,
                                  String description,
                                  @NotNull(message = "请选择至少一个脚本步骤。") List<String> stepKeys) {
    }

    public record RunRequest(Map<String, String> runtimeOverrides) {
    }

    public record ScheduleRequest(@NotBlank(message = "定时任务名称不能为空。") String name,
                                  @NotNull(message = "请选择 pipeline。") Long pipelineId,
                                  @NotBlank(message = "Cron 表达式不能为空。") String cronExpression,
                                  boolean enabled) {
    }

    public record ToggleScheduleRequest(boolean enabled) {
    }

    private final ExecutionService executionService;
    private final ScheduleService scheduleService;

    public ExecutionController(ExecutionService executionService, ScheduleService scheduleService) {
        this.executionService = executionService;
        this.scheduleService = scheduleService;
    }

    @GetMapping("/catalog/steps")
    public ApiResponse<List<Map<String, Object>>> catalog() {
        return ApiResponse.ok(executionService.listCatalog());
    }

    @GetMapping("/projects/{projectId}/pipelines")
    public ApiResponse<List<Map<String, Object>>> listPipelines(@PathVariable Long projectId) {
        return ApiResponse.ok(executionService.listPipelines(projectId));
    }

    @PostMapping("/projects/{projectId}/pipelines")
    public ApiResponse<Map<String, Object>> createPipeline(@PathVariable Long projectId,
                                                           @Valid @RequestBody PipelineRequest request) {
        return ApiResponse.ok(executionService.createPipeline(projectId, request.name(), request.description(), request.stepKeys()));
    }

    @PostMapping("/projects/{projectId}/steps/{stepKey}/run")
    public ApiResponse<Map<String, Object>> runStep(@PathVariable Long projectId,
                                                    @PathVariable String stepKey,
                                                    @RequestBody(required = false) RunRequest request) {
        return ApiResponse.ok(executionService.runSingleStep(
            projectId,
            stepKey,
            request == null ? Map.of() : request.runtimeOverrides(),
            "manual"
        ));
    }

    @PostMapping("/projects/{projectId}/pipelines/{pipelineId}/run")
    public ApiResponse<Map<String, Object>> runPipeline(@PathVariable Long projectId,
                                                        @PathVariable Long pipelineId,
                                                        @RequestBody(required = false) RunRequest request) {
        return ApiResponse.ok(executionService.runPipeline(
            projectId,
            pipelineId,
            request == null ? Map.of() : request.runtimeOverrides(),
            "manual"
        ));
    }

    @GetMapping("/projects/{projectId}/jobs")
    public ApiResponse<List<Map<String, Object>>> listJobs(@PathVariable Long projectId) {
        return ApiResponse.ok(executionService.listJobs(projectId));
    }

    @GetMapping("/jobs/{jobId}")
    public ApiResponse<Map<String, Object>> getJob(@PathVariable Long jobId) {
        return ApiResponse.ok(executionService.getJobView(jobId));
    }

    @GetMapping("/jobs/{jobId}/log")
    public ApiResponse<Map<String, Object>> getJobLog(@PathVariable Long jobId) {
        return ApiResponse.ok(executionService.getJobLog(jobId));
    }

    @GetMapping("/projects/{projectId}/schedules")
    public ApiResponse<List<Map<String, Object>>> listSchedules(@PathVariable Long projectId) {
        return ApiResponse.ok(scheduleService.listSchedules(projectId));
    }

    @PostMapping("/projects/{projectId}/schedules")
    public ApiResponse<Map<String, Object>> createSchedule(@PathVariable Long projectId,
                                                           @Valid @RequestBody ScheduleRequest request) {
        return ApiResponse.ok(scheduleService.createSchedule(
            projectId,
            request.pipelineId(),
            request.name(),
            request.cronExpression(),
            request.enabled()
        ));
    }

    @PostMapping("/schedules/{scheduleId}/toggle")
    public ApiResponse<Map<String, Object>> toggleSchedule(@PathVariable Long scheduleId,
                                                           @RequestBody ToggleScheduleRequest request) {
        return ApiResponse.ok(scheduleService.toggleSchedule(scheduleId, request.enabled()));
    }

    @DeleteMapping("/schedules/{scheduleId}")
    public ApiResponse<Map<String, Object>> deleteSchedule(@PathVariable Long scheduleId) {
        scheduleService.deleteSchedule(scheduleId);
        return ApiResponse.ok(Map.of("deleted", true));
    }
}
