package com.churn.manager.schedule;

import com.churn.manager.common.ApiResponse;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/projects/{projectId}/schedules")
public class ScheduleController {

    private final ScheduleService scheduleService;

    public ScheduleController(ScheduleService scheduleService) {
        this.scheduleService = scheduleService;
    }

    @GetMapping
    public ApiResponse<List<JobSchedule>> list(@PathVariable Long projectId) {
        return ApiResponse.ok(scheduleService.listByProject(projectId));
    }

    @PostMapping
    public ApiResponse<JobSchedule> create(@PathVariable Long projectId,
                                            @RequestBody Map<String, Object> body) {
        String name = (String) body.getOrDefault("name", "未命名调度");
        String cron = (String) body.get("cronExpression");
        Long pipelineId = body.get("pipelineId") != null
                ? Long.valueOf(body.get("pipelineId").toString()) : null;
        String stepKey = (String) body.get("stepKey");
        boolean enabled = body.get("enabled") == null || Boolean.TRUE.equals(body.get("enabled"));
        return ApiResponse.ok(scheduleService.create(projectId, name, cron, pipelineId, stepKey, enabled));
    }

    @PutMapping("/{id}")
    public ApiResponse<JobSchedule> update(@PathVariable Long projectId,
                                            @PathVariable Long id,
                                            @RequestBody Map<String, Object> body) {
        String name = (String) body.get("name");
        String cron = (String) body.get("cronExpression");
        return ApiResponse.ok(scheduleService.update(id, name, cron));
    }

    @PostMapping("/{id}/toggle")
    public ApiResponse<JobSchedule> toggle(@PathVariable Long projectId, @PathVariable Long id) {
        return ApiResponse.ok(scheduleService.toggle(id));
    }

    @PostMapping("/{id}/run-now")
    public ApiResponse<com.churn.manager.execution.JobRecord> runNow(
            @PathVariable Long projectId, @PathVariable Long id) {
        return ApiResponse.ok(scheduleService.runNow(id));
    }

    @DeleteMapping("/{id}")
    public ApiResponse<Void> delete(@PathVariable Long projectId, @PathVariable Long id) {
        scheduleService.delete(id);
        return ApiResponse.ok();
    }
}
