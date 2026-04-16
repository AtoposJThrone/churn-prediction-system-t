package com.churn.manager.schedule;

import com.churn.manager.common.ApiException;
import com.churn.manager.execution.ExecutionService;
import com.churn.manager.execution.JobRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpStatus;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.scheduling.support.CronTrigger;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;

@Service
public class ScheduleService {

    private static final Logger log = LoggerFactory.getLogger(ScheduleService.class);

    private final JobScheduleRepository scheduleRepo;
    private final ExecutionService executionService;
    private final ThreadPoolTaskScheduler taskScheduler;

    /** In-memory map of active ScheduledFuture instances keyed by schedule id */
    private final Map<Long, ScheduledFuture<?>> activeFutures = new ConcurrentHashMap<>();

    public ScheduleService(JobScheduleRepository scheduleRepo,
                           ExecutionService executionService,
                           ThreadPoolTaskScheduler taskScheduler) {
        this.scheduleRepo = scheduleRepo;
        this.executionService = executionService;
        this.taskScheduler = taskScheduler;
    }

    /** Reload all enabled schedules after application starts (runs after DataInitializer). */
    @EventListener(ApplicationReadyEvent.class)
    @Order(2)
    public void reloadSchedules() {
        List<JobSchedule> enabled = scheduleRepo.findByEnabled(true);
        for (JobSchedule s : enabled) {
            register(s);
        }
        log.info("Reloaded {} active schedule(s).", enabled.size());
    }

    @Transactional(readOnly = true)
    public List<JobSchedule> listByProject(Long projectId) {
        return scheduleRepo.findByProjectIdOrderByCreatedAtDesc(projectId);
    }

    @Transactional
    public JobSchedule create(Long projectId, String name, String cronExpression,
                              Long pipelineId, String stepKey, boolean enabled) {
        validateCron(cronExpression);
        JobSchedule s = new JobSchedule();
        s.setProjectId(projectId);
        s.setName(name);
        s.setCronExpression(cronExpression);
        s.setPipelineId(pipelineId);
        s.setStepKey(stepKey);
        s.setEnabled(enabled);
        scheduleRepo.save(s);
        if (enabled) register(s);
        return s;
    }

    @Transactional
    public JobSchedule update(Long id, String name, String cronExpression) {
        JobSchedule s = getOrThrow(id);
        if (name != null && !name.isBlank()) s.setName(name);
        if (cronExpression != null && !cronExpression.isBlank()) {
            validateCron(cronExpression);
            s.setCronExpression(cronExpression);
        }
        scheduleRepo.save(s);
        // Re-register if active
        cancel(id);
        if (Boolean.TRUE.equals(s.getEnabled())) register(s);
        return s;
    }

    @Transactional
    public JobSchedule toggle(Long id) {
        JobSchedule s = getOrThrow(id);
        boolean newState = !Boolean.TRUE.equals(s.getEnabled());
        s.setEnabled(newState);
        scheduleRepo.save(s);
        if (newState) { register(s); } else { cancel(id); }
        return s;
    }

    @Transactional
    public void delete(Long id) {
        JobSchedule s = getOrThrow(id);
        cancel(id);
        scheduleRepo.delete(s);
    }

    @Transactional
    public JobRecord runNow(Long id) {
        JobSchedule s = getOrThrow(id);
        String source = "manual-trigger:" + id;
        if (s.getPipelineId() != null) {
            return executionService.runPipeline(s.getProjectId(), s.getPipelineId(), Map.of(), source);
        } else if (s.getStepKey() != null) {
            return executionService.runStep(s.getProjectId(), s.getStepKey(), Map.of(), source);
        }
        throw new ApiException(HttpStatus.BAD_REQUEST, "该调度未配置运行目标（流水线或步骤）。");
    }

    // --- private ---

    private void register(JobSchedule s) {
        try {
            ScheduledFuture<?> future = taskScheduler.schedule(
                    () -> trigger(s.getId()),
                    new CronTrigger(s.getCronExpression()));
            activeFutures.put(s.getId(), future);
            log.debug("Registered schedule id={} cron={}", s.getId(), s.getCronExpression());
        } catch (Exception e) {
            log.error("Failed to register schedule id={}: {}", s.getId(), e.getMessage());
        }
    }

    private void cancel(Long id) {
        ScheduledFuture<?> f = activeFutures.remove(id);
        if (f != null) f.cancel(false);
    }

    @org.springframework.transaction.annotation.Transactional
    private void trigger(Long scheduleId) {
        scheduleRepo.findById(scheduleId).ifPresent(s -> {
            try {
                s.setLastTriggeredAt(Instant.now());
                scheduleRepo.save(s);
                String source = "schedule:" + scheduleId;
                if (s.getPipelineId() != null) {
                    executionService.runPipeline(s.getProjectId(), s.getPipelineId(), Map.of(), source);
                } else if (s.getStepKey() != null) {
                    executionService.runStep(s.getProjectId(), s.getStepKey(), Map.of(), source);
                }
            } catch (Exception e) {
                log.error("Schedule trigger failed id={}: {}", scheduleId, e.getMessage());
            }
        });
    }

    private JobSchedule getOrThrow(Long id) {
        return scheduleRepo.findById(id)
                .orElseThrow(() -> new ApiException(HttpStatus.NOT_FOUND, "调度不存在：" + id));
    }

    private void validateCron(String cron) {
        try {
            new CronTrigger(cron);
        } catch (Exception e) {
            throw new ApiException(HttpStatus.BAD_REQUEST, "无效的 Cron 表达式：" + cron);
        }
    }
}
