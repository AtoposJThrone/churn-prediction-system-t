package com.userchurn.manager.service;

import com.userchurn.manager.domain.JobSchedule;
import com.userchurn.manager.domain.ManagedProject;
import com.userchurn.manager.domain.PipelineTemplate;
import com.userchurn.manager.repo.JobScheduleRepository;
import com.userchurn.manager.web.ApiException;
import jakarta.annotation.PostConstruct;
import org.springframework.http.HttpStatus;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.support.CronTrigger;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;

@Service
public class ScheduleService {

    private final JobScheduleRepository jobScheduleRepository;
    private final ExecutionService executionService;
    private final ProjectService projectService;
    private final TaskScheduler taskScheduler;
    private final Map<Long, ScheduledFuture<?>> scheduledTasks = new ConcurrentHashMap<>();

    public ScheduleService(JobScheduleRepository jobScheduleRepository,
                           ExecutionService executionService,
                           ProjectService projectService,
                           TaskScheduler taskScheduler) {
        this.jobScheduleRepository = jobScheduleRepository;
        this.executionService = executionService;
        this.projectService = projectService;
        this.taskScheduler = taskScheduler;
    }

    @PostConstruct
    public void reloadOnStartup() {
        jobScheduleRepository.findByEnabledTrue().forEach(this::scheduleInternal);
    }

    @Transactional(readOnly = true)
    public List<Map<String, Object>> listSchedules(Long projectId) {
        projectService.getProject(projectId);
        return jobScheduleRepository.findByProjectIdOrderByUpdatedAtDesc(projectId).stream()
            .map(this::toView)
            .toList();
    }

    @Transactional
    public Map<String, Object> createSchedule(Long projectId, Long pipelineId, String name, String cronExpression, boolean enabled) {
        ManagedProject project = projectService.getProject(projectId);
        PipelineTemplate pipeline = executionService.getPipeline(projectId, pipelineId);
        validateCron(cronExpression);

        JobSchedule schedule = new JobSchedule();
        schedule.setProject(project);
        schedule.setPipeline(pipeline);
        schedule.setName(name);
        schedule.setCronExpression(cronExpression);
        schedule.setEnabled(enabled);
        jobScheduleRepository.save(schedule);

        if (enabled) {
            scheduleInternal(schedule);
        }
        return toView(schedule);
    }

    @Transactional
    public Map<String, Object> toggleSchedule(Long scheduleId, boolean enabled) {
        JobSchedule schedule = getSchedule(scheduleId);
        schedule.setEnabled(enabled);
        jobScheduleRepository.save(schedule);
        cancelInternal(schedule.getId());
        if (enabled) {
            scheduleInternal(schedule);
        }
        return toView(schedule);
    }

    @Transactional
    public void deleteSchedule(Long scheduleId) {
        JobSchedule schedule = getSchedule(scheduleId);
        cancelInternal(schedule.getId());
        jobScheduleRepository.delete(schedule);
    }

    private void scheduleInternal(JobSchedule schedule) {
        cancelInternal(schedule.getId());
        ScheduledFuture<?> future = taskScheduler.schedule(
            () -> executionService.runPipeline(
                schedule.getProject().getId(),
                schedule.getPipeline().getId(),
                Map.of(),
                "schedule:" + schedule.getId()
            ),
            new CronTrigger(schedule.getCronExpression())
        );
        scheduledTasks.put(schedule.getId(), future);
    }

    private void cancelInternal(Long scheduleId) {
        ScheduledFuture<?> future = scheduledTasks.remove(scheduleId);
        if (future != null) {
            future.cancel(false);
        }
    }

    private JobSchedule getSchedule(Long scheduleId) {
        return jobScheduleRepository.findById(scheduleId)
            .orElseThrow(() -> new ApiException(HttpStatus.NOT_FOUND, "定时任务不存在。"));
    }

    private void validateCron(String cronExpression) {
        try {
            new CronTrigger(cronExpression);
        } catch (IllegalArgumentException ex) {
            throw new ApiException(HttpStatus.BAD_REQUEST, "Cron 表达式无效: " + ex.getMessage());
        }
    }

    private Map<String, Object> toView(JobSchedule schedule) {
        Map<String, Object> view = new LinkedHashMap<>();
        view.put("id", schedule.getId());
        view.put("projectId", schedule.getProject().getId());
        view.put("pipelineId", schedule.getPipeline().getId());
        view.put("pipelineName", schedule.getPipeline().getName());
        view.put("name", schedule.getName());
        view.put("cronExpression", schedule.getCronExpression());
        view.put("enabled", schedule.getEnabled());
        view.put("createdAt", schedule.getCreatedAt());
        view.put("updatedAt", schedule.getUpdatedAt());
        return view;
    }
}
