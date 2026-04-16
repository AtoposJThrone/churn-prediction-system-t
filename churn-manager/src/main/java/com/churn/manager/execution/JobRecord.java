package com.churn.manager.execution;

import jakarta.persistence.*;
import java.time.Instant;

@Entity
@Table(name = "job_record")
public class JobRecord {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false)
    private Long projectId;

    /** Null for single-step jobs */
    private Long pipelineId;

    /** Null for pipeline jobs */
    @Column(length = 20)
    private String stepKey;

    @Column(nullable = false, length = 300)
    private String name;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 20)
    private JobStatus status;

    @Column(length = 200)
    private String triggerSource;

    @Column(length = 500)
    private String remoteJobDir;

    @Column(length = 500)
    private String logPath;

    @Column(length = 20)
    private String pid;

    @Column(length = 500)
    private String exitPath;

    @Column(nullable = false, updatable = false)
    private Instant createdAt;
    private Instant startedAt;
    private Instant finishedAt;

    @PrePersist
    void onCreate() { createdAt = Instant.now(); }

    public Long getId() { return id; }
    public Long getProjectId() { return projectId; }
    public void setProjectId(Long projectId) { this.projectId = projectId; }
    public Long getPipelineId() { return pipelineId; }
    public void setPipelineId(Long pipelineId) { this.pipelineId = pipelineId; }
    public String getStepKey() { return stepKey; }
    public void setStepKey(String stepKey) { this.stepKey = stepKey; }
    public String getName() { return name; }
    public void setName(String name) { this.name = name; }
    public JobStatus getStatus() { return status; }
    public void setStatus(JobStatus status) { this.status = status; }
    public String getTriggerSource() { return triggerSource; }
    public void setTriggerSource(String triggerSource) { this.triggerSource = triggerSource; }
    public String getRemoteJobDir() { return remoteJobDir; }
    public void setRemoteJobDir(String remoteJobDir) { this.remoteJobDir = remoteJobDir; }
    public String getLogPath() { return logPath; }
    public void setLogPath(String logPath) { this.logPath = logPath; }
    public String getPid() { return pid; }
    public void setPid(String pid) { this.pid = pid; }
    public String getExitPath() { return exitPath; }
    public void setExitPath(String exitPath) { this.exitPath = exitPath; }
    public Instant getCreatedAt() { return createdAt; }
    public Instant getStartedAt() { return startedAt; }
    public void setStartedAt(Instant startedAt) { this.startedAt = startedAt; }
    public Instant getFinishedAt() { return finishedAt; }
    public void setFinishedAt(Instant finishedAt) { this.finishedAt = finishedAt; }
}
