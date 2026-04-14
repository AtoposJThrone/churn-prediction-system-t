package com.userchurn.manager.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.FetchType;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.Lob;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.PrePersist;
import jakarta.persistence.PreUpdate;
import jakarta.persistence.Table;

import java.time.Instant;

@Entity
@Table(name = "job_record")
public class JobRecord {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "project_id", nullable = false)
    private ManagedProject project;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "pipeline_id")
    private PipelineTemplate pipeline;

    @Column(length = 60)
    private String stepKey;

    @Column(nullable = false, length = 150)
    private String displayName;

    @Lob
    private String commandTemplate;

    @Lob
    private String resolvedCommandMasked;

    @Column(length = 500)
    private String remoteJobDir;

    @Column(length = 500)
    private String logPath;

    @Column(length = 500)
    private String exitPath;

    @Column(length = 500)
    private String pidPath;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 30)
    private JobStatus status;

    @Column(length = 40)
    private String triggerSource;

    @Column(length = 1000)
    private String errorSummary;

    @Column(nullable = false)
    private Instant createdAt;

    private Instant startedAt;

    private Instant endedAt;

    @PrePersist
    public void onCreate() {
        if (createdAt == null) {
            createdAt = Instant.now();
        }
        if (status == null) {
            status = JobStatus.QUEUED;
        }
    }

    @PreUpdate
    public void onUpdate() {
        if (status == JobStatus.RUNNING && startedAt == null) {
            startedAt = Instant.now();
        }
        if ((status == JobStatus.SUCCESS || status == JobStatus.FAILED || status == JobStatus.CANCELLED) && endedAt == null) {
            endedAt = Instant.now();
        }
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public ManagedProject getProject() {
        return project;
    }

    public void setProject(ManagedProject project) {
        this.project = project;
    }

    public PipelineTemplate getPipeline() {
        return pipeline;
    }

    public void setPipeline(PipelineTemplate pipeline) {
        this.pipeline = pipeline;
    }

    public String getStepKey() {
        return stepKey;
    }

    public void setStepKey(String stepKey) {
        this.stepKey = stepKey;
    }

    public String getDisplayName() {
        return displayName;
    }

    public void setDisplayName(String displayName) {
        this.displayName = displayName;
    }

    public String getCommandTemplate() {
        return commandTemplate;
    }

    public void setCommandTemplate(String commandTemplate) {
        this.commandTemplate = commandTemplate;
    }

    public String getResolvedCommandMasked() {
        return resolvedCommandMasked;
    }

    public void setResolvedCommandMasked(String resolvedCommandMasked) {
        this.resolvedCommandMasked = resolvedCommandMasked;
    }

    public String getRemoteJobDir() {
        return remoteJobDir;
    }

    public void setRemoteJobDir(String remoteJobDir) {
        this.remoteJobDir = remoteJobDir;
    }

    public String getLogPath() {
        return logPath;
    }

    public void setLogPath(String logPath) {
        this.logPath = logPath;
    }

    public String getExitPath() {
        return exitPath;
    }

    public void setExitPath(String exitPath) {
        this.exitPath = exitPath;
    }

    public String getPidPath() {
        return pidPath;
    }

    public void setPidPath(String pidPath) {
        this.pidPath = pidPath;
    }

    public JobStatus getStatus() {
        return status;
    }

    public void setStatus(JobStatus status) {
        this.status = status;
    }

    public String getTriggerSource() {
        return triggerSource;
    }

    public void setTriggerSource(String triggerSource) {
        this.triggerSource = triggerSource;
    }

    public String getErrorSummary() {
        return errorSummary;
    }

    public void setErrorSummary(String errorSummary) {
        this.errorSummary = errorSummary;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(Instant createdAt) {
        this.createdAt = createdAt;
    }

    public Instant getStartedAt() {
        return startedAt;
    }

    public void setStartedAt(Instant startedAt) {
        this.startedAt = startedAt;
    }

    public Instant getEndedAt() {
        return endedAt;
    }

    public void setEndedAt(Instant endedAt) {
        this.endedAt = endedAt;
    }
}
