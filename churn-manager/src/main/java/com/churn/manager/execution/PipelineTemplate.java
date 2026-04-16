package com.churn.manager.execution;

import jakarta.persistence.*;
import java.time.Instant;

@Entity
@Table(name = "pipeline_template")
public class PipelineTemplate {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false)
    private Long projectId;

    @Column(nullable = false, length = 200)
    private String name;

    @Column(length = 500)
    private String description;

    /** JSON array of step keys, e.g. ["s11","s14","s20"] */
    @Column(nullable = false, columnDefinition = "TEXT")
    private String stepKeysJson;

    @Column(nullable = false, updatable = false)
    private Instant createdAt;
    @Column(nullable = false)
    private Instant updatedAt;

    @PrePersist
    void onCreate() { Instant now = Instant.now(); createdAt = now; updatedAt = now; }
    @PreUpdate
    void onUpdate() { updatedAt = Instant.now(); }

    public Long getId() { return id; }
    public Long getProjectId() { return projectId; }
    public void setProjectId(Long projectId) { this.projectId = projectId; }
    public String getName() { return name; }
    public void setName(String name) { this.name = name; }
    public String getDescription() { return description; }
    public void setDescription(String description) { this.description = description; }
    public String getStepKeysJson() { return stepKeysJson; }
    public void setStepKeysJson(String stepKeysJson) { this.stepKeysJson = stepKeysJson; }
    public Instant getCreatedAt() { return createdAt; }
    public Instant getUpdatedAt() { return updatedAt; }
}
