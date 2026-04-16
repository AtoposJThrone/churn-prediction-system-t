package com.churn.manager.project;

import jakarta.persistence.*;
import java.time.Instant;

@Entity
@Table(name = "managed_project")
public class ManagedProject {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false, unique = true, length = 120)
    private String name;

    @Column(length = 500)
    private String description;

    @Column(nullable = false, length = 255)
    private String host;

    @Column(nullable = false)
    private Integer sshPort = 22;

    // --- directory configuration ---
    @Column(length = 500)
    private String projectRoot;
    @Column(length = 500)
    private String scriptsDir;
    @Column(length = 500)
    private String alertOutputDir;
    @Column(length = 500)
    private String experimentResultsDir;
    @Column(length = 500)
    private String plotsDir;
    @Column(length = 500)
    private String logsDir;
    @Column(length = 500)
    private String originDataDir;
    @Column(length = 500)
    private String transformedDataDir;
    @Column(length = 255)
    private String hiveDb;
    @Column(length = 500)
    private String hdfsLandingDir;

    // --- command overrides ---
    @Column(length = 100)
    private String pythonCommand;
    @Column(length = 500)
    private String sparkSubmitCommand;
    @Column(length = 500)
    private String beelineCommand;

    @Column(nullable = false, updatable = false)
    private Instant createdAt;
    @Column(nullable = false)
    private Instant updatedAt;

    @OneToOne(mappedBy = "project", cascade = CascadeType.ALL, orphanRemoval = true, fetch = FetchType.LAZY)
    private ProjectSecret secret;

    @PrePersist
    void onCreate() {
        Instant now = Instant.now();
        createdAt = now;
        updatedAt = now;
    }

    @PreUpdate
    void onUpdate() {
        updatedAt = Instant.now();
    }

    // --- getters / setters ---
    public Long getId() { return id; }
    public String getName() { return name; }
    public void setName(String name) { this.name = name; }
    public String getDescription() { return description; }
    public void setDescription(String description) { this.description = description; }
    public String getHost() { return host; }
    public void setHost(String host) { this.host = host; }
    public Integer getSshPort() { return sshPort; }
    public void setSshPort(Integer sshPort) { this.sshPort = sshPort; }
    public String getProjectRoot() { return projectRoot; }
    public void setProjectRoot(String projectRoot) { this.projectRoot = projectRoot; }
    public String getScriptsDir() { return scriptsDir; }
    public void setScriptsDir(String scriptsDir) { this.scriptsDir = scriptsDir; }
    public String getAlertOutputDir() { return alertOutputDir; }
    public void setAlertOutputDir(String alertOutputDir) { this.alertOutputDir = alertOutputDir; }
    public String getExperimentResultsDir() { return experimentResultsDir; }
    public void setExperimentResultsDir(String experimentResultsDir) { this.experimentResultsDir = experimentResultsDir; }
    public String getPlotsDir() { return plotsDir; }
    public void setPlotsDir(String plotsDir) { this.plotsDir = plotsDir; }
    public String getLogsDir() { return logsDir; }
    public void setLogsDir(String logsDir) { this.logsDir = logsDir; }
    public String getOriginDataDir() { return originDataDir; }
    public void setOriginDataDir(String originDataDir) { this.originDataDir = originDataDir; }
    public String getTransformedDataDir() { return transformedDataDir; }
    public void setTransformedDataDir(String transformedDataDir) { this.transformedDataDir = transformedDataDir; }
    public String getHiveDb() { return hiveDb; }
    public void setHiveDb(String hiveDb) { this.hiveDb = hiveDb; }
    public String getHdfsLandingDir() { return hdfsLandingDir; }
    public void setHdfsLandingDir(String hdfsLandingDir) { this.hdfsLandingDir = hdfsLandingDir; }
    public String getPythonCommand() { return pythonCommand; }
    public void setPythonCommand(String pythonCommand) { this.pythonCommand = pythonCommand; }
    public String getSparkSubmitCommand() { return sparkSubmitCommand; }
    public void setSparkSubmitCommand(String sparkSubmitCommand) { this.sparkSubmitCommand = sparkSubmitCommand; }
    public String getBeelineCommand() { return beelineCommand; }
    public void setBeelineCommand(String beelineCommand) { this.beelineCommand = beelineCommand; }
    public Instant getCreatedAt() { return createdAt; }
    public Instant getUpdatedAt() { return updatedAt; }
    public ProjectSecret getSecret() { return secret; }
    public void setSecret(ProjectSecret secret) { this.secret = secret; }
}
