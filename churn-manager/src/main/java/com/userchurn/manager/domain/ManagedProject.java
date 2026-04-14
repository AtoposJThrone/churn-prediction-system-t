package com.userchurn.manager.domain;

import jakarta.persistence.CascadeType;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.OneToOne;
import jakarta.persistence.PrePersist;
import jakarta.persistence.PreUpdate;
import jakarta.persistence.Table;

import java.time.Instant;

@Entity
@Table(name = "managed_project")
public class ManagedProject {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false, unique = true, length = 120)
    private String name;

    @Column(length = 1000)
    private String description;

    @Column(nullable = false, length = 255)
    private String host;

    @Column(nullable = false)
    private Integer sshPort = 22;

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

    @Column(length = 100)
    private String pythonCommand;

    @Column(length = 255)
    private String sparkSubmitCommand;

    @Column(length = 255)
    private String beelineCommand;

    @Column(length = 30)
    private String lastStatDate;

    @Column(nullable = false)
    private Instant createdAt;

    @Column(nullable = false)
    private Instant updatedAt;

    @OneToOne(mappedBy = "project", cascade = CascadeType.ALL, orphanRemoval = true, fetch = FetchType.LAZY)
    private ProjectSecret secret;

    @PrePersist
    public void onCreate() {
        Instant now = Instant.now();
        if (createdAt == null) {
            createdAt = now;
        }
        updatedAt = now;
    }

    @PreUpdate
    public void onUpdate() {
        updatedAt = Instant.now();
    }

    public void attachSecret(ProjectSecret secret) {
        this.secret = secret;
        if (secret != null) {
            secret.setProject(this);
        }
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public Integer getSshPort() {
        return sshPort;
    }

    public void setSshPort(Integer sshPort) {
        this.sshPort = sshPort;
    }

    public String getProjectRoot() {
        return projectRoot;
    }

    public void setProjectRoot(String projectRoot) {
        this.projectRoot = projectRoot;
    }

    public String getScriptsDir() {
        return scriptsDir;
    }

    public void setScriptsDir(String scriptsDir) {
        this.scriptsDir = scriptsDir;
    }

    public String getAlertOutputDir() {
        return alertOutputDir;
    }

    public void setAlertOutputDir(String alertOutputDir) {
        this.alertOutputDir = alertOutputDir;
    }

    public String getExperimentResultsDir() {
        return experimentResultsDir;
    }

    public void setExperimentResultsDir(String experimentResultsDir) {
        this.experimentResultsDir = experimentResultsDir;
    }

    public String getPlotsDir() {
        return plotsDir;
    }

    public void setPlotsDir(String plotsDir) {
        this.plotsDir = plotsDir;
    }

    public String getLogsDir() {
        return logsDir;
    }

    public void setLogsDir(String logsDir) {
        this.logsDir = logsDir;
    }

    public String getOriginDataDir() {
        return originDataDir;
    }

    public void setOriginDataDir(String originDataDir) {
        this.originDataDir = originDataDir;
    }

    public String getTransformedDataDir() {
        return transformedDataDir;
    }

    public void setTransformedDataDir(String transformedDataDir) {
        this.transformedDataDir = transformedDataDir;
    }

    public String getHiveDb() {
        return hiveDb;
    }

    public void setHiveDb(String hiveDb) {
        this.hiveDb = hiveDb;
    }

    public String getHdfsLandingDir() {
        return hdfsLandingDir;
    }

    public void setHdfsLandingDir(String hdfsLandingDir) {
        this.hdfsLandingDir = hdfsLandingDir;
    }

    public String getPythonCommand() {
        return pythonCommand;
    }

    public void setPythonCommand(String pythonCommand) {
        this.pythonCommand = pythonCommand;
    }

    public String getSparkSubmitCommand() {
        return sparkSubmitCommand;
    }

    public void setSparkSubmitCommand(String sparkSubmitCommand) {
        this.sparkSubmitCommand = sparkSubmitCommand;
    }

    public String getBeelineCommand() {
        return beelineCommand;
    }

    public void setBeelineCommand(String beelineCommand) {
        this.beelineCommand = beelineCommand;
    }

    public String getLastStatDate() {
        return lastStatDate;
    }

    public void setLastStatDate(String lastStatDate) {
        this.lastStatDate = lastStatDate;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(Instant createdAt) {
        this.createdAt = createdAt;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(Instant updatedAt) {
        this.updatedAt = updatedAt;
    }

    public ProjectSecret getSecret() {
        return secret;
    }

    public void setSecret(ProjectSecret secret) {
        attachSecret(secret);
    }
}
