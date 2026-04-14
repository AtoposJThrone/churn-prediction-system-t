package com.userchurn.manager.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.Lob;
import jakarta.persistence.MapsId;
import jakarta.persistence.OneToOne;
import jakarta.persistence.PrePersist;
import jakarta.persistence.PreUpdate;
import jakarta.persistence.Table;

import java.time.Instant;

@Entity
@Table(name = "project_secret")
public class ProjectSecret {

    @Id
    private Long id;

    @MapsId
    @OneToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "project_id")
    private ManagedProject project;

    @Column(length = 1000)
    private String sshUsernameEncrypted;

    @Column(length = 4000)
    private String sshPasswordEncrypted;

    @Lob
    private String sshPrivateKeyEncrypted;

    @Column(length = 4000)
    private String mysqlUrlEncrypted;

    @Column(length = 2000)
    private String mysqlUsernameEncrypted;

    @Column(length = 4000)
    private String mysqlPasswordEncrypted;

    @Column(length = 4000)
    private String hiveJdbcUrlEncrypted;

    @Column(length = 2000)
    private String hiveUsernameEncrypted;

    @Column(length = 4000)
    private String hivePasswordEncrypted;

    @Column(nullable = false)
    private Instant updatedAt;

    @PrePersist
    @PreUpdate
    public void touch() {
        updatedAt = Instant.now();
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

    public String getSshUsernameEncrypted() {
        return sshUsernameEncrypted;
    }

    public void setSshUsernameEncrypted(String sshUsernameEncrypted) {
        this.sshUsernameEncrypted = sshUsernameEncrypted;
    }

    public String getSshPasswordEncrypted() {
        return sshPasswordEncrypted;
    }

    public void setSshPasswordEncrypted(String sshPasswordEncrypted) {
        this.sshPasswordEncrypted = sshPasswordEncrypted;
    }

    public String getSshPrivateKeyEncrypted() {
        return sshPrivateKeyEncrypted;
    }

    public void setSshPrivateKeyEncrypted(String sshPrivateKeyEncrypted) {
        this.sshPrivateKeyEncrypted = sshPrivateKeyEncrypted;
    }

    public String getMysqlUrlEncrypted() {
        return mysqlUrlEncrypted;
    }

    public void setMysqlUrlEncrypted(String mysqlUrlEncrypted) {
        this.mysqlUrlEncrypted = mysqlUrlEncrypted;
    }

    public String getMysqlUsernameEncrypted() {
        return mysqlUsernameEncrypted;
    }

    public void setMysqlUsernameEncrypted(String mysqlUsernameEncrypted) {
        this.mysqlUsernameEncrypted = mysqlUsernameEncrypted;
    }

    public String getMysqlPasswordEncrypted() {
        return mysqlPasswordEncrypted;
    }

    public void setMysqlPasswordEncrypted(String mysqlPasswordEncrypted) {
        this.mysqlPasswordEncrypted = mysqlPasswordEncrypted;
    }

    public String getHiveJdbcUrlEncrypted() {
        return hiveJdbcUrlEncrypted;
    }

    public void setHiveJdbcUrlEncrypted(String hiveJdbcUrlEncrypted) {
        this.hiveJdbcUrlEncrypted = hiveJdbcUrlEncrypted;
    }

    public String getHiveUsernameEncrypted() {
        return hiveUsernameEncrypted;
    }

    public void setHiveUsernameEncrypted(String hiveUsernameEncrypted) {
        this.hiveUsernameEncrypted = hiveUsernameEncrypted;
    }

    public String getHivePasswordEncrypted() {
        return hivePasswordEncrypted;
    }

    public void setHivePasswordEncrypted(String hivePasswordEncrypted) {
        this.hivePasswordEncrypted = hivePasswordEncrypted;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(Instant updatedAt) {
        this.updatedAt = updatedAt;
    }
}
