package com.churn.manager.project;

import jakarta.persistence.*;

@Entity
@Table(name = "project_secret")
public class ProjectSecret {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @OneToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "project_id", nullable = false, unique = true)
    private ManagedProject project;

    @Column(length = 200)
    private String sshUsername;

    /** AES-GCM encrypted SSH password */
    @Column(length = 1000)
    private String sshPasswordEnc;

    /** AES-GCM encrypted SSH private key (PEM text) */
    @Column(columnDefinition = "TEXT")
    private String sshPrivateKeyEnc;

    @Column(length = 500)
    private String mysqlUrl;

    @Column(length = 200)
    private String mysqlUsername;

    /** AES-GCM encrypted MySQL password */
    @Column(length = 1000)
    private String mysqlPasswordEnc;

    @Column(length = 500)
    private String hiveJdbcUrl;

    @Column(length = 200)
    private String hiveUsername;

    /** AES-GCM encrypted Hive password */
    @Column(length = 1000)
    private String hivePasswordEnc;

    public Long getId() { return id; }
    public ManagedProject getProject() { return project; }
    public void setProject(ManagedProject project) { this.project = project; }
    public String getSshUsername() { return sshUsername; }
    public void setSshUsername(String sshUsername) { this.sshUsername = sshUsername; }
    public String getSshPasswordEnc() { return sshPasswordEnc; }
    public void setSshPasswordEnc(String sshPasswordEnc) { this.sshPasswordEnc = sshPasswordEnc; }
    public String getSshPrivateKeyEnc() { return sshPrivateKeyEnc; }
    public void setSshPrivateKeyEnc(String sshPrivateKeyEnc) { this.sshPrivateKeyEnc = sshPrivateKeyEnc; }
    public String getMysqlUrl() { return mysqlUrl; }
    public void setMysqlUrl(String mysqlUrl) { this.mysqlUrl = mysqlUrl; }
    public String getMysqlUsername() { return mysqlUsername; }
    public void setMysqlUsername(String mysqlUsername) { this.mysqlUsername = mysqlUsername; }
    public String getMysqlPasswordEnc() { return mysqlPasswordEnc; }
    public void setMysqlPasswordEnc(String mysqlPasswordEnc) { this.mysqlPasswordEnc = mysqlPasswordEnc; }
    public String getHiveJdbcUrl() { return hiveJdbcUrl; }
    public void setHiveJdbcUrl(String hiveJdbcUrl) { this.hiveJdbcUrl = hiveJdbcUrl; }
    public String getHiveUsername() { return hiveUsername; }
    public void setHiveUsername(String hiveUsername) { this.hiveUsername = hiveUsername; }
    public String getHivePasswordEnc() { return hivePasswordEnc; }
    public void setHivePasswordEnc(String hivePasswordEnc) { this.hivePasswordEnc = hivePasswordEnc; }
}
