package com.churn.manager.auth;

import jakarta.persistence.*;
import java.time.Instant;

import org.hibernate.annotations.UpdateTimestamp;

@Entity
@Table(name = "app_user")
public class AppUser implements java.io.Serializable {

    public static final long serialVersionUID = 1L;

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false, unique = true, length = 100)
    private String username;

    @Column(nullable = false, length = 65)
    private String passwordHash;

    @Column(nullable = false, updatable = false)
    private Instant createdAt;

    // Hibernate 扩展注解
    @UpdateTimestamp
    private Instant updatedAt;

    private Instant lastLoginAt;

    @Column(length = 45)
    private String lastLoginIp;

    // VersionLock
    @Version
    @Column(nullable = false)
    private Integer version;

    @PrePersist
    void onCreate() {
        createdAt = Instant.now();
        updatedAt = createdAt;
    }

    public Long getId() { return id; }
    public String getUsername() { return username; }
    public void setUsername(String username) { this.username = username; }
    public String getPasswordHash() { return passwordHash; }
    public void setPasswordHash(String passwordHash) { this.passwordHash = passwordHash; }
    public Instant getCreatedAt() { return createdAt; }
    public Instant getUpdatedAt() { return updatedAt; }
    public Instant getLastLoginAt() { return lastLoginAt; }
    public String getLastLoginIp() { return lastLoginIp; }
    public Integer getVersion() { return version; }
    public void setLastLoginAt(Instant lastLoginAt) { this.lastLoginAt = lastLoginAt;}
    public void setLastLoginIp(String lastLoginIp) { this.lastLoginIp = lastLoginIp; }

}