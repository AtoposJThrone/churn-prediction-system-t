package com.churn.manager.project;

import com.churn.manager.common.ApiResponse;
import com.churn.manager.ssh.SshClient;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;

@RestController
@RequestMapping("/api/projects")
public class ProjectController {

    private final ProjectService projectService;
    private final SshClient sshClient;

    public ProjectController(ProjectService projectService, SshClient sshClient) {
        this.projectService = projectService;
        this.sshClient = sshClient;
    }

    @GetMapping
    public ApiResponse<List<Map<String, Object>>> list() {
        List<Map<String, Object>> result = projectService.listAll().stream()
                .map(this::toSummary).toList();
        return ApiResponse.ok(result);
    }

    @GetMapping("/{id}")
    public ApiResponse<Map<String, Object>> get(@PathVariable Long id) {
        return ApiResponse.ok(toDetail(projectService.getOrThrow(id)));
    }

    @PostMapping
    public ApiResponse<Map<String, Object>> create(@RequestBody ProjectRequest req) {
        return ApiResponse.ok(toDetail(projectService.create(req)));
    }

    @PutMapping("/{id}")
    public ApiResponse<Map<String, Object>> update(@PathVariable Long id, @RequestBody ProjectRequest req) {
        return ApiResponse.ok(toDetail(projectService.update(id, req)));
    }

    @DeleteMapping("/{id}")
    public ApiResponse<Void> delete(@PathVariable Long id) {
        projectService.delete(id);
        return ApiResponse.ok();
    }

    /** Test SSH, MySQL and Hive connectivity concurrently. */
    @PostMapping("/{id}/test-connection")
    public ApiResponse<Map<String, Object>> testConnection(@PathVariable Long id) {
        ManagedProject p = projectService.getOrThrow(id);
        DecryptedSecrets s = projectService.decryptSecrets(id);

        ExecutorService pool = Executors.newFixedThreadPool(3);
        Map<String, Object> results = new LinkedHashMap<>();

        Future<Map<String, Object>> sshFuture = pool.submit(() -> testSsh(p, s));
        Future<Map<String, Object>> mysqlFuture = pool.submit(() -> testMysql(s));

        try {
            results.put("ssh", sshFuture.get(20, TimeUnit.SECONDS));
        } catch (Exception e) {
            results.put("ssh", Map.of("ok", false, "message", e.getMessage()));
        }
        try {
            results.put("mysql", mysqlFuture.get(20, TimeUnit.SECONDS));
        } catch (Exception e) {
            results.put("mysql", Map.of("ok", false, "message", e.getMessage()));
        }
        pool.shutdownNow();
        return ApiResponse.ok(results);
    }

    /** List files in a named scope directory on the remote host. */
    @GetMapping("/{id}/files")
    public ApiResponse<List<Map<String, Object>>> listFiles(
            @PathVariable Long id,
            @RequestParam String scope,
            @RequestParam(required = false) String path) {
        ManagedProject p = projectService.getOrThrow(id);
        DecryptedSecrets s = projectService.decryptSecrets(id);
        String dir = resolveScope(p, scope);
        String remotePath = (path != null && !path.isBlank()) ? path : dir;
        List<SshClient.RemoteFileEntry> entries = sshClient.listDirectory(
                p.getHost(), p.getSshPort(), s.sshUsername(), s.sshPassword(), s.sshPrivateKey(), remotePath);
        List<Map<String, Object>> rows = entries.stream().map(e -> {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("name", e.name());
            m.put("path", e.path());
            m.put("directory", e.directory());
            m.put("size", e.size());
            m.put("modifiedAt", e.modifiedAt());
            return m;
        }).toList();
        return ApiResponse.ok(rows);
    }

    /** Preview a text file from the remote host (up to maxLines lines). */
    @GetMapping("/{id}/file-content")
    public ApiResponse<Map<String, Object>> fileContent(
            @PathVariable Long id,
            @RequestParam String scope,
            @RequestParam String path,
            @RequestParam(defaultValue = "200") int maxLines) {
        ManagedProject p = projectService.getOrThrow(id);
        DecryptedSecrets s = projectService.decryptSecrets(id);
        String content = sshClient.readFileContent(
                p.getHost(), p.getSshPort(), s.sshUsername(), s.sshPassword(), s.sshPrivateKey(), path, maxLines);
        return ApiResponse.ok(Map.of("path", path, "content", content));
    }

    // --- private helpers ---

    private Map<String, Object> testSsh(ManagedProject p, DecryptedSecrets s) {
        long start = System.currentTimeMillis();
        try {
            SshClient.CommandResult r = sshClient.executeCommand(
                    p.getHost(), p.getSshPort(), s.sshUsername(), s.sshPassword(), s.sshPrivateKey(),
                    "echo ok");
            long elapsed = System.currentTimeMillis() - start;
            boolean ok = r.exitCode() == 0;
            return Map.of("ok", ok, "message", ok ? "连接成功" : r.stderr(), "elapsedMs", elapsed);
        } catch (Exception e) {
            return Map.of("ok", false, "message", e.getMessage(), "elapsedMs", System.currentTimeMillis() - start);
        }
    }

    private Map<String, Object> testMysql(DecryptedSecrets s) {
        long start = System.currentTimeMillis();
        if (s.mysqlUrl() == null || s.mysqlUrl().isBlank()) {
            return Map.of("ok", false, "message", "MySQL URL 未配置");
        }
        try (var conn = java.sql.DriverManager.getConnection(s.mysqlUrl(), s.mysqlUsername(), s.mysqlPassword())) {
            conn.createStatement().execute("SELECT 1");
            return Map.of("ok", true, "message", "连接成功", "elapsedMs", System.currentTimeMillis() - start);
        } catch (Exception e) {
            return Map.of("ok", false, "message", e.getMessage(), "elapsedMs", System.currentTimeMillis() - start);
        }
    }

    private String resolveScope(ManagedProject p, String scope) {
        return switch (scope) {
            case "scripts" -> p.getScriptsDir() != null ? p.getScriptsDir() : p.getProjectRoot() + "/scripts";
            case "alert_output" -> p.getAlertOutputDir() != null ? p.getAlertOutputDir() : p.getProjectRoot() + "/alert_output";
            case "experiment_results" -> p.getExperimentResultsDir() != null ? p.getExperimentResultsDir() : p.getProjectRoot() + "/experiment_results";
            case "plots" -> p.getPlotsDir() != null ? p.getPlotsDir() : p.getProjectRoot() + "/plots";
            case "logs" -> p.getLogsDir() != null ? p.getLogsDir() : p.getProjectRoot() + "/logs";
            case "origin" -> p.getOriginDataDir() != null ? p.getOriginDataDir() : "/DataSet_Origin";
            case "transformed" -> p.getTransformedDataDir() != null ? p.getTransformedDataDir() : "/DataSet_Transformed";
            default -> p.getProjectRoot() != null ? p.getProjectRoot() : "/home/hadoop";
        };
    }

    private Map<String, Object> toSummary(ManagedProject p) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("id", p.getId());
        m.put("name", p.getName());
        m.put("description", p.getDescription());
        m.put("host", p.getHost());
        m.put("sshPort", p.getSshPort());
        m.put("createdAt", p.getCreatedAt());
        m.put("updatedAt", p.getUpdatedAt());
        return m;
    }

    private Map<String, Object> toDetail(ManagedProject p) {
        Map<String, Object> m = toSummary(p);
        m.put("projectRoot", p.getProjectRoot());
        m.put("scriptsDir", p.getScriptsDir());
        m.put("alertOutputDir", p.getAlertOutputDir());
        m.put("experimentResultsDir", p.getExperimentResultsDir());
        m.put("plotsDir", p.getPlotsDir());
        m.put("logsDir", p.getLogsDir());
        m.put("originDataDir", p.getOriginDataDir());
        m.put("transformedDataDir", p.getTransformedDataDir());
        m.put("hiveDb", p.getHiveDb());
        m.put("hdfsLandingDir", p.getHdfsLandingDir());
        m.put("pythonCommand", p.getPythonCommand());
        m.put("sparkSubmitCommand", p.getSparkSubmitCommand());
        m.put("beelineCommand", p.getBeelineCommand());
        // Retrieve non-sensitive secret fields
        ProjectSecret s = p.getSecret();
        if (s != null) {
            m.put("sshUsername", s.getSshUsername());
            m.put("mysqlUrl", s.getMysqlUrl());
            m.put("mysqlUsername", s.getMysqlUsername());
            m.put("hiveJdbcUrl", s.getHiveJdbcUrl());
            m.put("hiveUsername", s.getHiveUsername());
        }
        return m;
    }
}
