package com.userchurn.manager.service;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;
import com.userchurn.manager.domain.ManagedProject;
import com.userchurn.manager.domain.ProjectSecret;
import com.userchurn.manager.web.ApiException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.Vector;

@Service
public class RemoteAccessService {

    public record DecryptedSecrets(
        String sshUsername,
        String sshPassword,
        String sshPrivateKey,
        String mysqlUrl,
        String mysqlUsername,
        String mysqlPassword,
        String hiveJdbcUrl,
        String hiveUsername,
        String hivePassword
    ) {
    }

    public record RemoteFileEntry(String name, String path, boolean directory, long size, Instant modifiedAt) {
    }

    public record CommandResult(int exitCode, String stdout, String stderr) {
    }

    public record LaunchedJob(String remoteJobDir, String logPath, String pidPath, String exitPath, String runPath) {
    }

    private static final Logger log = LoggerFactory.getLogger(RemoteAccessService.class);

    private final SecretCryptoService secretCryptoService;
    private final ProjectService projectService;
    private final String defaultLogSubdir;
    private final int previewLineLimit;

    public RemoteAccessService(SecretCryptoService secretCryptoService,
                               ProjectService projectService,
                               @Value("${app.jobs.default-log-subdir:manager-jobs}") String defaultLogSubdir,
                               @Value("${app.jobs.preview-line-limit:120}") int previewLineLimit) {
        this.secretCryptoService = secretCryptoService;
        this.projectService = projectService;
        this.defaultLogSubdir = defaultLogSubdir;
        this.previewLineLimit = previewLineLimit;
    }

    @Transactional(readOnly = true)
    public Map<String, Object> testConnections(ManagedProject project) {
        Map<String, Object> result = new LinkedHashMap<>();
        try {
            testSsh(project);
            result.put("ssh", Map.of("success", true));
        } catch (Exception ex) {
            result.put("ssh", Map.of("success", false, "message", ex.getMessage()));
        }

        try {
            testMysql(project);
            result.put("mysql", Map.of("success", true));
        } catch (Exception ex) {
            result.put("mysql", Map.of("success", false, "message", ex.getMessage()));
        }

        try {
            testHiveBeeline(project);
            result.put("hive", Map.of("success", true));
        } catch (Exception ex) {
            result.put("hive", Map.of("success", false, "message", ex.getMessage()));
        }
        return result;
    }

    @Transactional(readOnly = true)
    public List<RemoteFileEntry> listDirectory(ManagedProject project, String scope, String path) {
        String resolvedPath = resolveWhitelistedPath(project, scope, path);
        Session session = null;
        try {
            session = openSession(project);
            ChannelSftp channel = openSftp(session);
            try {
                Vector<ChannelSftp.LsEntry> entries = channel.ls(resolvedPath);
                List<RemoteFileEntry> files = new ArrayList<>();
                for (ChannelSftp.LsEntry entry : entries) {
                    String name = entry.getFilename();
                    if (".".equals(name) || "..".equals(name)) {
                        continue;
                    }
                    boolean directory = entry.getAttrs().isDir();
                    String childPath = appendPath(resolvedPath, name);
                    files.add(new RemoteFileEntry(
                        name,
                        childPath,
                        directory,
                        entry.getAttrs().getSize(),
                        Instant.ofEpochSecond(entry.getAttrs().getMTime())
                    ));
                }
                files.sort(Comparator.comparing(RemoteFileEntry::directory).reversed().thenComparing(RemoteFileEntry::name));
                return files;
            } finally {
                channel.disconnect();
            }
        } catch (JSchException | SftpException ex) {
            throw new ApiException(HttpStatus.BAD_GATEWAY, "远程目录读取失败: " + ex.getMessage());
        } finally {
            disconnectSession(session);
        }
    }

    @Transactional(readOnly = true)
    public Map<String, Object> readTextFile(ManagedProject project, String scope, String path) {
        String resolvedPath = resolveWhitelistedPath(project, scope, path);
        Session session = null;
        try {
            session = openSession(project);
            ChannelSftp channel = openSftp(session);
            try (InputStream inputStream = channel.get(resolvedPath)) {
                byte[] bytes = inputStream.readAllBytes();
                String text = new String(bytes, StandardCharsets.UTF_8);
                return Map.of(
                    "path", resolvedPath,
                    "content", text,
                    "lineCount", text.lines().count()
                );
            } finally {
                channel.disconnect();
            }
        } catch (JSchException | SftpException | IOException ex) {
            throw new ApiException(HttpStatus.BAD_GATEWAY, "远程文件预览失败: " + ex.getMessage());
        } finally {
            disconnectSession(session);
        }
    }

    @Transactional(readOnly = true)
    public String tailRemoteFile(ManagedProject project, String absolutePath) {
        Map<String, Object> preview = readTextFile(project, "absolute", absolutePath);
        String content = preview.get("content").toString();
        List<String> lines = content.lines().toList();
        int fromIndex = Math.max(0, lines.size() - previewLineLimit);
        return String.join("\n", lines.subList(fromIndex, lines.size()));
    }

    @Transactional(readOnly = true)
    public LaunchedJob launchBackgroundJob(ManagedProject project,
                                           String shellBody,
                                           Map<String, String> envVars) {
        String rootDir = appendPath(project.getLogsDir(), defaultLogSubdir);
        String jobId = UUID.randomUUID().toString();
        String remoteJobDir = appendPath(rootDir, jobId);
        String envPath = appendPath(remoteJobDir, "env.sh");
        String runPath = appendPath(remoteJobDir, "run.sh");
        String logPath = appendPath(remoteJobDir, "job.log");
        String pidPath = appendPath(remoteJobDir, "job.pid");
        String exitPath = appendPath(remoteJobDir, "job.exit");

        Session session = null;
        try {
            session = openSession(project);
            executeOrThrow(session, "mkdir -p " + shellQuote(remoteJobDir), Duration.ofSeconds(10), "初始化远程任务目录失败");
            ChannelSftp channel = openSftp(session);
            try {
                uploadText(channel, envPath, buildEnvFile(envVars));
                uploadText(channel, runPath, buildRunScript(envPath, exitPath, shellBody));
            } finally {
                channel.disconnect();
            }

            executeOrThrow(session, "chmod 600 " + shellQuote(envPath) + " && chmod 700 " + shellQuote(runPath), Duration.ofSeconds(10), "设置远程脚本权限失败");
            String launchCommand = "nohup bash " + shellQuote(runPath) + " > " + shellQuote(logPath) + " 2>&1 < /dev/null & echo $! > " + shellQuote(pidPath);
            executeOrThrow(session, launchCommand, Duration.ofSeconds(10), "启动远程任务失败");
            return new LaunchedJob(remoteJobDir, logPath, pidPath, exitPath, runPath);
        } catch (JSchException ex) {
            throw new ApiException(HttpStatus.BAD_GATEWAY, "SSH 连接失败: " + ex.getMessage());
        } finally {
            disconnectSession(session);
        }
    }

    @Transactional(readOnly = true)
    public Map<String, Object> resolveRemoteJobStatus(ManagedProject project, String pidPath, String exitPath) {
        Session session = null;
        try {
            session = openSession(project);
            CommandResult exitCheck = execute(session, "if [ -f " + shellQuote(exitPath) + " ]; then cat " + shellQuote(exitPath) + "; fi", Duration.ofSeconds(10));
            if (exitCheck.exitCode() == 0 && !exitCheck.stdout().isBlank()) {
                String exitCode = exitCheck.stdout().trim();
                return Map.of(
                    "status", "0".equals(exitCode) ? "SUCCESS" : "FAILED",
                    "exitCode", exitCode
                );
            }

            CommandResult pidCheck = execute(session, "if [ -f " + shellQuote(pidPath) + " ]; then PID=$(cat " + shellQuote(pidPath) + "); ps -p $PID >/dev/null 2>&1; echo $?; fi", Duration.ofSeconds(10));
            if (pidCheck.exitCode() == 0 && !pidCheck.stdout().isBlank() && pidCheck.stdout().trim().equals("0")) {
                return Map.of("status", "RUNNING");
            }
            return Map.of("status", "FAILED", "exitCode", "unknown");
        } catch (JSchException ex) {
            throw new ApiException(HttpStatus.BAD_GATEWAY, "获取任务状态失败: " + ex.getMessage());
        } finally {
            disconnectSession(session);
        }
    }

    @Transactional(readOnly = true)
    public DecryptedSecrets getSecrets(ManagedProject project) {
        ManagedProject actualProject = projectService.getProject(project.getId());
        ProjectSecret secret = Optional.ofNullable(actualProject.getSecret()).orElse(new ProjectSecret());
        return new DecryptedSecrets(
            decrypt(secret.getSshUsernameEncrypted()),
            decrypt(secret.getSshPasswordEncrypted()),
            decrypt(secret.getSshPrivateKeyEncrypted()),
            decrypt(secret.getMysqlUrlEncrypted()),
            decrypt(secret.getMysqlUsernameEncrypted()),
            decrypt(secret.getMysqlPasswordEncrypted()),
            decrypt(secret.getHiveJdbcUrlEncrypted()),
            decrypt(secret.getHiveUsernameEncrypted()),
            decrypt(secret.getHivePasswordEncrypted())
        );
    }

    private void testSsh(ManagedProject project) {
        Session session = null;
        try {
            session = openSession(project);
            executeOrThrow(session, "echo connected", Duration.ofSeconds(10), "SSH 测试失败");
        } catch (JSchException ex) {
            throw new ApiException(HttpStatus.BAD_GATEWAY, "SSH 测试失败: " + ex.getMessage());
        } finally {
            disconnectSession(session);
        }
    }

    private void testMysql(ManagedProject project) {
        DecryptedSecrets secrets = getSecrets(project);
        if (secrets.mysqlUrl() == null || secrets.mysqlUsername() == null || secrets.mysqlPassword() == null) {
            throw new ApiException(HttpStatus.BAD_REQUEST, "MySQL 连接参数尚未配置完整。" );
        }
        try (var connection = DriverManager.getConnection(secrets.mysqlUrl(), secrets.mysqlUsername(), secrets.mysqlPassword());
             var statement = connection.createStatement()) {
            statement.execute("SELECT 1");
        } catch (SQLException ex) {
            throw new ApiException(HttpStatus.BAD_GATEWAY, "MySQL 测试失败: " + ex.getMessage());
        }
    }

    private void testHiveBeeline(ManagedProject project) {
        DecryptedSecrets secrets = getSecrets(project);
        if (secrets.hiveJdbcUrl() == null || secrets.hiveUsername() == null || secrets.hivePassword() == null) {
            throw new ApiException(HttpStatus.BAD_REQUEST, "Hive beeline 连接参数尚未配置完整。" );
        }
        String command = project.getBeelineCommand()
            + " -u " + shellQuote(secrets.hiveJdbcUrl())
            + " -n " + shellQuote(secrets.hiveUsername())
            + " -p " + shellQuote(secrets.hivePassword())
            + " -e " + shellQuote("show databases;");
        Session session = null;
        try {
            session = openSession(project);
            executeOrThrow(session, command, Duration.ofSeconds(20), "Hive beeline 测试失败");
        } catch (JSchException ex) {
            throw new ApiException(HttpStatus.BAD_GATEWAY, "Hive beeline 测试失败: " + ex.getMessage());
        } finally {
            disconnectSession(session);
        }
    }

    private Session openSession(ManagedProject project) throws JSchException {
        DecryptedSecrets secrets = getSecrets(project);
        if ((secrets.sshUsername() == null || secrets.sshUsername().isBlank())
            || ((secrets.sshPassword() == null || secrets.sshPassword().isBlank())
            && (secrets.sshPrivateKey() == null || secrets.sshPrivateKey().isBlank()))) {
            throw new ApiException(HttpStatus.BAD_REQUEST, "SSH 用户名与认证信息尚未配置完整。" );
        }

        JSch jsch = new JSch();
        if (secrets.sshPrivateKey() != null && !secrets.sshPrivateKey().isBlank()) {
            jsch.addIdentity("manager-inline-key", secrets.sshPrivateKey().getBytes(StandardCharsets.UTF_8), null, null);
        }

        Session session = jsch.getSession(secrets.sshUsername(), project.getHost(), project.getSshPort() == null ? 22 : project.getSshPort());
        if (secrets.sshPassword() != null && !secrets.sshPassword().isBlank()) {
            session.setPassword(secrets.sshPassword());
        }
        session.setConfig("StrictHostKeyChecking", "no");
        session.connect(10_000);
        return session;
    }

    private ChannelSftp openSftp(Session session) throws JSchException {
        Channel channel = session.openChannel("sftp");
        channel.connect(10_000);
        return (ChannelSftp) channel;
    }

    private CommandResult execute(Session session, String command, Duration timeout) {
        ByteArrayOutputStream stderr = new ByteArrayOutputStream();
        ChannelExec channel = null;
        try {
            channel = (ChannelExec) session.openChannel("exec");
            channel.setCommand(command);
            channel.setErrStream(stderr);
            InputStream stdout = channel.getInputStream();
            channel.connect(10_000);

            ByteArrayOutputStream out = new ByteArrayOutputStream();
            byte[] buffer = new byte[4096];
            long deadline = System.currentTimeMillis() + timeout.toMillis();

            while (true) {
                while (stdout.available() > 0) {
                    int read = stdout.read(buffer);
                    if (read < 0) {
                        break;
                    }
                    out.write(buffer, 0, read);
                }

                if (channel.isClosed()) {
                    while (stdout.available() > 0) {
                        int read = stdout.read(buffer);
                        if (read < 0) {
                            break;
                        }
                        out.write(buffer, 0, read);
                    }
                    int exitCode = channel.getExitStatus();
                    return new CommandResult(exitCode, out.toString(StandardCharsets.UTF_8), stderr.toString(StandardCharsets.UTF_8));
                }

                if (System.currentTimeMillis() > deadline) {
                    throw new ApiException(HttpStatus.GATEWAY_TIMEOUT, "远程命令执行超时。" );
                }

                Thread.sleep(100);
            }
        } catch (JSchException | IOException ex) {
            throw new ApiException(HttpStatus.BAD_GATEWAY, "远程命令执行失败: " + ex.getMessage());
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new ApiException(HttpStatus.INTERNAL_SERVER_ERROR, "远程命令执行被中断。" );
        } finally {
            if (channel != null && channel.isConnected()) {
                channel.disconnect();
            }
        }
    }

    private void executeOrThrow(Session session, String command, Duration timeout, String message) {
        CommandResult result = execute(session, command, timeout);
        if (result.exitCode() != 0) {
            log.warn("Remote command failed: {}", result.stderr());
            throw new ApiException(HttpStatus.BAD_GATEWAY, message + "，stderr=" + result.stderr());
        }
    }

    private void disconnectSession(Session session) {
        if (session != null && session.isConnected()) {
            session.disconnect();
        }
    }

    private void uploadText(ChannelSftp channel, String path, String content) {
        try (OutputStream outputStream = channel.put(path)) {
            outputStream.write(content.getBytes(StandardCharsets.UTF_8));
        } catch (SftpException | IOException ex) {
            throw new ApiException(HttpStatus.BAD_GATEWAY, "远程文件上传失败: " + ex.getMessage());
        }
    }

    private String buildEnvFile(Map<String, String> envVars) {
        StringBuilder builder = new StringBuilder();
        for (Map.Entry<String, String> entry : envVars.entrySet()) {
            builder.append("export ")
                .append(entry.getKey())
                .append("=")
                .append(shellQuote(entry.getValue()))
                .append('\n');
        }
        return builder.toString();
    }

    private String buildRunScript(String envPath, String exitPath, String shellBody) {
        return "#!/usr/bin/env bash\n"
            + "trap 'code=$?; printf \"%s\" \"$code\" > " + shellQuote(exitPath) + "' EXIT\n"
            + "set -e\n"
            + "set -a\n"
            + "source " + shellQuote(envPath) + "\n"
            + "rm -f " + shellQuote(envPath) + "\n"
            + "set +a\n"
            + shellBody
            + "\n";
    }

    private String decrypt(String encrypted) {
        return encrypted == null ? null : secretCryptoService.decrypt(encrypted);
    }

    private String resolveWhitelistedPath(ManagedProject project, String scope, String path) {
        String normalizedScope = scope == null ? "scripts" : scope.trim().toLowerCase();
        if ("absolute".equals(normalizedScope)) {
            String candidate = normalizePath(path);
            ensureWithinAllowedRoots(project, candidate);
            return candidate;
        }

        String base = switch (normalizedScope) {
            case "scripts" -> project.getScriptsDir();
            case "alert_output" -> project.getAlertOutputDir();
            case "experiment_results" -> project.getExperimentResultsDir();
            case "plots" -> project.getPlotsDir();
            case "logs" -> project.getLogsDir();
            case "origin" -> project.getOriginDataDir();
            case "transformed" -> project.getTransformedDataDir();
            default -> throw new ApiException(HttpStatus.BAD_REQUEST, "不支持的文件作用域: " + scope);
        };
        String candidate = path == null || path.isBlank()
            ? normalizePath(base)
            : (normalizePath(path).startsWith("/") ? normalizePath(path) : appendPath(base, path));
        String normalizedBase = normalizePath(base);
        if (!candidate.startsWith(normalizedBase)) {
            throw new ApiException(HttpStatus.BAD_REQUEST, "访问路径超出允许目录范围。" );
        }
        return candidate;
    }

    private void ensureWithinAllowedRoots(ManagedProject project, String candidate) {
        List<String> roots = List.of(
            normalizePath(project.getScriptsDir()),
            normalizePath(project.getAlertOutputDir()),
            normalizePath(project.getExperimentResultsDir()),
            normalizePath(project.getPlotsDir()),
            normalizePath(project.getLogsDir()),
            normalizePath(project.getOriginDataDir()),
            normalizePath(project.getTransformedDataDir())
        );
        boolean allowed = roots.stream().filter(root -> root != null && !root.isBlank()).anyMatch(candidate::startsWith);
        if (!allowed) {
            throw new ApiException(HttpStatus.BAD_REQUEST, "访问路径不在白名单目录中。" );
        }
    }

    private String normalizePath(String path) {
        if (path == null) {
            return null;
        }
        String normalized = path.replace('\\', '/').replaceAll("/+", "/");
        if (normalized.endsWith("/") && normalized.length() > 1) {
            normalized = normalized.substring(0, normalized.length() - 1);
        }
        return normalized;
    }

    private String appendPath(String root, String child) {
        String normalizedRoot = normalizePath(root);
        String normalizedChild = normalizePath(child);
        if (normalizedRoot == null || normalizedRoot.isBlank()) {
            return normalizedChild;
        }
        if (normalizedChild == null || normalizedChild.isBlank()) {
            return normalizedRoot;
        }
        if (normalizedChild.startsWith("/")) {
            return normalizedChild;
        }
        return normalizedRoot + "/" + normalizedChild;
    }

    private String shellQuote(String value) {
        if (value == null) {
            return "''";
        }
        return "'" + value.replace("'", "'\"'\"'") + "'";
    }
}