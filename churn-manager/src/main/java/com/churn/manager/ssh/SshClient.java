package com.churn.manager.ssh;

import com.churn.manager.common.ApiException;
import com.jcraft.jsch.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

/**
 * Thin wrapper around JSch for SSH command execution and SFTP file operations.
 * Each method opens and closes its own session — no connection pooling needed for MVP.
 */
@Component
public class SshClient {

    private static final Logger log = LoggerFactory.getLogger(SshClient.class);
    private static final int CONNECT_TIMEOUT_MS = 15_000;
    private static final int CMD_TIMEOUT_MS = 30_000;

    public record CommandResult(int exitCode, String stdout, String stderr) {}

    public record RemoteFileEntry(String name, String path, boolean directory, long size, Instant modifiedAt) {}

    // -------------------------------------------------------------------------
    // Public API
    // -------------------------------------------------------------------------

    public CommandResult executeCommand(String host, int port,
                                        String username, String password, String privateKey,
                                        String command) {
        Session session = openSession(host, port, username, password, privateKey);
        try {
            ChannelExec channel = (ChannelExec) session.openChannel("exec");
            channel.setCommand(command);
            ByteArrayOutputStream stdout = new ByteArrayOutputStream();
            ByteArrayOutputStream stderr = new ByteArrayOutputStream();
            channel.setOutputStream(stdout);
            channel.setErrStream(stderr);
            channel.connect(CONNECT_TIMEOUT_MS);
            waitForExit(channel, CMD_TIMEOUT_MS);
            int code = channel.getExitStatus();
            channel.disconnect();
            return new CommandResult(code,
                    stdout.toString(StandardCharsets.UTF_8),
                    stderr.toString(StandardCharsets.UTF_8));
        } catch (JSchException e) {
            throw new ApiException(HttpStatus.BAD_GATEWAY, "SSH command failed: " + e.getMessage());
        } finally {
            session.disconnect();
        }
    }

    /**
     * Launch a shell script in the background via nohup.
     * Writes env.sh + run.sh to remoteJobDir, then starts: nohup bash run.sh &gt; job.log 2&gt;&amp;1 &amp;
     *
     * @param envContent  content of env.sh (export K=V lines)
     * @param runContent  content of run.sh (the actual commands)
     * @param remoteJobDir  absolute path on the remote host
     * @return PID string
     */
    public String launchBackground(String host, int port,
                                   String username, String password, String privateKey,
                                   String remoteJobDir, String envContent, String runContent) {
        Session session = openSession(host, port, username, password, privateKey);
        try {
            // Create job directory
            runCommand(session, "mkdir -p " + shellEscape(remoteJobDir));
            // Write env.sh
            writeRemoteFile(session, remoteJobDir + "/env.sh", envContent);
            // Write run.sh
            writeRemoteFile(session, remoteJobDir + "/run.sh", runContent);
            // Execute in background
            String cmd = "cd " + shellEscape(remoteJobDir) +
                    " && chmod +x run.sh" +
                    " && nohup bash run.sh > job.log 2>&1 & echo $!";
            CommandResult r = runCommand(session, cmd);
            return r.stdout().trim();
        } finally {
            session.disconnect();
        }
    }

    public boolean isProcessAlive(String host, int port,
                                  String username, String password, String privateKey,
                                  String pid) {
        try {
            CommandResult r = executeCommand(host, port, username, password, privateKey,
                    "kill -0 " + pid + " 2>/dev/null && echo alive || echo dead");
            return r.stdout().trim().equals("alive");
        } catch (Exception e) {
            return false;
        }
    }

    public String readRemoteFile(String host, int port,
                                 String username, String password, String privateKey,
                                 String remotePath) {
        Session session = openSession(host, port, username, password, privateKey);
        try {
            ChannelSftp sftp = (ChannelSftp) session.openChannel("sftp");
            sftp.connect(CONNECT_TIMEOUT_MS);
            try (InputStream is = sftp.get(remotePath)) {
                return new String(is.readAllBytes(), StandardCharsets.UTF_8);
            } finally {
                sftp.disconnect();
            }
        } catch (SftpException e) {
            if (e.id == ChannelSftp.SSH_FX_NO_SUCH_FILE) return null;
            throw new ApiException(HttpStatus.BAD_GATEWAY, "SFTP read failed: " + e.getMessage());
        } catch (Exception e) {
            throw new ApiException(HttpStatus.BAD_GATEWAY, "SFTP error: " + e.getMessage());
        } finally {
            session.disconnect();
        }
    }

    public String readFileContent(String host, int port,
                                  String username, String password, String privateKey,
                                  String remotePath, int maxLines) {
        String content = readRemoteFile(host, port, username, password, privateKey, remotePath);
        if (content == null) return "";
        String[] lines = content.split("\n");
        if (lines.length <= maxLines) return content;
        StringBuilder sb = new StringBuilder();
        int start = lines.length - maxLines;
        for (int i = start; i < lines.length; i++) {
            sb.append(lines[i]).append('\n');
        }
        return "... (showing last " + maxLines + " lines) ...\n" + sb;
    }

    @SuppressWarnings("unchecked")
    public List<RemoteFileEntry> listDirectory(String host, int port,
                                               String username, String password, String privateKey,
                                               String remotePath) {
        Session session = openSession(host, port, username, password, privateKey);
        try {
            ChannelSftp sftp = (ChannelSftp) session.openChannel("sftp");
            sftp.connect(CONNECT_TIMEOUT_MS);
            List<RemoteFileEntry> entries = new ArrayList<>();
            try {
                Vector<ChannelSftp.LsEntry> ls = sftp.ls(remotePath);
                for (ChannelSftp.LsEntry e : ls) {
                    if (e.getFilename().equals(".") || e.getFilename().equals("..")) continue;
                    SftpATTRS attrs = e.getAttrs();
                    entries.add(new RemoteFileEntry(
                            e.getFilename(),
                            remotePath.endsWith("/") ? remotePath + e.getFilename() : remotePath + "/" + e.getFilename(),
                            attrs.isDir(),
                            attrs.getSize(),
                            Instant.ofEpochSecond(attrs.getMTime())
                    ));
                }
            } finally {
                sftp.disconnect();
            }
            return entries;
        } catch (SftpException e) {
            if (e.id == ChannelSftp.SSH_FX_NO_SUCH_FILE) {
                return List.of();
            }
            throw new ApiException(HttpStatus.BAD_GATEWAY, "SFTP ls failed: " + e.getMessage());
        } catch (Exception e) {
            throw new ApiException(HttpStatus.BAD_GATEWAY, "SFTP error: " + e.getMessage());
        } finally {
            session.disconnect();
        }
    }

    // -------------------------------------------------------------------------
    // Private helpers
    // -------------------------------------------------------------------------

    private Session openSession(String host, int port, String username, String password, String privateKey) {
        try {
            JSch jsch = new JSch();
            if (privateKey != null && !privateKey.isBlank()) {
                byte[] keyBytes = privateKey.getBytes(StandardCharsets.UTF_8);
                jsch.addIdentity("key", keyBytes, null, null);
            }
            Session session = jsch.getSession(username, host, port);
            if (password != null && !password.isBlank()) {
                session.setPassword(password);
            }
            session.setConfig("StrictHostKeyChecking", "no");
            session.setConfig("PreferredAuthentications", "publickey,password");
            session.connect(CONNECT_TIMEOUT_MS);
            return session;
        } catch (JSchException e) {
            throw new ApiException(HttpStatus.BAD_GATEWAY, "SSH connection failed (" + host + ":" + port + "): " + e.getMessage());
        }
    }

    private CommandResult runCommand(Session session, String command) {
        try {
            ChannelExec channel = (ChannelExec) session.openChannel("exec");
            channel.setCommand(command);
            ByteArrayOutputStream stdout = new ByteArrayOutputStream();
            ByteArrayOutputStream stderr = new ByteArrayOutputStream();
            channel.setOutputStream(stdout);
            channel.setErrStream(stderr);
            channel.connect(CONNECT_TIMEOUT_MS);
            waitForExit(channel, CMD_TIMEOUT_MS);
            int code = channel.getExitStatus();
            channel.disconnect();
            return new CommandResult(code,
                    stdout.toString(StandardCharsets.UTF_8),
                    stderr.toString(StandardCharsets.UTF_8));
        } catch (JSchException e) {
            throw new ApiException(HttpStatus.BAD_GATEWAY, "SSH command error: " + e.getMessage());
        }
    }

    private void writeRemoteFile(Session session, String remotePath, String content) {
        try {
            ChannelSftp sftp = (ChannelSftp) session.openChannel("sftp");
            sftp.connect(CONNECT_TIMEOUT_MS);
            try (InputStream is = new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8))) {
                sftp.put(is, remotePath);
            } finally {
                sftp.disconnect();
            }
        } catch (Exception e) {
            throw new ApiException(HttpStatus.BAD_GATEWAY, "SFTP write failed (" + remotePath + "): " + e.getMessage());
        }
    }

    private void waitForExit(ChannelExec channel, long timeoutMs) {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (!channel.isClosed() && System.currentTimeMillis() < deadline) {
            try { Thread.sleep(200); } catch (InterruptedException e) { Thread.currentThread().interrupt(); break; }
        }
    }

    /** Single-quote shell escaping for safe path usage in remote commands. */
    private static String shellEscape(String s) {
        return "'" + s.replace("'", "'\\''") + "'";
    }
}
