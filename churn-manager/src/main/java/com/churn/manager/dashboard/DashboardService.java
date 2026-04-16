package com.churn.manager.dashboard;

import com.churn.manager.common.ApiException;
import com.churn.manager.project.DecryptedSecrets;
import com.churn.manager.project.ManagedProject;
import com.churn.manager.project.ProjectService;
import com.churn.manager.ssh.SshClient;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ClassPathResource;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.*;

@Service
public class DashboardService {

    private static final Logger log = LoggerFactory.getLogger(DashboardService.class);

    private final ProjectService projectService;
    private final SshClient sshClient;

    public DashboardService(ProjectService projectService, SshClient sshClient) {
        this.projectService = projectService;
        this.sshClient = sshClient;
    }

    /**
     * Load dashboard data for a project.
     * Tier 1: SSH read remote CSVs (from project's alert_output / experiment_results dirs)
     * Tier 2: MySQL ADS query (if mysql credentials are available)
     * Tier 3: Bundled fallback CSVs from classpath
     */
    public DashboardData load(Long projectId) {
        if (projectId != null) {
            try {
                ManagedProject p = projectService.getOrThrow(projectId);
                DecryptedSecrets s = projectService.decryptSecrets(projectId);
                return loadFromRemote(p, s);
            } catch (ApiException e) {
                throw e;
            } catch (Exception e) {
                log.warn("Remote dashboard load failed for project {}: {}", projectId, e.getMessage());
            }
            // Tier 2: MySQL
            try {
                ManagedProject p = projectService.getOrThrow(projectId);
                DecryptedSecrets s = projectService.decryptSecrets(projectId);
                if (s.mysqlUrl() != null && !s.mysqlUrl().isBlank()) {
                    return loadFromMysql(s);
                }
            } catch (Exception e) {
                log.warn("MySQL dashboard load failed for project {}: {}", projectId, e.getMessage());
            }
        }
        // Tier 3: fallback
        return loadFallback();
    }

    // -------------------------------------------------------------------------
    // Tier 1: remote CSV via SSH
    // -------------------------------------------------------------------------

    private DashboardData loadFromRemote(ManagedProject p, DecryptedSecrets s) throws Exception {
        String alertDir = p.getAlertOutputDir();
        String expDir = p.getExperimentResultsDir();
        if (alertDir == null || expDir == null) throw new RuntimeException("目录未配置");

        String summaryContent = sshClient.readRemoteFile(
                p.getHost(), p.getSshPort(), s.sshUsername(), s.sshPassword(), s.sshPrivateKey(),
                alertDir + "/daily_summary.csv");
        String alertContent = sshClient.readRemoteFile(
                p.getHost(), p.getSshPort(), s.sshUsername(), s.sshPassword(), s.sshPrivateKey(),
                alertDir + "/alert_result.csv");
        String modelContent = sshClient.readRemoteFile(
                p.getHost(), p.getSshPort(), s.sshUsername(), s.sshPassword(), s.sshPrivateKey(),
                expDir + "/model_comparison_full.csv");
        String featContent = sshClient.readRemoteFile(
                p.getHost(), p.getSshPort(), s.sshUsername(), s.sshPassword(), s.sshPrivateKey(),
                expDir + "/feat_imp_randomforest.csv");

        if (summaryContent == null || alertContent == null) throw new RuntimeException("远程文件为空");

        return buildFromCsvStrings(summaryContent, alertContent, modelContent, featContent, "remote");
    }

    // -------------------------------------------------------------------------
    // Tier 2: MySQL ADS tables
    // -------------------------------------------------------------------------

    private DashboardData loadFromMysql(DecryptedSecrets s) throws Exception {
        try (Connection conn = DriverManager.getConnection(
                s.mysqlUrl(), s.mysqlUsername(), s.mysqlPassword())) {

            DashboardData.DailySummary summary = queryDailySummary(conn);
            List<Map<String, Object>> riskTrend = queryRiskTrend(conn);
            List<Map<String, Object>> modelComparison = queryModelComparison(conn);
            List<Map<String, Object>> featureImportance = queryFeatureImportance(conn);
            List<Map<String, Object>> recentAlerts = queryRecentAlerts(conn);

            List<Map<String, Object>> riskDistribution = List.of(
                    Map.of("name", "高风险", "value", summary.highRiskCount()),
                    Map.of("name", "中风险", "value", summary.mediumRiskCount()),
                    Map.of("name", "低风险", "value", summary.lowRiskCount())
            );

            return new DashboardData(summary, riskTrend, riskDistribution,
                    modelComparison, featureImportance, recentAlerts,
                    summary.avgChurnProb(), "mysql");
        }
    }

    private DashboardData.DailySummary queryDailySummary(Connection conn) throws Exception {
        String sql = "SELECT stat_date, total_active_users, high_risk_count, medium_risk_count, " +
                "low_risk_count, high_risk_rate, avg_churn_prob, top_stuck_map_id, d1_no_tutorial_count " +
                "FROM ads_daily_summary ORDER BY stat_date DESC LIMIT 1";
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            if (rs.next()) {
                return new DashboardData.DailySummary(
                        rs.getString("stat_date"), rs.getLong("total_active_users"),
                        rs.getLong("high_risk_count"), rs.getLong("medium_risk_count"),
                        rs.getLong("low_risk_count"), rs.getDouble("high_risk_rate"),
                        rs.getDouble("avg_churn_prob"), rs.getString("top_stuck_map_id"),
                        rs.getLong("d1_no_tutorial_count"));
            }
        }
        throw new RuntimeException("ads_daily_summary 无数据");
    }

    private List<Map<String, Object>> queryRiskTrend(Connection conn) throws Exception {
        String sql = "SELECT stat_date, high_risk_count, medium_risk_count, low_risk_count " +
                "FROM ads_daily_summary ORDER BY stat_date DESC LIMIT 30";
        return queryToList(conn, sql);
    }

    private List<Map<String, Object>> queryModelComparison(Connection conn) throws Exception {
        String sql = "SELECT model, window_type AS `window`, auc, f1 FROM ads_model_comparison";
        return queryToList(conn, sql);
    }

    private List<Map<String, Object>> queryFeatureImportance(Connection conn) throws Exception {
        String sql = "SELECT feature, importance FROM ads_feature_importance " +
                "ORDER BY importance DESC LIMIT 15";
        return queryToList(conn, sql);
    }

    private List<Map<String, Object>> queryRecentAlerts(Connection conn) throws Exception {
        String sql = "SELECT user_id, churn_prob, risk_level, top_reason_1 AS topReason " +
                "FROM ads_alert_result WHERE final_alert = 1 ORDER BY churn_prob DESC LIMIT 50";
        return queryToList(conn, sql);
    }

    private List<Map<String, Object>> queryToList(Connection conn, String sql) throws Exception {
        List<Map<String, Object>> list = new ArrayList<>();
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            ResultSetMetaData meta = rs.getMetaData();
            int cols = meta.getColumnCount();
            while (rs.next()) {
                Map<String, Object> row = new LinkedHashMap<>();
                for (int i = 1; i <= cols; i++) row.put(meta.getColumnLabel(i), rs.getObject(i));
                list.add(row);
            }
        }
        return list;
    }

    // -------------------------------------------------------------------------
    // Tier 3: classpath fallback
    // -------------------------------------------------------------------------

    public DashboardData loadFallback() {
        try {
            String summaryContent = readClasspath("fallback/daily_summary.csv");
            String alertContent = readClasspath("fallback/alert_result.csv");
            String modelContent = readClasspath("fallback/model_comparison_full.csv");
            String featContent = readClasspath("fallback/feat_imp_randomforest.csv");
            return buildFromCsvStrings(summaryContent, alertContent, modelContent, featContent, "fallback");
        } catch (Exception e) {
            log.error("Fallback load failed: {}", e.getMessage());
            throw new ApiException(HttpStatus.INTERNAL_SERVER_ERROR, "仪表盘数据加载失败：" + e.getMessage());
        }
    }

    // -------------------------------------------------------------------------
    // CSV parsing
    // -------------------------------------------------------------------------

    private DashboardData buildFromCsvStrings(String summaryStr, String alertStr,
                                               String modelStr, String featStr,
                                               String source) throws Exception {
        DashboardData.DailySummary summary = parseDailySummary(summaryStr);
        List<Map<String, Object>> recentAlerts = parseAlerts(alertStr);
        List<Map<String, Object>> modelComparison = modelStr != null ? parseModelComparison(modelStr) : List.of();
        List<Map<String, Object>> featureImportance = featStr != null ? parseFeatureImportance(featStr) : List.of();

        List<Map<String, Object>> riskDistribution = List.of(
                Map.of("name", "高风险", "value", summary.highRiskCount()),
                Map.of("name", "中风险", "value", summary.mediumRiskCount()),
                Map.of("name", "低风险", "value", summary.lowRiskCount())
        );

        // Build a single-row trend from the summary (no history in fallback)
        List<Map<String, Object>> riskTrend = List.of(Map.of(
                "date", summary.statDate(),
                "highRisk", summary.highRiskCount(),
                "mediumRisk", summary.mediumRiskCount(),
                "lowRisk", summary.lowRiskCount()
        ));

        return new DashboardData(summary, riskTrend, riskDistribution,
                modelComparison, featureImportance, recentAlerts,
                summary.avgChurnProb(), source);
    }

    private DashboardData.DailySummary parseDailySummary(String csv) throws Exception {
        try (CSVParser parser = CSVParser.parse(csv, CSVFormat.DEFAULT.builder().setHeader().setSkipHeaderRecord(true).build())) {
            for (CSVRecord r : parser) {
                return new DashboardData.DailySummary(
                        r.get("stat_date"),
                        parseLong(r.get("total_active_users")),
                        parseLong(r.get("high_risk_count")),
                        parseLong(r.get("medium_risk_count")),
                        parseLong(r.get("low_risk_count")),
                        parseDouble(r.get("high_risk_rate")),
                        parseDouble(r.get("avg_churn_prob")),
                        r.get("top_stuck_map_id"),
                        parseLong(r.get("d1_no_tutorial_count")));
            }
        }
        throw new RuntimeException("daily_summary.csv 无数据行");
    }

    private List<Map<String, Object>> parseAlerts(String csv) throws Exception {
        List<Map<String, Object>> list = new ArrayList<>();
        try (CSVParser parser = CSVParser.parse(csv, CSVFormat.DEFAULT.builder().setHeader().setSkipHeaderRecord(true).build())) {
            int count = 0;
            for (CSVRecord r : parser) {
                if (count++ >= 50) break;
                Map<String, Object> m = new LinkedHashMap<>();
                m.put("userId", r.get("user_id"));
                m.put("churnProb", parseDouble(r.get("churn_prob")));
                m.put("riskLevel", r.get("risk_level"));
                m.put("finalAlert", r.get("final_alert"));
                m.put("topReason", r.get("top_reason_1"));
                list.add(m);
            }
        }
        return list;
    }

    private List<Map<String, Object>> parseModelComparison(String csv) throws Exception {
        List<Map<String, Object>> list = new ArrayList<>();
        try (CSVParser parser = CSVParser.parse(csv, CSVFormat.DEFAULT.builder().setHeader().setSkipHeaderRecord(true).build())) {
            for (CSVRecord r : parser) {
                Map<String, Object> m = new LinkedHashMap<>();
                m.put("model", r.get("model"));
                m.put("window", r.get("window"));
                m.put("auc", parseDouble(r.get("auc")));
                m.put("f1", parseDouble(r.get("f1")));
                m.put("accuracy", parseDouble(r.get("accuracy")));
                m.put("precision", parseDouble(r.get("precision")));
                m.put("recall", parseDouble(r.get("recall")));
                list.add(m);
            }
        }
        return list;
    }

    private List<Map<String, Object>> parseFeatureImportance(String csv) throws Exception {
        List<Map<String, Object>> list = new ArrayList<>();
        try (CSVParser parser = CSVParser.parse(csv, CSVFormat.DEFAULT.builder().setHeader().setSkipHeaderRecord(true).build())) {
            int count = 0;
            for (CSVRecord r : parser) {
                if (count++ >= 15) break;
                Map<String, Object> m = new LinkedHashMap<>();
                m.put("feature", r.get("feature"));
                m.put("importance", parseDouble(r.get("importance")));
                list.add(m);
            }
        }
        return list;
    }

    private String readClasspath(String path) throws IOException {
        ClassPathResource res = new ClassPathResource(path);
        try (InputStream in = res.getInputStream()) {
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    private static double parseDouble(String s) {
        try { return Double.parseDouble(s.trim()); } catch (Exception e) { return 0.0; }
    }

    private static long parseLong(String s) {
        try { return Long.parseLong(s.trim()); } catch (Exception e) { return 0L; }
    }
}
