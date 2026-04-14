package com.userchurn.manager.service;

import com.userchurn.manager.domain.ManagedProject;
import com.userchurn.manager.web.ApiException;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.io.IOException;
import java.io.StringReader;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Service
public class DashboardService {

    private final ProjectService projectService;
    private final RemoteAccessService remoteAccessService;

    public DashboardService(ProjectService projectService, RemoteAccessService remoteAccessService) {
        this.projectService = projectService;
        this.remoteAccessService = remoteAccessService;
    }

    @Transactional(readOnly = true)
    public Map<String, Object> loadDashboard(Long projectId) {
        ManagedProject project = projectService.getProject(projectId);

        Map<String, Object> summary;
        List<Map<String, Object>> riskUsers;
        List<Map<String, Object>> reasonCounts;
        String source;

        try {
            RemoteAccessService.DecryptedSecrets secrets = remoteAccessService.getSecrets(project);
            summary = queryLatestSummary(secrets);
            riskUsers = queryRiskUsers(secrets);
            reasonCounts = queryReasonCounts(secrets);
            source = "mysql+remote-files";
        } catch (Exception ex) {
            summary = readSingleCsvRow(project, project.getAlertOutputDir() + "/daily_summary.csv");
            riskUsers = parseRiskUsers(project);
            reasonCounts = aggregateReasons(riskUsers);
            source = "remote-files";
        }

        List<Map<String, Object>> correlationTop = parseCorrelation(project);
        List<Map<String, Object>> modelComparison = parseModelComparison(project);

        Map<String, Object> riskDistribution = Map.of(
            "high", toInt(summary.get("high_risk_count")),
            "medium", toInt(summary.get("medium_risk_count")),
            "low", toInt(summary.get("low_risk_count"))
        );

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("source", source);
        result.put("summary", summary);
        result.put("riskDistribution", riskDistribution);
        result.put("riskUsers", riskUsers);
        result.put("reasonCounts", reasonCounts);
        result.put("correlationTop", correlationTop);
        result.put("modelComparison", modelComparison);
        return result;
    }

    private Map<String, Object> queryLatestSummary(RemoteAccessService.DecryptedSecrets secrets) {
        String sql = "select stat_date,total_active_users,high_risk_count,medium_risk_count,low_risk_count,high_risk_rate,avg_churn_prob,top_stuck_map_id,d1_no_tutorial_count from ads_daily_churn_summary order by stat_date desc limit 1";
        try (var connection = DriverManager.getConnection(secrets.mysqlUrl(), secrets.mysqlUsername(), secrets.mysqlPassword());
             var statement = connection.createStatement();
             var resultSet = statement.executeQuery(sql)) {
            if (!resultSet.next()) {
                throw new ApiException(HttpStatus.NOT_FOUND, "ads_daily_churn_summary 暂无数据。" );
            }
            Map<String, Object> summary = new LinkedHashMap<>();
            summary.put("stat_date", resultSet.getString("stat_date"));
            summary.put("total_active_users", resultSet.getInt("total_active_users"));
            summary.put("high_risk_count", resultSet.getInt("high_risk_count"));
            summary.put("medium_risk_count", resultSet.getInt("medium_risk_count"));
            summary.put("low_risk_count", resultSet.getInt("low_risk_count"));
            summary.put("high_risk_rate", resultSet.getDouble("high_risk_rate"));
            summary.put("avg_churn_prob", resultSet.getDouble("avg_churn_prob"));
            summary.put("top_stuck_map_id", resultSet.getInt("top_stuck_map_id"));
            summary.put("d1_no_tutorial_count", resultSet.getInt("d1_no_tutorial_count"));
            return summary;
        } catch (SQLException ex) {
            throw new ApiException(HttpStatus.BAD_GATEWAY, "读取 MySQL 日汇总失败: " + ex.getMessage());
        }
    }

    private List<Map<String, Object>> queryRiskUsers(RemoteAccessService.DecryptedSecrets secrets) {
        String sql = "select user_id,churn_prob,risk_level,top_reason_1,top_reason_2,top_reason_3,predict_dt from ads_user_churn_risk order by predict_dt desc, churn_prob desc limit 20";
        try (var connection = DriverManager.getConnection(secrets.mysqlUrl(), secrets.mysqlUsername(), secrets.mysqlPassword());
             var statement = connection.createStatement();
             var resultSet = statement.executeQuery(sql)) {
            List<Map<String, Object>> rows = new ArrayList<>();
            while (resultSet.next()) {
                Map<String, Object> row = new LinkedHashMap<>();
                row.put("user_id", resultSet.getInt("user_id"));
                row.put("churn_prob", resultSet.getDouble("churn_prob"));
                row.put("risk_level", resultSet.getString("risk_level"));
                row.put("top_reason_1", resultSet.getString("top_reason_1"));
                row.put("top_reason_2", resultSet.getString("top_reason_2"));
                row.put("top_reason_3", resultSet.getString("top_reason_3"));
                row.put("predict_dt", resultSet.getString("predict_dt"));
                rows.add(row);
            }
            return rows;
        } catch (SQLException ex) {
            throw new ApiException(HttpStatus.BAD_GATEWAY, "读取 MySQL 风险用户失败: " + ex.getMessage());
        }
    }

    private List<Map<String, Object>> queryReasonCounts(RemoteAccessService.DecryptedSecrets secrets) {
        String sql = "select reason, count(*) as cnt from ("
            + " select top_reason_1 as reason from ads_user_churn_risk where top_reason_1 is not null and top_reason_1 <> ''"
            + " union all select top_reason_2 as reason from ads_user_churn_risk where top_reason_2 is not null and top_reason_2 <> ''"
            + " union all select top_reason_3 as reason from ads_user_churn_risk where top_reason_3 is not null and top_reason_3 <> ''"
            + ") t group by reason order by cnt desc limit 8";
        try (var connection = DriverManager.getConnection(secrets.mysqlUrl(), secrets.mysqlUsername(), secrets.mysqlPassword());
             var statement = connection.createStatement();
             var resultSet = statement.executeQuery(sql)) {
            List<Map<String, Object>> rows = new ArrayList<>();
            while (resultSet.next()) {
                rows.add(Map.of(
                    "reason", resultSet.getString("reason"),
                    "count", resultSet.getInt("cnt")
                ));
            }
            return rows;
        } catch (SQLException ex) {
            throw new ApiException(HttpStatus.BAD_GATEWAY, "读取 MySQL 流失原因失败: " + ex.getMessage());
        }
    }

    private Map<String, Object> readSingleCsvRow(ManagedProject project, String absolutePath) {
        List<Map<String, String>> rows = parseCsv(remoteAccessService.readTextFile(project, "absolute", absolutePath).get("content").toString());
        if (rows.isEmpty()) {
            throw new ApiException(HttpStatus.NOT_FOUND, "CSV 文件为空: " + absolutePath);
        }
        return new LinkedHashMap<>(rows.get(0));
    }

    private List<Map<String, Object>> parseRiskUsers(ManagedProject project) {
        List<Map<String, String>> rows = parseCsv(remoteAccessService.readTextFile(project, "absolute", project.getAlertOutputDir() + "/alert_result.csv").get("content").toString());
        return rows.stream()
            .sorted(Comparator.comparing((Map<String, String> row) -> parseDouble(row.get("churn_prob"))).reversed())
            .limit(20)
            .map(row -> {
                Map<String, Object> mapped = new LinkedHashMap<>();
                mapped.put("user_id", row.get("user_id"));
                mapped.put("churn_prob", parseDouble(row.get("churn_prob")));
                mapped.put("risk_level", row.get("risk_level"));
                mapped.put("top_reason_1", row.get("top_reason_1"));
                mapped.put("top_reason_2", row.get("top_reason_2"));
                mapped.put("top_reason_3", row.get("top_reason_3"));
                mapped.put("predict_dt", row.get("predict_dt"));
                return mapped;
            })
            .toList();
    }

    private List<Map<String, Object>> aggregateReasons(List<Map<String, Object>> riskUsers) {
        Map<String, Integer> counts = new LinkedHashMap<>();
        for (Map<String, Object> user : riskUsers) {
            for (String key : List.of("top_reason_1", "top_reason_2", "top_reason_3")) {
                Object value = user.get(key);
                if (value == null) {
                    continue;
                }
                String reason = value.toString().trim();
                if (!reason.isEmpty()) {
                    counts.merge(reason, 1, Integer::sum);
                }
            }
        }
        return counts.entrySet().stream()
            .sorted(Map.Entry.<String, Integer>comparingByValue().reversed())
            .limit(8)
            .map(entry -> {
                Map<String, Object> row = new LinkedHashMap<>();
                row.put("reason", entry.getKey());
                row.put("count", entry.getValue());
                return row;
            })
            .toList();
    }

    private List<Map<String, Object>> parseCorrelation(ManagedProject project) {
        String correlationPath = project.getProjectRoot() + "/eda_output/feature_label_corr.csv";
        List<Map<String, String>> rows = parseCsv(remoteAccessService.readTextFile(project, "absolute", correlationPath).get("content").toString());
        return rows.stream().limit(10).map(row -> {
            Map<String, Object> mapped = new LinkedHashMap<>();
            mapped.put("feature", row.get("feature"));
            mapped.put("abs_corr_with_label", parseDouble(row.get("abs_corr_with_label")));
            return mapped;
        }).toList();
    }

    private List<Map<String, Object>> parseModelComparison(ManagedProject project) {
        List<Map<String, String>> rows = parseCsv(remoteAccessService.readTextFile(project, "absolute", project.getExperimentResultsDir() + "/model_comparison_full.csv").get("content").toString());
        return rows.stream().map(row -> {
            Map<String, Object> mapped = new LinkedHashMap<>();
            mapped.put("model", row.get("model"));
            mapped.put("window", row.get("window"));
            mapped.put("auc", parseDouble(row.get("auc")));
            mapped.put("f1", parseDouble(row.get("f1")));
            mapped.put("accuracy", parseDouble(row.get("accuracy")));
            mapped.put("precision", parseDouble(row.get("precision")));
            mapped.put("recall", parseDouble(row.get("recall")));
            return mapped;
        }).toList();
    }

    private List<Map<String, String>> parseCsv(String text) {
        try (CSVParser parser = CSVFormat.DEFAULT.builder().setHeader().setSkipHeaderRecord(true).build().parse(new StringReader(text))) {
            List<Map<String, String>> rows = new ArrayList<>();
            for (CSVRecord record : parser) {
                Map<String, String> row = new LinkedHashMap<>();
                for (Map.Entry<String, Integer> header : parser.getHeaderMap().entrySet()) {
                    row.put(header.getKey(), record.get(header.getKey()));
                }
                rows.add(row);
            }
            return rows;
        } catch (IOException ex) {
            throw new ApiException(HttpStatus.BAD_GATEWAY, "CSV 解析失败: " + ex.getMessage());
        }
    }

    private double parseDouble(String value) {
        if (value == null || value.isBlank()) {
            return 0.0;
        }
        return Double.parseDouble(value);
    }

    private int toInt(Object value) {
        if (value == null) {
            return 0;
        }
        if (value instanceof Number number) {
            return number.intValue();
        }
        return Integer.parseInt(value.toString());
    }
}