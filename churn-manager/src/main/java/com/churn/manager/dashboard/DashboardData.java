package com.churn.manager.dashboard;

import java.util.List;
import java.util.Map;

/** Aggregate view model for the dashboard page. */
public record DashboardData(
        DailySummary summary,
        List<Map<String, Object>> riskTrend,        // [{date, highRisk, mediumRisk, lowRisk}]
        List<Map<String, Object>> riskDistribution, // [{name, value}] for pie chart
        List<Map<String, Object>> modelComparison,  // [{model, window, auc, f1}]
        List<Map<String, Object>> featureImportance,// [{feature, importance}]
        List<Map<String, Object>> recentAlerts,     // [{userId, churnProb, riskLevel, topReason}]
        double avgChurnProb,
        String dataSource                           // "remote" | "mysql" | "fallback"
) {
    public record DailySummary(
            String statDate,
            long totalActiveUsers,
            long highRiskCount,
            long mediumRiskCount,
            long lowRiskCount,
            double highRiskRate,
            double avgChurnProb,
            String topStuckMapId,
            long d1NoTutorialCount
    ) {}
}
