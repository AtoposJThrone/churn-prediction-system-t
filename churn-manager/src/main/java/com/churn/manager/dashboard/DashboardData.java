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
        List<Map<String, Object>> mapHotspot,       // [{mapId, difficultyTier, failRate, ...}] 关卡卡关热力图
        double avgChurnProb,
        String dataSource,                          // "remote" | "mysql" | "fallback"
        List<Integer> churnProbDistribution         // [count_0_10, count_10_20, ..., count_90_100] 10 buckets
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
            long d1NoTutorialCount,
            // 新增扩展字段
            double avgBattlesPerUser,
            long stuckUserCount,
            double narrowWinRateOverall,
            String topStuckMapId2
    ) {}
}
