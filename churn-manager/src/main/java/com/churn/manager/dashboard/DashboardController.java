package com.churn.manager.dashboard;

import com.churn.manager.common.ApiResponse;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/dashboard")
public class DashboardController {

    private final DashboardService dashboardService;

    public DashboardController(DashboardService dashboardService) {
        this.dashboardService = dashboardService;
    }

    /**
     * GET /api/dashboard?projectId={id}
     * If projectId is omitted, returns fallback data directly.
     */
    @GetMapping
    public ApiResponse<DashboardData> getDashboard(
            @RequestParam(value = "projectId", required = false) Long projectId) {
        return ApiResponse.ok(dashboardService.load(projectId));
    }

    /** Explicit fallback endpoint — always returns bundled data */
    @GetMapping("/fallback")
    public ApiResponse<DashboardData> getFallback() {
        return ApiResponse.ok(dashboardService.loadFallback());
    }
}
