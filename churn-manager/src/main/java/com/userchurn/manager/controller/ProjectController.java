package com.userchurn.manager.controller;

import com.userchurn.manager.domain.ManagedProject;
import com.userchurn.manager.service.ProjectService;
import com.userchurn.manager.service.RemoteAccessService;
import com.userchurn.manager.web.ApiResponse;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/projects")
public class ProjectController {

    public record ProjectRequest(
        @NotBlank(message = "项目名称不能为空。") String name,
        String description,
        @NotBlank(message = "主机地址不能为空。") String host,
        Integer sshPort,
        String projectRoot,
        String scriptsDir,
        String alertOutputDir,
        String experimentResultsDir,
        String plotsDir,
        String logsDir,
        String originDataDir,
        String transformedDataDir,
        String hiveDb,
        String hdfsLandingDir,
        String pythonCommand,
        String sparkSubmitCommand,
        String beelineCommand,
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

    private final ProjectService projectService;
    private final RemoteAccessService remoteAccessService;

    public ProjectController(ProjectService projectService, RemoteAccessService remoteAccessService) {
        this.projectService = projectService;
        this.remoteAccessService = remoteAccessService;
    }

    @GetMapping
    public ApiResponse<List<Map<String, Object>>> list() {
        return ApiResponse.ok(projectService.listProjects());
    }

    @GetMapping("/{projectId}")
    public ApiResponse<Map<String, Object>> detail(@PathVariable Long projectId) {
        return ApiResponse.ok(projectService.getProjectView(projectId));
    }

    @PostMapping
    public ApiResponse<Map<String, Object>> create(@Valid @RequestBody ProjectRequest request) {
        return ApiResponse.ok(projectService.createProject(toCommand(request)));
    }

    @PutMapping("/{projectId}")
    public ApiResponse<Map<String, Object>> update(@PathVariable Long projectId, @Valid @RequestBody ProjectRequest request) {
        return ApiResponse.ok(projectService.updateProject(projectId, toCommand(request)));
    }

    @PostMapping("/{projectId}/test-connection")
    public ApiResponse<Map<String, Object>> testConnection(@PathVariable Long projectId) {
        ManagedProject project = projectService.getProject(projectId);
        return ApiResponse.ok(remoteAccessService.testConnections(project));
    }

    @GetMapping("/{projectId}/files")
    public ApiResponse<List<RemoteAccessService.RemoteFileEntry>> listFiles(@PathVariable Long projectId,
                                                                            @RequestParam String scope,
                                                                            @RequestParam(required = false) String path) {
        ManagedProject project = projectService.getProject(projectId);
        return ApiResponse.ok(remoteAccessService.listDirectory(project, scope, path));
    }

    @GetMapping("/{projectId}/file-content")
    public ApiResponse<Map<String, Object>> fileContent(@PathVariable Long projectId,
                                                        @RequestParam String scope,
                                                        @RequestParam String path) {
        ManagedProject project = projectService.getProject(projectId);
        return ApiResponse.ok(remoteAccessService.readTextFile(project, scope, path));
    }

    private ProjectService.ProjectCommand toCommand(ProjectRequest request) {
        return new ProjectService.ProjectCommand(
            request.name(),
            request.description(),
            request.host(),
            request.sshPort(),
            request.projectRoot(),
            request.scriptsDir(),
            request.alertOutputDir(),
            request.experimentResultsDir(),
            request.plotsDir(),
            request.logsDir(),
            request.originDataDir(),
            request.transformedDataDir(),
            request.hiveDb(),
            request.hdfsLandingDir(),
            request.pythonCommand(),
            request.sparkSubmitCommand(),
            request.beelineCommand(),
            request.sshUsername(),
            request.sshPassword(),
            request.sshPrivateKey(),
            request.mysqlUrl(),
            request.mysqlUsername(),
            request.mysqlPassword(),
            request.hiveJdbcUrl(),
            request.hiveUsername(),
            request.hivePassword()
        );
    }
}
