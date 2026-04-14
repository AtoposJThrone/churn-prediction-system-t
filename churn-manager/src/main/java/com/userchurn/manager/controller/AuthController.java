package com.userchurn.manager.controller;

import com.userchurn.manager.service.AuthService;
import com.userchurn.manager.web.ApiResponse;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@RequestMapping("/api/auth")
public class AuthController {

    public record CredentialRequest(@NotBlank(message = "用户名不能为空。") String username,
                                    @NotBlank(message = "密码不能为空。") String password) {
    }

    private final AuthService authService;

    public AuthController(AuthService authService) {
        this.authService = authService;
    }

    @GetMapping("/status")
    public ApiResponse<Map<String, Object>> status() {
        return ApiResponse.ok(authService.getStatus());
    }

    @PostMapping("/init")
    public ApiResponse<Map<String, Object>> initialize(@Valid @RequestBody CredentialRequest request) {
        authService.initialize(request.username(), request.password());
        return ApiResponse.ok(Map.of("initialized", true));
    }

    @PostMapping("/login")
    public ApiResponse<Map<String, Object>> login(@Valid @RequestBody CredentialRequest request) {
        return ApiResponse.ok(authService.login(request.username(), request.password()));
    }

    @GetMapping("/me")
    public ApiResponse<Map<String, Object>> currentUser(HttpServletRequest request) {
        return ApiResponse.ok(Map.of("username", request.getAttribute("authUser")));
    }
}
