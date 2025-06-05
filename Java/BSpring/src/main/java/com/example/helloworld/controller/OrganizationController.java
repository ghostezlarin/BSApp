package com.example.helloworld.controller;

import com.example.helloworld.service.OrganizationService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/api/organizations")
@Tag(name = "Organization API")
public class OrganizationController {
    private final OrganizationService service;

    @Autowired
    public OrganizationController(OrganizationService service) {
        this.service = service;
    }

    @Operation(summary = "Получить информацию по счёту")
    @GetMapping("/account")
    public ResponseEntity<?> getAccountInfo(
            @Parameter(description = "Номер счёта", required = true)
            @RequestParam String account) {

        try {
            Map<String, Object> accountInfo = service.getAccountInfo(account);
            return ResponseEntity.ok(accountInfo);
        } catch (RuntimeException e) {
            return ResponseEntity.badRequest().body(e.getMessage());
        }
    }
}