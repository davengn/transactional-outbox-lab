package com.transactional.outbox.controller;

import com.transactional.outbox.service.ScanJobService;
import java.util.Map;
import java.util.UUID;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/scan-jobs")
public class ScanJobController {

    private final ScanJobService service;

    public ScanJobController(ScanJobService service) {
        this.service = service;
    }

    public record CreateScanJobRequest(UUID tenantId, String sourceName) {

    }

    @PostMapping
    public ResponseEntity<Map<String, String>> create(@RequestBody CreateScanJobRequest request) {
        UUID jobId = service.createScanJob(request.tenantId(), request.sourceName());
        return ResponseEntity.ok(Map.of("scanJobId", jobId.toString()));
    }
}
