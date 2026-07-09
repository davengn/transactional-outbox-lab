package com.transactional.outbox.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.transactional.outbox.domain.CdcOutboxEvent;
import com.transactional.outbox.domain.LegacyOutboxEvent;
import com.transactional.outbox.domain.ScanJob;
import com.transactional.outbox.repository.CdcOutboxEventRepository;
import com.transactional.outbox.repository.LegacyOutboxEventRepository;
import com.transactional.outbox.repository.ScanJobRepository;
import jakarta.transaction.Transactional;
import java.time.OffsetDateTime;
import java.util.Map;
import java.util.UUID;
import org.springframework.stereotype.Service;

@Service
public class ScanJobService {

    private final ScanJobRepository scanJobRepository;
    private final CdcOutboxEventRepository cdcOutboxRepository;
    private final LegacyOutboxEventRepository legacyOutboxRepository;
    private final ObjectMapper objectMapper;

    public ScanJobService(ScanJobRepository scanJobRepository,
        CdcOutboxEventRepository cdcOutboxRepository,
        LegacyOutboxEventRepository legacyOutboxRepository,
        ObjectMapper objectMapper) {
        this.scanJobRepository = scanJobRepository;
        this.cdcOutboxRepository = cdcOutboxRepository;
        this.legacyOutboxRepository = legacyOutboxRepository;
        this.objectMapper = objectMapper;
    }

    @Transactional
    public UUID createScanJob(UUID tenantId, String sourceName) {
        UUID jobId = UUID.randomUUID();
        OffsetDateTime now = OffsetDateTime.now();

        scanJobRepository.save(new ScanJob(jobId, tenantId, sourceName, "PENDING", now));

        String payload = writeJson(Map.of(
            "scanJobId", jobId.toString(),
            "tenantId", tenantId.toString(),
            "sourceName", sourceName,
            "status", "PENDING",
            "occurredAt", now.toString()
        ));

        // Path A: CDC will pick this up via the replication slot (Step 8)
        cdcOutboxRepository.save(new CdcOutboxEvent(
            UUID.randomUUID(), "SCAN_JOB", jobId.toString(), "scan_job.created", 1, tenantId, now,
            payload
        ));

        // Path B: only written when comparing against the old polling model (Step 9)
        legacyOutboxRepository.save(new LegacyOutboxEvent(
            UUID.randomUUID(), "SCAN_JOB", jobId.toString(), "scan_job.created", tenantId, now,
            payload
        ));

        return jobId;
    }

    private String writeJson(Map<String, Object> value) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to serialize outbox payload", e);
        }
    }
}
