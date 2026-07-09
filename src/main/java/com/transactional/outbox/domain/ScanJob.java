package com.transactional.outbox.domain;

import jakarta.persistence.*;
import java.time.OffsetDateTime;
import java.util.UUID;

@Entity
@Table(name = "scan_jobs")
public class ScanJob {

    @Id
    private UUID id;
    private UUID tenantId;
    private String sourceName;
    private String status;
    private OffsetDateTime createdAt;

    protected ScanJob() { }

    public ScanJob(UUID id, UUID tenantId, String sourceName, String status, OffsetDateTime createdAt) {
        this.id = id;
        this.tenantId = tenantId;
        this.sourceName = sourceName;
        this.status = status;
        this.createdAt = createdAt;
    }

    public UUID getId() { return id; }
    public UUID getTenantId() { return tenantId; }
    public String getSourceName() { return sourceName; }
    public String getStatus() { return status; }
    public OffsetDateTime getCreatedAt() { return createdAt; }
}