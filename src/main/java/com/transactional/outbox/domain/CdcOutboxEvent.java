package com.transactional.outbox.domain;

import jakarta.persistence.*;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;
import java.time.OffsetDateTime;
import java.util.UUID;

@Entity
@Table(name = "cdc_outbox_events")
public class CdcOutboxEvent {

    @Id
    private UUID id;
    private String aggregateType;
    private String aggregateId;
    private String eventType;
    private Integer schemaVersion;
    private UUID tenantId;
    private OffsetDateTime occurredAt;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(columnDefinition = "jsonb")
    private String payload;

    protected CdcOutboxEvent() { }

    public CdcOutboxEvent(UUID id, String aggregateType, String aggregateId, String eventType,
        Integer schemaVersion, UUID tenantId, OffsetDateTime occurredAt, String payload) {
        this.id = id;
        this.aggregateType = aggregateType;
        this.aggregateId = aggregateId;
        this.eventType = eventType;
        this.schemaVersion = schemaVersion;
        this.tenantId = tenantId;
        this.occurredAt = occurredAt;
        this.payload = payload;
    }
}
