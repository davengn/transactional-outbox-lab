package com.transactional.outbox.domain;

import jakarta.persistence.*;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;
import java.time.OffsetDateTime;
import java.util.UUID;

@Entity
@Table(name = "legacy_outbox_events")
public class LegacyOutboxEvent {

    @Id
    private UUID id;
    private String aggregateType;
    private String aggregateId;
    private String eventType;
    private UUID tenantId;
    private OffsetDateTime occurredAt;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(columnDefinition = "jsonb")
    private String payload;

    private boolean published;
    private OffsetDateTime publishedAt;

    protected LegacyOutboxEvent() { }

    public LegacyOutboxEvent(UUID id, String aggregateType, String aggregateId, String eventType,
        UUID tenantId, OffsetDateTime occurredAt, String payload) {
        this.id = id;
        this.aggregateType = aggregateType;
        this.aggregateId = aggregateId;
        this.eventType = eventType;
        this.tenantId = tenantId;
        this.occurredAt = occurredAt;
        this.payload = payload;
        this.published = false;
    }

    public UUID getId() { return id; }
    public String getEventType() { return eventType; }
    public String getPayload() { return payload; }
    public boolean isPublished() { return published; }
    public void markPublished(OffsetDateTime at) { this.published = true; this.publishedAt = at; }
}