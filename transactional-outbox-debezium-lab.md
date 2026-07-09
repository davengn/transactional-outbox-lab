# Transactional Outbox + Debezium CDC — Experiment Lab

A hands-on lab to prove (or disprove) the Debezium outbox CDC pattern before adopting it, using a
throwaway Spring Boot 3 / Java 21 service instead of your real backend. Domain and field names below
mirror the `cdc_outbox_events` shape and go/no-go criteria from your CDC evaluation doc, so results
here transfer directly to that decision.

## 1. What this lab proves

This is not a "hello Debezium" tutorial. It's built to answer the specific open questions from your
migration plan:

| Question from your CDC evaluation | How this lab answers it |
|---|---|
| Does CDC preserve the same event envelope/semantics as the app worker? | Step 7: inspect the actual Kafka message and compare to your envelope shape |
| What happens to WAL/replication slot if the connector dies? | Step 10: kill Kafka Connect, watch `pg_replication_slots` grow |
| Does snapshot mode replay history we don't want replayed? | Step 11: pre-seed rows, register with two different `snapshot.mode` values |
| Does insert-only publication avoid noisy CDC events from cleanup deletes? | Step 12: run a cleanup job concurrently, confirm Debezium sees nothing |
| Is CDC actually faster/lower-overhead than polling? | Step 9: run the same workload through both paths, compare latency |
| Does heartbeat prevent slot stall on a quiet database? | Step 13: idle the app, watch `confirmed_flush_lsn` still advance |

Work through the steps in order — later steps assume the environment from earlier ones is still running.

## 2. Architecture of the lab

```text
                    ┌─────────────────────┐
  POST /scan-jobs → │ Spring Boot 3 app    │
                    │ (Java 21)            │
                    │  - writes scan_jobs   │──┐
                    │  - writes             │  │ same DB transaction
                    │    cdc_outbox_events   │──┘
                    └─────────┬────────────┘
                              │
                    ┌─────────▼────────────┐
                    │ PostgreSQL 16          │
                    │ wal_level=logical      │
                    │ publication:           │
                    │   publish = 'insert'   │
                    └─────────┬────────────┘
                              │ logical replication slot
                    ┌─────────▼────────────┐
                    │ Kafka Connect          │
                    │ + Debezium Postgres    │
                    │ + Outbox Event Router  │
                    └─────────┬────────────┘
                              │
                    ┌─────────▼────────────┐
                    │ Kafka (KRaft, single   │
                    │ broker)                │
                    │  topic: scan_job.created│
                    └────────────────────────┘

  Optional comparison path (Step 9):
  Spring Boot app also runs a naive polling publisher against a
  separate legacy_outbox_events table → topic: polling.scan_job.created
```

## 3. Prerequisites

- Docker + Docker Compose v2
- Java 21 (Temurin recommended)
- Maven 3.9+
- `curl` and `jq`
- `kcat` (formerly `kafkacat`) — for reading topics without writing a consumer. Install via your package
  manager (`brew install kcat`, `apt install kafkacat`, etc.)

Everything else (Postgres, Kafka, Kafka Connect, Debezium) runs in Docker — no local installs needed.

## 4. Step 1 — Bring up the environment

Create a working directory and `docker-compose.yml`:

```yaml
# docker-compose.yml
name: outbox-lab

services:
  postgres:
    image: postgres:16
    command: ["postgres", "-c", "wal_level=logical", "-c", "max_replication_slots=4", "-c", "max_wal_senders=4"]
    environment:
      POSTGRES_USER: dataflow
      POSTGRES_PASSWORD: dataflow
      POSTGRES_DB: outbox_lab
    ports:
      - "5432:5432"
    volumes:
      - ./sql:/docker-entrypoint-initdb.d

  kafka:
    image: apache/kafka:3.7.0
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_LISTENERS: PLAINTEXT://:9092,CONTROLLER://:9093,EXTERNAL://:9094
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092,EXTERNAL://localhost:9094
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,EXTERNAL:PLAINTEXT
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      CLUSTER_ID: "lab-cluster-outbox-1"
    ports:
      - "9094:9094"

  connect:
    image: debezium/connect:2.7
    depends_on:
      - kafka
      - postgres
    environment:
      BOOTSTRAP_SERVERS: kafka:9092
      GROUP_ID: outbox-lab-connect
      CONFIG_STORAGE_TOPIC: connect_configs
      OFFSET_STORAGE_TOPIC: connect_offsets
      STATUS_STORAGE_TOPIC: connect_statuses
      CONFIG_STORAGE_REPLICATION_FACTOR: 1
      OFFSET_STORAGE_REPLICATION_FACTOR: 1
      STATUS_STORAGE_REPLICATION_FACTOR: 1
    ports:
      - "8083:8083"

  kafka-ui:
    image: provectuslabs/kafka-ui:latest
    depends_on:
      - kafka
      - connect
    environment:
      KAFKA_CLUSTERS_0_NAME: lab
      KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: kafka:9092
      KAFKA_CLUSTERS_0_KAFKACONNECT_0_NAME: connect
      KAFKA_CLUSTERS_0_KAFKACONNECT_0_ADDRESS: http://connect:8083
    ports:
      - "8080:8080"
```

Create `sql/01-init.sql` (auto-run by the Postgres image on first boot):

```sql
-- sql/01-init.sql

create table scan_jobs (
    id uuid primary key default gen_random_uuid(),
    tenant_id uuid not null,
    source_name text not null,
    status text not null default 'PENDING',
    created_at timestamptz not null default now()
);

-- Shape matches the cdc_outbox_events design from the CDC evaluation doc
create table cdc_outbox_events (
    id uuid primary key default gen_random_uuid(),
    aggregate_type text not null,
    aggregate_id text not null,
    event_type text not null,
    schema_version int not null default 1,
    tenant_id uuid not null,
    occurred_at timestamptz not null default now(),
    payload jsonb not null
);

-- Comparison path only (Step 9): the "old" app-polling model, kept deliberately separate
-- from cdc_outbox_events so the two paths never double-publish the same row.
create table legacy_outbox_events (
    id uuid primary key default gen_random_uuid(),
    aggregate_type text not null,
    aggregate_id text not null,
    event_type text not null,
    tenant_id uuid not null,
    occurred_at timestamptz not null default now(),
    payload jsonb not null,
    published boolean not null default false,
    published_at timestamptz
);

create table heartbeat_ping (
    pinged_at timestamptz not null default now()
);

-- Critical: publish only inserts. Cleanup DELETEs and any future status UPDATEs
-- never generate WAL traffic Debezium has to filter through. See Step 12.
create publication cdc_outbox_pub for table cdc_outbox_events, heartbeat_ping with (publish = 'insert');
```

Start everything:

```bash
docker compose up -d
docker compose ps          # all 4 services should be "running"/"healthy"
```

**Check before moving on:** `docker exec -it outbox-lab-postgres-1 psql -U dataflow -d outbox_lab -c "\dt"` should list all four tables.

## 5. Step 2 — Scaffold the Spring Boot app

`pom.xml` (Java 21, Spring Boot 3.3):

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
  <modelVersion>4.0.0</modelVersion>
  <parent>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-parent</artifactId>
    <version>3.3.4</version>
  </parent>
  <groupId>com.dataflow.lab</groupId>
  <artifactId>outbox-lab</artifactId>
  <version>0.1.0</version>

  <properties>
    <java.version>21</java.version>
  </properties>

  <dependencies>
    <dependency>
      <groupId>org.springframework.boot</groupId>
      <artifactId>spring-boot-starter-web</artifactId>
    </dependency>
    <dependency>
      <groupId>org.springframework.boot</groupId>
      <artifactId>spring-boot-starter-data-jpa</artifactId>
    </dependency>
    <dependency>
      <groupId>org.springframework.kafka</groupId>
      <artifactId>spring-kafka</artifactId>
    </dependency>
    <dependency>
      <groupId>org.postgresql</groupId>
      <artifactId>postgresql</artifactId>
      <scope>runtime</scope>
    </dependency>
  </dependencies>

  <build>
    <plugins>
      <plugin>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-maven-plugin</artifactId>
      </plugin>
    </plugins>
  </build>
</project>
```

`src/main/resources/application.yml`:

```yaml
spring:
  datasource:
    url: jdbc:postgresql://localhost:5432/outbox_lab
    username: dataflow
    password: dataflow
  jpa:
    hibernate:
      ddl-auto: none    # schema is owned by sql/01-init.sql, not Hibernate
    open-in-view: false
  kafka:
    bootstrap-servers: localhost:9094
    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.apache.kafka.common.serialization.StringSerializer

outbox-lab:
  polling-publisher:
    enabled: false   # flip to true only for Step 9's comparison run
    interval-ms: 500
```

## 6. Step 3 — Entities and repositories

```java
// src/main/java/com/dataflow/lab/domain/ScanJob.java
package com.dataflow.lab.domain;

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
```

```java
// src/main/java/com/dataflow/lab/domain/CdcOutboxEvent.java
package com.dataflow.lab.domain;

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
```

```java
// src/main/java/com/dataflow/lab/domain/LegacyOutboxEvent.java
// Comparison path only — see Step 9.
package com.dataflow.lab.domain;

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
```

Repositories are plain Spring Data interfaces:

```java
package com.dataflow.lab.domain;

import org.springframework.data.jpa.repository.JpaRepository;
import java.util.List;
import java.util.UUID;

public interface ScanJobRepository extends JpaRepository<ScanJob, UUID> { }

public interface CdcOutboxEventRepository extends JpaRepository<CdcOutboxEvent, UUID> { }

public interface LegacyOutboxEventRepository extends JpaRepository<LegacyOutboxEvent, UUID> {
    List<LegacyOutboxEvent> findTop100ByPublishedFalseOrderByOccurredAtAsc();
}
```

## 7. Step 4 — The service that proves the outbox pattern

The whole point of the experiment is this method: one `@Transactional` boundary writes canonical
state and event intent together. Everything downstream (Debezium or the polling comparator) reads
from what this method committed.

```java
// src/main/java/com/dataflow/lab/ScanJobService.java
package com.dataflow.lab;

import com.dataflow.lab.domain.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.OffsetDateTime;
import java.util.Map;
import java.util.UUID;

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
            UUID.randomUUID(), "SCAN_JOB", jobId.toString(), "scan_job.created", 1, tenantId, now, payload
        ));

        // Path B: only written when comparing against the old polling model (Step 9)
        legacyOutboxRepository.save(new LegacyOutboxEvent(
            UUID.randomUUID(), "SCAN_JOB", jobId.toString(), "scan_job.created", tenantId, now, payload
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
```

```java
// src/main/java/com/dataflow/lab/ScanJobController.java
package com.dataflow.lab;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/scan-jobs")
public class ScanJobController {

    private final ScanJobService service;

    public ScanJobController(ScanJobService service) {
        this.service = service;
    }

    public record CreateScanJobRequest(UUID tenantId, String sourceName) { }

    @PostMapping
    public ResponseEntity<Map<String, String>> create(@RequestBody CreateScanJobRequest request) {
        UUID jobId = service.createScanJob(request.tenantId(), request.sourceName());
        return ResponseEntity.ok(Map.of("scanJobId", jobId.toString()));
    }
}
```

```java
// src/main/java/com/dataflow/lab/OutboxLabApplication.java
package com.dataflow.lab;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class OutboxLabApplication {
    public static void main(String[] args) {
        SpringApplication.run(OutboxLabApplication.class, args);
    }
}
```

Run it:

```bash
mvn spring-boot:run
```

**Check before moving on:**

```bash
curl -s -X POST localhost:8080/scan-jobs \
  -H 'Content-Type: application/json' \
  -d '{"tenantId":"11111111-1111-1111-1111-111111111111","sourceName":"prod-postgres-1"}' | jq
```

Confirm the row landed in both tables:

```bash
docker exec -it outbox-lab-postgres-1 psql -U dataflow -d outbox_lab \
  -c "select event_type, aggregate_id, occurred_at from cdc_outbox_events order by occurred_at desc limit 3;"
```

## 8. Step 5 — Register the Debezium connector

Save as `connector-config.json`:

```json
{
  "name": "scanjob-outbox-connector",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "database.hostname": "postgres",
    "database.port": "5432",
    "database.user": "dataflow",
    "database.password": "dataflow",
    "database.dbname": "outbox_lab",
    "topic.prefix": "lab",
    "schema.include.list": "public",
    "table.include.list": "public.cdc_outbox_events",
    "plugin.name": "pgoutput",
    "publication.name": "cdc_outbox_pub",
    "publication.autocreate.mode": "disabled",
    "slot.name": "cdc_outbox_slot",
    "snapshot.mode": "no_data",
    "heartbeat.interval.ms": "5000",
    "heartbeat.action.query": "insert into heartbeat_ping(pinged_at) values (now())",
    "transforms": "outbox",
    "transforms.outbox.type": "io.debezium.transforms.outbox.EventRouter",
    "transforms.outbox.table.field.event.id": "id",
    "transforms.outbox.table.field.event.key": "aggregate_id",
    "transforms.outbox.table.field.event.type": "event_type",
    "transforms.outbox.table.field.event.timestamp": "occurred_at",
    "transforms.outbox.table.field.event.payload": "payload",
    "transforms.outbox.route.by.field": "event_type",
    "transforms.outbox.route.topic.replacement": "${routedByValue}",
    "transforms.outbox.table.fields.additional.placement": "tenant_id:header:tenantId,schema_version:header:schemaVersion",
    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable": "false",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "false"
  }
}
```

Note the `route.by.field` / `route.topic.replacement` pair: this routes by **`event_type`**, not the
Debezium default of `aggregate_type`, so the Kafka topic ends up named `scan_job.created` — matching
your doc's stated intent that `event_type` is "target topic such as `catalog.metadata.accepted`,"
rather than Debezium's default `outbox.event.<aggregatetype>` naming.

Also note `publication.autocreate.mode: disabled` — the publication was already created explicitly in
`01-init.sql` with `publish = 'insert'`. If you let Debezium autocreate it, it defaults to capturing
all DML operations, which reopens exactly the "noisy CDC events" problem from your evaluation doc.

Register it:

```bash
curl -s -X POST localhost:8083/connectors \
  -H 'Content-Type: application/json' \
  -d @connector-config.json | jq

# Confirm it's running
curl -s localhost:8083/connectors/scanjob-outbox-connector/status | jq
```

**Check before moving on:** status should show `"state": "RUNNING"` for both the connector and its task.

## 9. Step 6 — Verify the event actually arrives correctly

Consume the topic:

```bash
kcat -b localhost:9094 -t scan_job.created -C -o beginning -f 'key=%k\nheaders=%h\npayload=%s\n\n'
```

POST another scan job from Step 4's curl command, and confirm:

- The message key matches `aggregate_id` (the `scan_job` UUID) — this is what preserves per-job
  ordering in Kafka partitioning.
- Headers include `tenantId` and `schemaVersion`.
- The payload body is your JSON envelope, not a raw Debezium-wrapped row (if you see `before`/`after`/
  `source` fields, the outbox router isn't active — check the `transforms` config).

This step is the direct proof for your evaluation doc's first open question: does CDC preserve the
same event envelope your processors already know how to consume, or does it leak physical row shape?
You should see the former.

## 10. Step 7 — Baseline vs. polling comparison

This step is your Phase 0 in miniature: is the thing you're replacing actually a bottleneck?

Enable the polling comparator by adding this class and flipping `outbox-lab.polling-publisher.enabled`
to `true` in `application.yml`:

```java
// src/main/java/com/dataflow/lab/LegacyPollingPublisher.java
package com.dataflow.lab;

import com.dataflow.lab.domain.LegacyOutboxEvent;
import com.dataflow.lab.domain.LegacyOutboxEventRepository;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.OffsetDateTime;
import java.util.List;

@Component
@ConditionalOnProperty(prefix = "outbox-lab.polling-publisher", name = "enabled", havingValue = "true")
public class LegacyPollingPublisher {

    private final LegacyOutboxEventRepository repository;
    private final KafkaTemplate<String, String> kafkaTemplate;

    public LegacyPollingPublisher(LegacyOutboxEventRepository repository,
                                   KafkaTemplate<String, String> kafkaTemplate) {
        this.repository = repository;
        this.kafkaTemplate = kafkaTemplate;
    }

    @Scheduled(fixedDelayString = "${outbox-lab.polling-publisher.interval-ms}")
    @Transactional
    public void publishPending() {
        List<LegacyOutboxEvent> pending = repository.findTop100ByPublishedFalseOrderByOccurredAtAsc();
        for (LegacyOutboxEvent event : pending) {
            kafkaTemplate.send("polling." + event.getEventType(), event.getId().toString(), event.getPayload());
            event.markPublished(OffsetDateTime.now());
        }
    }
}
```

Add `@EnableScheduling` to `OutboxLabApplication`.

Run the same workload through both paths (each `POST /scan-jobs` call writes to both
`cdc_outbox_events` and `legacy_outbox_events`, so this is an apples-to-apples comparison):

```bash
for i in $(seq 1 200); do
  curl -s -o /dev/null -X POST localhost:8080/scan-jobs \
    -H 'Content-Type: application/json' \
    -d "{\"tenantId\":\"11111111-1111-1111-1111-111111111111\",\"sourceName\":\"source-$i\"}"
done
```

Capture consume timestamps from both topics:

```bash
kcat -b localhost:9094 -t scan_job.created         -C -o beginning -f '%T scan_job.created\n' > cdc-times.log
kcat -b localhost:9094 -t polling.scan_job.created  -C -o beginning -f '%T polling.scan_job.created\n' > polling-times.log
```

`%T` gives you the Kafka broker's message timestamp in epoch milliseconds. Compare against your
request-send timestamps (log them from the curl loop, or add a client-side `date +%s%3N` before each
call) to get end-to-end latency distributions for each path.

**What you're looking for:** if CDC's p50/p99 latency isn't meaningfully better than the polling path
at your real event volume, that's evidence for your doc's own no-go criterion — *"current bottleneck
is [not] outbox publishing."* Don't let the lab's simplicity bias you toward CDC; a 500ms polling
interval on a toy workload will look artificially slow compared to WAL-tailing. Re-run with the
polling interval set closer to what you'd actually configure in production (e.g. 100–200ms) before
drawing conclusions.

## 11. Step 8 — Failure drill: kill the connector, watch WAL grow

This is the risk your evaluation doc calls the biggest new failure mode.

```bash
# Generate some events with the connector still running
for i in $(seq 1 20); do
  curl -s -o /dev/null -X POST localhost:8080/scan-jobs \
    -H 'Content-Type: application/json' -d "{\"tenantId\":\"11111111-1111-1111-1111-111111111111\",\"sourceName\":\"pre-kill-$i\"}"
done

# Kill Kafka Connect
docker compose stop connect

# Keep generating events while it's down
for i in $(seq 1 50); do
  curl -s -o /dev/null -X POST localhost:8080/scan-jobs \
    -H 'Content-Type: application/json' -d "{\"tenantId\":\"11111111-1111-1111-1111-111111111111\",\"sourceName\":\"during-outage-$i\"}"
done

# Watch the replication slot
docker exec -it outbox-lab-postgres-1 psql -U dataflow -d outbox_lab -c \
  "select slot_name, active, restart_lsn, confirmed_flush_lsn, pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) as retained_wal from pg_replication_slots;"
```

You should see `active = f` and `retained_wal` growing with each event insert while the connector is
down — this is WAL Postgres can't reclaim until the slot advances again. This is exactly the disk-
pressure risk your doc flags.

Bring the connector back and confirm it catches up without data loss:

```bash
docker compose start connect
sleep 15
docker exec -it outbox-lab-postgres-1 psql -U dataflow -d outbox_lab -c \
  "select slot_name, active, pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) as retained_wal from pg_replication_slots;"

kcat -b localhost:9094 -t scan_job.created -C -o beginning -e | grep -c 'during-outage'
```

**Check before moving on:** `retained_wal` should shrink back down after the connector resumes, and all
70 events (20 pre-kill + 50 during-outage) should eventually show up on the topic — none silently
dropped. If you want to push this further, also try `docker compose stop postgres` while Connect is
running, to see how the connector's own retry/backoff behaves against a dead source database.

## 12. Step 9 — Snapshot mode: does history replay when you don't want it to?

Your evaluation doc calls this out as an easy-to-misconfigure risk. Prove it both ways.

First, delete and re-create the connector with `snapshot.mode` set to `initial` (Debezium's default),
against a table that already has rows from earlier steps:

```bash
curl -X DELETE localhost:8083/connectors/scanjob-outbox-connector

# Edit connector-config.json: change "snapshot.mode": "no_data" to "snapshot.mode": "initial"
curl -s -X POST localhost:8083/connectors -H 'Content-Type: application/json' -d @connector-config-initial.json

kcat -b localhost:9094 -t scan_job.created -C -o beginning -c 20 -f '%s\n'
```

You should see every historical row replay to the topic immediately on connector start — including
events from Steps 4–8 that were already published once. This is the "entire event history republished
on first connector start" surprise from your doc, reproduced deliberately so you recognize it.

Now reset and re-register with `snapshot.mode: no_data` (what Step 5 originally used) against the same
table with existing history:

```bash
curl -X DELETE localhost:8083/connectors/scanjob-outbox-connector
docker exec -it outbox-lab-postgres-1 psql -U dataflow -d outbox_lab -c "select pg_drop_replication_slot('cdc_outbox_slot');"
curl -s -X POST localhost:8083/connectors -H 'Content-Type: application/json' -d @connector-config.json

# Insert one new row, confirm ONLY the new row appears, nothing historical
curl -s -X POST localhost:8080/scan-jobs -H 'Content-Type: application/json' \
  -d '{"tenantId":"11111111-1111-1111-1111-111111111111","sourceName":"post-reset-check"}'

kcat -b localhost:9094 -t scan_job.created -C -o end -e -f '%s\n'
```

**Check before moving on:** with `no_data`, only newly-inserted rows appear — this is the setting your
real migration must use, since your production `outbox_events`/`cdc_outbox_events` table will have
real history by the time you cut over.

## 13. Step 10 — Cleanup job vs. connector: prove there's no race

Your evaluation doc flags outbox table cleanup as something that "must not race connector consumption."
With `publish = 'insert'` on the publication, deletes should be invisible to Debezium entirely — prove it:

```bash
# Insert a few rows, let Debezium consume them, then delete them straight from Postgres
docker exec -it outbox-lab-postgres-1 psql -U dataflow -d outbox_lab -c \
  "delete from cdc_outbox_events where occurred_at < now() - interval '1 minute';"

# Connect logs should show no activity related to this delete
docker compose logs connect --since 1m | grep -i delete
```

**Check before moving on:** the delete succeeds against Postgres, and Kafka Connect's logs show nothing
about it — no tombstone, no error, no extra WAL processing. This confirms `publish = 'insert'` is doing
its job: cleanup and CDC can run concurrently without coordination.

## 14. Step 11 — Heartbeat: does the slot advance when the app goes quiet?

```bash
# Stop sending any new events, just watch
watch -n 5 'docker exec outbox-lab-postgres-1 psql -U dataflow -d outbox_lab -c \
  "select slot_name, confirmed_flush_lsn from pg_replication_slots;"'
```

Leave it running for ~1 minute with no new `scan_job` inserts. `confirmed_flush_lsn` should still tick
forward every ~5 seconds (matching `heartbeat.interval.ms`), driven by the `heartbeat.action.query`
inserting into `heartbeat_ping`. If you temporarily remove `heartbeat.action.query` from the connector
config and repeat this, you should see the slot's position stop advancing on a quiet database — this
is the exact failure mode the SeatGeek article describes solving.

## 15. Step 12 (optional/stretch) — The WAL-direct message variant

The article's "modern twist" bypasses the outbox table entirely using
`pg_logical_emit_message()` instead of an INSERT. This is worth understanding conceptually even if you
don't build the full custom SMT — the routing logic Debezium's built-in Outbox Event Router provides
for free on a table, you'd have to reimplement yourself for message-based capture.

Quick proof that the mechanism itself works, without building a routing SMT:

```sql
-- Run this directly; it writes a message into the WAL with no table involved
select pg_logical_emit_message(true, 'scan_job.created', '{"scanJobId":"test-123"}');
```

With `wal2json` or `pgoutput` and a Debezium connector configured for message decoding, this shows up
as a generic decoding message rather than a routed Kafka topic — because the built-in Outbox Event
Router assumes a table with `aggregate_type`/`event_type`/`payload` columns to route from, and
message-based capture has no such structure for it to read. Getting the same routing behavior as
Step 8 requires writing a custom Kafka Connect SMT that parses your own message-prefix convention (this
is genuinely a Java Kafka Connect development project, not a config change — budget it as a separate,
larger effort if you want to pursue this path instead of the table-based approach).

**Recommendation for your project:** stick with the table-based outbox (Steps 1–11). The WAL-direct
approach only pays off at SeatGeek's write-throughput scale, where the outbox table itself becomes a
lock-contention bottleneck — which isn't a problem your current-state assessment has evidence for yet.

## 16. Interpreting your results

Map what you observed back to your evaluation doc's actual go/no-go table:

| Go/no-go criterion | Lab evidence to check |
|---|---|
| Baseline shows outbox publishing is worth simplifying | Step 7 latency comparison, run at realistic polling interval |
| Team accepts Kafka Connect as a production dependency | Steps 5, 8, 11 — how much operational attention did Connect need? |
| Replication slot/WAL monitoring is implementable | Step 8's query is your starting alert — wire it into Prometheus/Grafana |
| Shadow topics prove event compatibility | Step 6 — envelope shape and headers matched expectations |
| Snapshot/replay/cleanup behavior rehearsed | Steps 9 and 10 |
| Privacy review of connector errors/DLQ/logs/headers | Not covered here — still needs a real review with actual payload fields |

If most of these came back clean and Step 7 showed a real latency or DB-load win at realistic volumes,
you have concrete grounds to move to your real Phase 1 (shadow mode against production shape). If
Step 7 didn't show a meaningful difference, that's a legitimate, evidence-based reason to stay with the
current app worker — possibly upgraded to LISTEN/NOTIFY-triggered polling — rather than adopting
Kafka Connect as a new production dependency for marginal gain.

## 17. Teardown

```bash
docker compose down -v   # -v also removes the Postgres data volume and WAL
```
