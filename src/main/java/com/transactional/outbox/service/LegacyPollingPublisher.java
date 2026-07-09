package com.transactional.outbox.service;

import com.transactional.outbox.domain.LegacyOutboxEvent;
import com.transactional.outbox.repository.LegacyOutboxEventRepository;
import jakarta.transaction.Transactional;
import java.time.OffsetDateTime;
import java.util.List;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

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
