package com.transactional.outbox.repository;

import com.transactional.outbox.domain.LegacyOutboxEvent;
import java.util.List;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;

public interface LegacyOutboxEventRepository extends JpaRepository<LegacyOutboxEvent, UUID> {
    List<LegacyOutboxEvent> findTop100ByPublishedFalseOrderByOccurredAtAsc();
}
