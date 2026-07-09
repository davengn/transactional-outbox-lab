package com.transactional.outbox.repository;

import com.transactional.outbox.domain.CdcOutboxEvent;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;

public interface CdcOutboxEventRepository extends JpaRepository<CdcOutboxEvent, UUID> { }


