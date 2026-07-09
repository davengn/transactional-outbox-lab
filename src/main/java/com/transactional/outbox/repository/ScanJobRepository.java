package com.transactional.outbox.repository;

import com.transactional.outbox.domain.ScanJob;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;

public interface ScanJobRepository extends JpaRepository<ScanJob, UUID> {


}
