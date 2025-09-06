package com.example.dlq.repository;

import com.example.dlq.entity.DlqMessage;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.List;

@Repository
public interface DlqMessageRepository extends JpaRepository<DlqMessage, Long> {

    Page<DlqMessage> findAllByOrderByCreatedAtDesc(Pageable pageable);

    List<DlqMessage> findByMessageKey(String messageKey);

    @Query("SELECT d FROM DlqMessage d WHERE d.createdAt < :cutoffTime ORDER BY d.createdAt ASC")
    List<DlqMessage> findOldestMessages(@Param("cutoffTime") Instant cutoffTime);

    @Query("SELECT COUNT(d) FROM DlqMessage d WHERE d.createdAt >= :since")
    Long countMessagesSince(@Param("since") Instant since);

    @Query("SELECT d FROM DlqMessage d WHERE d.requeuedCount < :maxRequeues ORDER BY d.createdAt DESC")
    Page<DlqMessage> findRequeueableMessages(@Param("maxRequeues") Integer maxRequeues, Pageable pageable);
}
