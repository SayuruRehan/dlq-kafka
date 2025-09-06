package com.example.dlq.service;

import com.example.dlq.entity.DlqMessage;
import com.example.dlq.repository.DlqMessageRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

@Service
public class DlqService {

    private static final Logger logger = LoggerFactory.getLogger(DlqService.class);

    private final DlqMessageRepository dlqMessageRepository;
    private final RetryService retryService;

    public DlqService(DlqMessageRepository dlqMessageRepository, RetryService retryService) {
        this.dlqMessageRepository = dlqMessageRepository;
        this.retryService = retryService;
    }

    public Page<DlqMessage> getDlqMessages(Pageable pageable) {
        return dlqMessageRepository.findAllByOrderByCreatedAtDesc(pageable);
    }

    public Optional<DlqMessage> getDlqMessage(Long id) {
        return dlqMessageRepository.findById(id);
    }

    public List<DlqMessage> getDlqMessagesByKey(String messageKey) {
        return dlqMessageRepository.findByMessageKey(messageKey);
    }

    @Transactional
    public boolean requeueMessage(Long id, String requeuedBy) {
        Optional<DlqMessage> dlqMessageOpt = dlqMessageRepository.findById(id);
        
        if (dlqMessageOpt.isEmpty()) {
            logger.warn("DLQ message not found for requeue: id={}", id);
            return false;
        }

        DlqMessage dlqMessage = dlqMessageOpt.get();
        
        // Check if message can be requeued (max requeues limit)
        if (dlqMessage.getRequeuedCount() >= 3) {
            logger.warn("Message has reached max requeue limit: id={}, requeuedCount={}", 
                       id, dlqMessage.getRequeuedCount());
            return false;
        }

        try {
            // Requeue the message
            retryService.requeueFromDlq(dlqMessage.getMessageKey(), dlqMessage.getMessageValue(), requeuedBy)
                .thenAccept(result -> {
                    logger.info("Successfully requeued message: id={}, key={}", id, dlqMessage.getMessageKey());
                })
                .exceptionally(throwable -> {
                    logger.error("Failed to requeue message: id={}, error={}", id, throwable.getMessage(), throwable);
                    return null;
                });

            // Update requeue metadata
            dlqMessage.setRequeuedCount(dlqMessage.getRequeuedCount() + 1);
            dlqMessage.setRequeuedBy(requeuedBy);
            dlqMessage.setRequeuedAt(Instant.now());
            dlqMessageRepository.save(dlqMessage);

            logger.info("Updated requeue metadata for message: id={}, requeuedBy={}", id, requeuedBy);
            return true;

        } catch (Exception e) {
            logger.error("Error requeuing message: id={}, error={}", id, e.getMessage(), e);
            return false;
        }
    }

    @Transactional
    public void deleteDlqMessage(Long id) {
        dlqMessageRepository.deleteById(id);
        logger.info("Deleted DLQ message: id={}", id);
    }

    public List<DlqMessage> getOldestMessages(Instant cutoffTime) {
        return dlqMessageRepository.findOldestMessages(cutoffTime);
    }

    public Long getMessageCountSince(Instant since) {
        return dlqMessageRepository.countMessagesSince(since);
    }

    public Page<DlqMessage> getRequeueableMessages(Pageable pageable) {
        return dlqMessageRepository.findRequeueableMessages(3, pageable);
    }
}
