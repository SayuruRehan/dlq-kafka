package com.example.dlq.entity;

import jakarta.persistence.*;
import java.time.Instant;

@Entity
@Table(name = "dlq_messages")
public class DlqMessage {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "message_key", nullable = false)
    private String messageKey;

    @Column(name = "message_value", columnDefinition = "TEXT", nullable = false)
    private String messageValue;

    @Column(name = "original_topic", nullable = false)
    private String originalTopic;

    @Column(name = "original_partition", nullable = false)
    private Integer originalPartition;

    @Column(name = "original_offset", nullable = false)
    private Long originalOffset;

    @Column(name = "retry_count", nullable = false)
    private Integer retryCount;

    @Column(name = "first_seen_ts", nullable = false)
    private Instant firstSeenTs;

    @Column(name = "last_error", columnDefinition = "TEXT")
    private String lastError;

    @Column(name = "stacktrace", columnDefinition = "TEXT")
    private String stacktrace;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    @Column(name = "requeued_count", nullable = false)
    private Integer requeuedCount = 0;

    @Column(name = "requeued_by")
    private String requeuedBy;

    @Column(name = "requeued_at")
    private Instant requeuedAt;

    // Default constructor
    public DlqMessage() {}

    // Constructor for creating new DLQ message
    public DlqMessage(String messageKey, String messageValue, String originalTopic,
                     Integer originalPartition, Long originalOffset, Integer retryCount,
                     Instant firstSeenTs, String lastError, String stacktrace) {
        this.messageKey = messageKey;
        this.messageValue = messageValue;
        this.originalTopic = originalTopic;
        this.originalPartition = originalPartition;
        this.originalOffset = originalOffset;
        this.retryCount = retryCount;
        this.firstSeenTs = firstSeenTs;
        this.lastError = lastError;
        this.stacktrace = stacktrace;
        this.createdAt = Instant.now();
        this.requeuedCount = 0;
    }

    // Getters and Setters
    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }

    public String getMessageKey() { return messageKey; }
    public void setMessageKey(String messageKey) { this.messageKey = messageKey; }

    public String getMessageValue() { return messageValue; }
    public void setMessageValue(String messageValue) { this.messageValue = messageValue; }

    public String getOriginalTopic() { return originalTopic; }
    public void setOriginalTopic(String originalTopic) { this.originalTopic = originalTopic; }

    public Integer getOriginalPartition() { return originalPartition; }
    public void setOriginalPartition(Integer originalPartition) { this.originalPartition = originalPartition; }

    public Long getOriginalOffset() { return originalOffset; }
    public void setOriginalOffset(Long originalOffset) { this.originalOffset = originalOffset; }

    public Integer getRetryCount() { return retryCount; }
    public void setRetryCount(Integer retryCount) { this.retryCount = retryCount; }

    public Instant getFirstSeenTs() { return firstSeenTs; }
    public void setFirstSeenTs(Instant firstSeenTs) { this.firstSeenTs = firstSeenTs; }

    public String getLastError() { return lastError; }
    public void setLastError(String lastError) { this.lastError = lastError; }

    public String getStacktrace() { return stacktrace; }
    public void setStacktrace(String stacktrace) { this.stacktrace = stacktrace; }

    public Instant getCreatedAt() { return createdAt; }
    public void setCreatedAt(Instant createdAt) { this.createdAt = createdAt; }

    public Integer getRequeuedCount() { return requeuedCount; }
    public void setRequeuedCount(Integer requeuedCount) { this.requeuedCount = requeuedCount; }

    public String getRequeuedBy() { return requeuedBy; }
    public void setRequeuedBy(String requeuedBy) { this.requeuedBy = requeuedBy; }

    public Instant getRequeuedAt() { return requeuedAt; }
    public void setRequeuedAt(Instant requeuedAt) { this.requeuedAt = requeuedAt; }
}
