package com.example.dlq.service;

import com.example.dlq.model.RetryHeaders;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.concurrent.CompletableFuture;

@Service
public class RetryService {

    private static final Logger logger = LoggerFactory.getLogger(RetryService.class);

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final ErrorClassifier errorClassifier;
    private final MetricsService metricsService;

    @Value("${kafka.topics.main}")
    private String mainTopic;

    @Value("${kafka.topics.retry-5s}")
    private String retry5sTopic;

    @Value("${kafka.topics.retry-30s}")
    private String retry30sTopic;

    @Value("${kafka.topics.retry-5m}")
    private String retry5mTopic;

    @Value("${kafka.topics.dlq}")
    private String dlqTopic;

    @Value("${retry.max-attempts}")
    private int maxAttempts;

    public RetryService(KafkaTemplate<String, String> kafkaTemplate, 
                       ObjectMapper objectMapper,
                       ErrorClassifier errorClassifier,
                       MetricsService metricsService) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.errorClassifier = errorClassifier;
        this.metricsService = metricsService;
    }

    @Transactional
    public CompletableFuture<SendResult<String, String>> sendToRetry(
            String key, String value, int currentRetryCount, 
            String originalTopic, int originalPartition, long originalOffset,
            Throwable error) {
        
        String nextTopic = getNextRetryTopic(currentRetryCount);
        if (nextTopic == null) {
            return sendToDlq(key, value, currentRetryCount, originalTopic, originalPartition, originalOffset, error);
        }

        RecordHeaders headers = createRetryHeaders(
            currentRetryCount, originalTopic, originalPartition, originalOffset, error
        );

        ProducerRecord<String, String> record = new ProducerRecord<>(nextTopic, null, key, value, headers);
        
        logger.info("Sending message to retry topic: {} (attempt {})", nextTopic, currentRetryCount + 1);
        metricsService.recordOrderRetry(nextTopic);
        
        return kafkaTemplate.send(record);
    }

    @Transactional
    public CompletableFuture<SendResult<String, String>> sendToDlq(
            String key, String value, int currentRetryCount,
            String originalTopic, int originalPartition, long originalOffset,
            Throwable error) {
        
        RecordHeaders headers = createDlqHeaders(
            currentRetryCount, originalTopic, originalPartition, originalOffset, error
        );

        ProducerRecord<String, String> record = new ProducerRecord<>(dlqTopic, null, key, value, headers);
        
        logger.warn("Sending message to DLQ: {} (final attempt {})", key, currentRetryCount + 1);
        metricsService.recordOrderDlq();
        
        return kafkaTemplate.send(record);
    }

    @Transactional
    public CompletableFuture<SendResult<String, String>> requeueFromDlq(
            String key, String value, String requeuedBy) {
        
        RecordHeaders headers = new RecordHeaders();
        headers.add(RetryHeaders.RETRY_COUNT, "0".getBytes());
        headers.add(RetryHeaders.FIRST_SEEN_TS, Instant.now().toString().getBytes());
        headers.add(RetryHeaders.REQUeUED_BY, requeuedBy.getBytes());
        headers.add(RetryHeaders.MAX_REQUEUES, "3".getBytes());

        ProducerRecord<String, String> record = new ProducerRecord<>(mainTopic, null, key, value, headers);
        
        logger.info("Requeuing message from DLQ: {} by {}", key, requeuedBy);
        metricsService.recordOrderRequeued();
        
        return kafkaTemplate.send(record);
    }

    private String getNextRetryTopic(int currentRetryCount) {
        if (currentRetryCount >= maxAttempts) {
            return null; // Go to DLQ
        }

        switch (currentRetryCount) {
            case 0: return retry5sTopic;
            case 1: return retry30sTopic;
            case 2: return retry5mTopic;
            default: return null;
        }
    }

    private RecordHeaders createRetryHeaders(int retryCount, String originalTopic, 
                                           int originalPartition, long originalOffset, Throwable error) {
        RecordHeaders headers = new RecordHeaders();
        
        headers.add(RetryHeaders.RETRY_COUNT, String.valueOf(retryCount + 1).getBytes());
        headers.add(RetryHeaders.FIRST_SEEN_TS, Instant.now().toString().getBytes());
        headers.add(RetryHeaders.LAST_ERROR, truncateString(error.getMessage(), 500).getBytes());
        headers.add(RetryHeaders.STACKTRACE, truncateString(getStackTrace(error), 1000).getBytes());
        headers.add(RetryHeaders.ORIGINAL_TOPIC, originalTopic.getBytes());
        headers.add(RetryHeaders.ORIGINAL_PARTITION, String.valueOf(originalPartition).getBytes());
        headers.add(RetryHeaders.ORIGINAL_OFFSET, String.valueOf(originalOffset).getBytes());
        
        // Set next retry time based on the retry topic
        String nextTopic = getNextRetryTopic(retryCount + 1);
        if (nextTopic != null) {
            Instant nextAt = calculateNextRetryTime(retryCount + 1);
            headers.add(RetryHeaders.NEXT_AT, nextAt.toString().getBytes());
        }
        
        return headers;
    }

    private RecordHeaders createDlqHeaders(int retryCount, String originalTopic, 
                                         int originalPartition, long originalOffset, Throwable error) {
        RecordHeaders headers = new RecordHeaders();
        
        headers.add(RetryHeaders.RETRY_COUNT, String.valueOf(retryCount + 1).getBytes());
        headers.add(RetryHeaders.FIRST_SEEN_TS, Instant.now().toString().getBytes());
        headers.add(RetryHeaders.LAST_ERROR, truncateString(error.getMessage(), 500).getBytes());
        headers.add(RetryHeaders.STACKTRACE, truncateString(getStackTrace(error), 1000).getBytes());
        headers.add(RetryHeaders.ORIGINAL_TOPIC, originalTopic.getBytes());
        headers.add(RetryHeaders.ORIGINAL_PARTITION, String.valueOf(originalPartition).getBytes());
        headers.add(RetryHeaders.ORIGINAL_OFFSET, String.valueOf(originalOffset).getBytes());
        
        return headers;
    }

    private Instant calculateNextRetryTime(int retryCount) {
        switch (retryCount) {
            case 1: return Instant.now().plusSeconds(5);
            case 2: return Instant.now().plusSeconds(30);
            case 3: return Instant.now().plusMinutes(5);
            default: return Instant.now();
        }
    }

    private String getStackTrace(Throwable throwable) {
        if (throwable == null) return "";
        
        java.io.StringWriter sw = new java.io.StringWriter();
        java.io.PrintWriter pw = new java.io.PrintWriter(sw);
        throwable.printStackTrace(pw);
        return sw.toString();
    }

    private String truncateString(String str, int maxLength) {
        if (str == null) return "";
        return str.length() > maxLength ? str.substring(0, maxLength) + "..." : str;
    }
}
