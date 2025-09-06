package com.example.dlq.consumer;

import com.example.dlq.model.RetryHeaders;
import com.example.dlq.service.ErrorClassifier;
import com.example.dlq.service.OrderProcessingService;
import com.example.dlq.service.RetryService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;

@Component
public class RetryConsumer {

    private static final Logger logger = LoggerFactory.getLogger(RetryConsumer.class);

    private final OrderProcessingService orderProcessingService;
    private final RetryService retryService;
    private final ErrorClassifier errorClassifier;
    private final ObjectMapper objectMapper;

    @Value("${retry.max-attempts}")
    private int maxAttempts;

    public RetryConsumer(OrderProcessingService orderProcessingService,
                        RetryService retryService,
                        ErrorClassifier errorClassifier,
                        ObjectMapper objectMapper) {
        this.orderProcessingService = orderProcessingService;
        this.retryService = retryService;
        this.errorClassifier = errorClassifier;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "${kafka.topics.retry-5s}", groupId = "retry-consumer-group")
    @Transactional
    public void handleRetry5s(ConsumerRecord<String, String> record,
                             Acknowledgment acknowledgment,
                             @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                             @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                             @Header(KafkaHeaders.OFFSET) long offset) {
        
        handleRetryMessage(record, acknowledgment, topic, partition, offset, "5s");
    }

    @KafkaListener(topics = "${kafka.topics.retry-30s}", groupId = "retry-consumer-group")
    @Transactional
    public void handleRetry30s(ConsumerRecord<String, String> record,
                              Acknowledgment acknowledgment,
                              @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                              @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                              @Header(KafkaHeaders.OFFSET) long offset) {
        
        handleRetryMessage(record, acknowledgment, topic, partition, offset, "30s");
    }

    @KafkaListener(topics = "${kafka.topics.retry-5m}", groupId = "retry-consumer-group")
    @Transactional
    public void handleRetry5m(ConsumerRecord<String, String> record,
                             Acknowledgment acknowledgment,
                             @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                             @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                             @Header(KafkaHeaders.OFFSET) long offset) {
        
        handleRetryMessage(record, acknowledgment, topic, partition, offset, "5m");
    }

    private void handleRetryMessage(ConsumerRecord<String, String> record,
                                   Acknowledgment acknowledgment,
                                   String topic, int partition, long offset,
                                   String delayType) {
        
        String key = record.key();
        String value = record.value();
        
        logger.info("Processing retry message: key={}, topic={}, delay={}", key, topic, delayType);

        // Check if it's time to process this retry
        Instant nextAt = RetryHeaders.getNextAt(record.headers());
        if (nextAt != null && Instant.now().isBefore(nextAt)) {
            logger.info("Retry not yet due, skipping: key={}, nextAt={}", key, nextAt);
            // Don't acknowledge - let it be reprocessed later
            return;
        }

        // Apply delay based on retry topic
        applyDelay(delayType);

        try {
            // Parse and process the order event
            com.example.dlq.model.OrderEvent orderEvent = objectMapper.readValue(value, com.example.dlq.model.OrderEvent.class);
            orderProcessingService.processOrder(orderEvent);
            
            // If successful, acknowledge the message
            acknowledgment.acknowledge();
            logger.info("Successfully processed retry message: key={}, delay={}", key, delayType);
            
        } catch (Exception e) {
            logger.error("Error processing retry message: key={}, delay={}, error={}", 
                        key, delayType, e.getMessage(), e);
            
            // Get current retry count from headers
            int currentRetryCount = RetryHeaders.getRetryCount(record.headers());
            
            // Check if we should retry again
            if (errorClassifier.shouldRetry(e, currentRetryCount, maxAttempts)) {
                logger.info("Sending to next retry level: key={}, attempt={}", key, currentRetryCount + 1);
                
                // Send to next retry topic or DLQ
                retryService.sendToRetry(key, value, currentRetryCount, topic, partition, offset, e)
                    .thenAccept(result -> {
                        logger.info("Successfully sent to next retry level: {}", result.getRecordMetadata().topic());
                        acknowledgment.acknowledge();
                    })
                    .exceptionally(throwable -> {
                        logger.error("Failed to send to next retry level: {}", throwable.getMessage(), throwable);
                        // Don't acknowledge - let it retry
                        return null;
                    });
            } else {
                logger.warn("Max retries reached, sending to DLQ: key={}", key);
                
                // Send to DLQ
                retryService.sendToDlq(key, value, currentRetryCount, topic, partition, offset, e)
                    .thenAccept(result -> {
                        logger.info("Successfully sent to DLQ: {}", result.getRecordMetadata().topic());
                        acknowledgment.acknowledge();
                    })
                    .exceptionally(throwable -> {
                        logger.error("Failed to send to DLQ: {}", throwable.getMessage(), throwable);
                        // Don't acknowledge - let it retry
                        return null;
                    });
            }
        }
    }

    private void applyDelay(String delayType) {
        try {
            switch (delayType) {
                case "5s":
                    Thread.sleep(5000);
                    break;
                case "30s":
                    Thread.sleep(30000);
                    break;
                case "5m":
                    Thread.sleep(300000); // 5 minutes
                    break;
                default:
                    Thread.sleep(1000); // Default 1 second
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.warn("Delay interrupted for delay type: {}", delayType);
        }
    }
}
