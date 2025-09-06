package com.example.dlq.consumer;

import com.example.dlq.model.OrderEvent;
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

@Component
public class OrderConsumer {

    private static final Logger logger = LoggerFactory.getLogger(OrderConsumer.class);

    private final OrderProcessingService orderProcessingService;
    private final RetryService retryService;
    private final ErrorClassifier errorClassifier;
    private final ObjectMapper objectMapper;

    @Value("${retry.max-attempts}")
    private int maxAttempts;

    public OrderConsumer(OrderProcessingService orderProcessingService,
                        RetryService retryService,
                        ErrorClassifier errorClassifier,
                        ObjectMapper objectMapper) {
        this.orderProcessingService = orderProcessingService;
        this.retryService = retryService;
        this.errorClassifier = errorClassifier;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "${kafka.topics.main}", groupId = "orders-consumer-group")
    @Transactional
    public void handleOrderEvent(ConsumerRecord<String, String> record,
                                Acknowledgment acknowledgment,
                                @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                                @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                                @Header(KafkaHeaders.OFFSET) long offset) {
        
        String key = record.key();
        String value = record.value();
        
        logger.info("Received order event: key={}, topic={}, partition={}, offset={}", 
                   key, topic, partition, offset);

        try {
            // Parse the order event
            OrderEvent orderEvent = objectMapper.readValue(value, OrderEvent.class);
            
            // Process the order
            orderProcessingService.processOrder(orderEvent);
            
            // If successful, acknowledge the message
            acknowledgment.acknowledge();
            logger.info("Successfully processed and acknowledged order: {}", orderEvent.getOrderId());
            
        } catch (Exception e) {
            logger.error("Error processing order event: key={}, error={}", key, e.getMessage(), e);
            
            // Get current retry count from headers
            int currentRetryCount = RetryHeaders.getRetryCount(record.headers());
            
            // Check if we should retry
            if (errorClassifier.shouldRetry(e, currentRetryCount, maxAttempts)) {
                logger.info("Sending order to retry: key={}, attempt={}", key, currentRetryCount + 1);
                
                // Send to retry topic
                retryService.sendToRetry(key, value, currentRetryCount, topic, partition, offset, e)
                    .thenAccept(result -> {
                        logger.info("Successfully sent to retry topic: {}", result.getRecordMetadata().topic());
                        acknowledgment.acknowledge();
                    })
                    .exceptionally(throwable -> {
                        logger.error("Failed to send to retry topic: {}", throwable.getMessage(), throwable);
                        // Don't acknowledge - let it retry
                        return null;
                    });
            } else {
                logger.warn("Max retries reached or permanent error, sending to DLQ: key={}", key);
                
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
}
