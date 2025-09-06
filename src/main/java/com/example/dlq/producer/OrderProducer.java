package com.example.dlq.producer;

import com.example.dlq.model.OrderEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;

@Component
public class OrderProducer {

    private static final Logger logger = LoggerFactory.getLogger(OrderProducer.class);

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    @Value("${kafka.topics.main}")
    private String mainTopic;

    public OrderProducer(KafkaTemplate<String, String> kafkaTemplate, ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    public CompletableFuture<SendResult<String, String>> sendOrder(OrderEvent orderEvent) {
        try {
            String key = orderEvent.getOrderId();
            String value = objectMapper.writeValueAsString(orderEvent);
            
            logger.info("Sending order event: {}", orderEvent.getOrderId());
            
            return kafkaTemplate.send(mainTopic, key, value)
                .thenApply(result -> {
                    logger.info("Successfully sent order event: {} to topic: {}", 
                               orderEvent.getOrderId(), result.getRecordMetadata().topic());
                    return result;
                })
                .exceptionally(throwable -> {
                    logger.error("Failed to send order event: {}, error: {}", 
                                orderEvent.getOrderId(), throwable.getMessage(), throwable);
                    return null;
                });
                
        } catch (Exception e) {
            logger.error("Error serializing order event: {}", orderEvent.getOrderId(), e);
            return CompletableFuture.failedFuture(e);
        }
    }
}
