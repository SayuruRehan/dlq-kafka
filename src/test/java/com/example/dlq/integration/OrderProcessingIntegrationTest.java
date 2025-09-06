package com.example.dlq.integration;

import com.example.dlq.entity.DlqMessage;
import com.example.dlq.model.OrderEvent;
import com.example.dlq.producer.OrderProducer;
import com.example.dlq.repository.DlqMessageRepository;
import com.example.dlq.service.DlqService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;

import java.time.Instant;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@EmbeddedKafka(partitions = 1, topics = {
    "test-orders.v1",
    "test-orders.v1.retry.5s",
    "test-orders.v1.retry.30s", 
    "test-orders.v1.retry.5m",
    "test-orders.v1.dlq"
})
@DirtiesContext
@ActiveProfiles("test")
class OrderProcessingIntegrationTest {

    @Autowired
    private OrderProducer orderProducer;

    @Autowired
    private DlqMessageRepository dlqMessageRepository;

    @Autowired
    private DlqService dlqService;

    @Test
    void testNormalOrderProcessing() throws InterruptedException {
        // Create a normal order
        OrderEvent orderEvent = OrderEvent.create("customer-123", "product-456", 2, 50.0);
        
        // Send the order
        orderProducer.sendOrder(orderEvent);
        
        // Wait for processing
        Thread.sleep(2000);
        
        // Verify no DLQ messages were created
        assertThat(dlqMessageRepository.count()).isZero();
    }

    @Test
    void testPermanentErrorGoesToDlq() throws InterruptedException {
        // Create an order that will cause a permanent error
        OrderEvent orderEvent = new OrderEvent(
            "invalid-test-order",
            "customer-123",
            OrderEvent.OrderStatus.PENDING,
            Instant.now(),
            "product-456",
            2,
            50.0
        );
        
        // Send the order
        orderProducer.sendOrder(orderEvent);
        
        // Wait for processing and retries
        Thread.sleep(5000);
        
        // Verify DLQ message was created
        assertThat(dlqMessageRepository.count()).isGreaterThan(0);
        
        DlqMessage dlqMessage = dlqMessageRepository.findAll().get(0);
        assertThat(dlqMessage.getMessageKey()).isEqualTo("invalid-test-order");
        assertThat(dlqMessage.getLastError()).contains("Invalid order format");
    }

    @Test
    void testDlqMessageRequeue() throws InterruptedException {
        // First create a DLQ message
        DlqMessage dlqMessage = new DlqMessage(
            "test-order-key",
            "{\"orderId\":\"test-order-key\",\"customerId\":\"customer-123\"}",
            "test-orders.v1",
            0,
            123L,
            3,
            Instant.now(),
            "Test error",
            "Test stacktrace"
        );
        
        dlqMessageRepository.save(dlqMessage);
        
        // Requeue the message
        boolean success = dlqService.requeueMessage(dlqMessage.getId(), "test-user");
        
        assertThat(success).isTrue();
        
        // Verify requeue metadata was updated
        DlqMessage updatedMessage = dlqMessageRepository.findById(dlqMessage.getId()).orElse(null);
        assertThat(updatedMessage).isNotNull();
        assertThat(updatedMessage.getRequeuedCount()).isEqualTo(1);
        assertThat(updatedMessage.getRequeuedBy()).isEqualTo("test-user");
        assertThat(updatedMessage.getRequeuedAt()).isNotNull();
    }
}
