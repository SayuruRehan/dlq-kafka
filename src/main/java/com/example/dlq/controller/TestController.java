package com.example.dlq.controller;

import com.example.dlq.model.OrderEvent;
import com.example.dlq.producer.OrderProducer;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/api/test")
public class TestController {

    private final OrderProducer orderProducer;

    public TestController(OrderProducer orderProducer) {
        this.orderProducer = orderProducer;
    }

    @PostMapping("/orders")
    public ResponseEntity<Map<String, String>> createTestOrder(@RequestParam(defaultValue = "normal") String type) {
        String orderId = generateOrderId(type);
        
        OrderEvent orderEvent = OrderEvent.create(
            "customer-" + UUID.randomUUID().toString().substring(0, 8),
            "product-" + UUID.randomUUID().toString().substring(0, 8),
            1 + (int)(Math.random() * 5), // 1-5 quantity
            10.0 + (Math.random() * 100) // $10-$110 amount
        );
        
        // Override order ID to include failure type
        orderEvent = new OrderEvent(
            orderId,
            orderEvent.getCustomerId(),
            orderEvent.getStatus(),
            orderEvent.getTimestamp(),
            orderEvent.getProductId(),
            orderEvent.getQuantity(),
            orderEvent.getAmount()
        );

        orderProducer.sendOrder(orderEvent);

        Map<String, String> response = new HashMap<>();
        response.put("orderId", orderId);
        response.put("type", type);
        response.put("message", "Test order created and sent to Kafka");

        return ResponseEntity.ok(response);
    }

    @PostMapping("/orders/batch")
    public ResponseEntity<Map<String, String>> createBatchTestOrders(@RequestParam(defaultValue = "10") int count) {
        for (int i = 0; i < count; i++) {
            String type = getRandomFailureType();
            createTestOrder(type);
        }

        Map<String, String> response = new HashMap<>();
        response.put("count", String.valueOf(count));
        response.put("message", "Batch test orders created and sent to Kafka");

        return ResponseEntity.ok(response);
    }

    private String generateOrderId(String type) {
        String baseId = UUID.randomUUID().toString().substring(0, 8);
        return switch (type) {
            case "invalid" -> "invalid-" + baseId;
            case "timeout" -> "timeout-" + baseId;
            case "network" -> "network-" + baseId;
            case "validation" -> "validation-" + baseId;
            default -> "normal-" + baseId;
        };
    }

    private String getRandomFailureType() {
        String[] types = {"normal", "normal", "normal", "normal", "normal", // 50% normal
                         "invalid", "timeout", "network", "validation"}; // 12.5% each failure type
        return types[(int)(Math.random() * types.length)];
    }
}
