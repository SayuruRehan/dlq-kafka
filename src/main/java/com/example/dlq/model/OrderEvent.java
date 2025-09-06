package com.example.dlq.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

import java.time.Instant;
import java.util.UUID;

public class OrderEvent {
    
    @NotBlank
    private final String orderId;
    
    @NotBlank
    private final String customerId;
    
    @NotNull
    private final OrderStatus status;
    
    @NotNull
    private final Instant timestamp;
    
    private final String productId;
    private final Integer quantity;
    private final Double amount;

    @JsonCreator
    public OrderEvent(
            @JsonProperty("orderId") String orderId,
            @JsonProperty("customerId") String customerId,
            @JsonProperty("status") OrderStatus status,
            @JsonProperty("timestamp") Instant timestamp,
            @JsonProperty("productId") String productId,
            @JsonProperty("quantity") Integer quantity,
            @JsonProperty("amount") Double amount) {
        this.orderId = orderId;
        this.customerId = customerId;
        this.status = status;
        this.timestamp = timestamp;
        this.productId = productId;
        this.quantity = quantity;
        this.amount = amount;
    }

    public static OrderEvent create(String customerId, String productId, Integer quantity, Double amount) {
        return new OrderEvent(
            UUID.randomUUID().toString(),
            customerId,
            OrderStatus.PENDING,
            Instant.now(),
            productId,
            quantity,
            amount
        );
    }

    // Getters
    public String getOrderId() { return orderId; }
    public String getCustomerId() { return customerId; }
    public OrderStatus getStatus() { return status; }
    public Instant getTimestamp() { return timestamp; }
    public String getProductId() { return productId; }
    public Integer getQuantity() { return quantity; }
    public Double getAmount() { return amount; }

    @Override
    public String toString() {
        return "OrderEvent{" +
                "orderId='" + orderId + '\'' +
                ", customerId='" + customerId + '\'' +
                ", status=" + status +
                ", timestamp=" + timestamp +
                ", productId='" + productId + '\'' +
                ", quantity=" + quantity +
                ", amount=" + amount +
                '}';
    }
}
