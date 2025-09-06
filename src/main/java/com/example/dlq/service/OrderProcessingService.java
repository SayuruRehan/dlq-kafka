package com.example.dlq.service;

import com.example.dlq.exception.ErrorType;
import com.example.dlq.exception.OrderProcessingException;
import com.example.dlq.model.OrderEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.Random;

@Service
public class OrderProcessingService {

    private static final Logger logger = LoggerFactory.getLogger(OrderProcessingService.class);
    private final ObjectMapper objectMapper;
    private final MetricsService metricsService;
    private final Random random = new Random();

    public OrderProcessingService(ObjectMapper objectMapper, MetricsService metricsService) {
        this.objectMapper = objectMapper;
        this.metricsService = metricsService;
    }

    public void processOrder(OrderEvent orderEvent) throws OrderProcessingException {
        logger.info("Processing order: {}", orderEvent.getOrderId());
        
        Timer.Sample sample = metricsService.startProcessingTimer();

        try {
            // Simulate different types of failures for demonstration
            simulateProcessingFailure(orderEvent);

            // Simulate actual processing
            Thread.sleep(100); // Simulate processing time
            
            // Record successful processing
            metricsService.recordOrderProcessed();
            metricsService.recordProcessingTime(sample);
            logger.info("Successfully processed order: {}", orderEvent.getOrderId());
            
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            metricsService.recordProcessingError("interrupted");
            throw new OrderProcessingException("Processing interrupted", ErrorType.TRANSIENT, orderEvent.getOrderId(), e);
        } catch (OrderProcessingException e) {
            metricsService.recordProcessingError(e.getErrorType().name().toLowerCase());
            throw e;
        } catch (Exception e) {
            metricsService.recordProcessingError("unknown");
            throw new OrderProcessingException("Unexpected error", ErrorType.TRANSIENT, orderEvent.getOrderId(), e);
        }
    }

    private void simulateProcessingFailure(OrderEvent orderEvent) throws OrderProcessingException {
        String orderId = orderEvent.getOrderId();
        
        // Simulate different failure scenarios based on order ID patterns
        if (orderId.contains("invalid")) {
            throw new OrderProcessingException(
                "Invalid order format: " + orderId, 
                ErrorType.PERMANENT, 
                orderId
            );
        }
        
        if (orderId.contains("timeout")) {
            throw new OrderProcessingException(
                "Processing timeout for order: " + orderId, 
                ErrorType.TRANSIENT, 
                orderId
            );
        }
        
        if (orderId.contains("network")) {
            throw new OrderProcessingException(
                "Network error processing order: " + orderId, 
                ErrorType.TRANSIENT, 
                orderId
            );
        }
        
        if (orderId.contains("validation")) {
            throw new OrderProcessingException(
                "Validation failed for order: " + orderId, 
                ErrorType.PERMANENT, 
                orderId
            );
        }
        
        // Random failures for demonstration (10% chance)
        if (random.nextDouble() < 0.1) {
            if (random.nextBoolean()) {
                throw new OrderProcessingException(
                    "Random transient error for order: " + orderId, 
                    ErrorType.TRANSIENT, 
                    orderId
                );
            } else {
                throw new OrderProcessingException(
                    "Random permanent error for order: " + orderId, 
                    ErrorType.PERMANENT, 
                    orderId
                );
            }
        }
    }
}
