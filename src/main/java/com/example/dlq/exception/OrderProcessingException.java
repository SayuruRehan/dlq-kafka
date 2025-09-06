package com.example.dlq.exception;

public class OrderProcessingException extends RuntimeException {
    
    private final ErrorType errorType;
    private final String orderId;
    
    public OrderProcessingException(String message, ErrorType errorType, String orderId) {
        super(message);
        this.errorType = errorType;
        this.orderId = orderId;
    }
    
    public OrderProcessingException(String message, ErrorType errorType, String orderId, Throwable cause) {
        super(message, cause);
        this.errorType = errorType;
        this.orderId = orderId;
    }
    
    public ErrorType getErrorType() {
        return errorType;
    }
    
    public String getOrderId() {
        return orderId;
    }
}
