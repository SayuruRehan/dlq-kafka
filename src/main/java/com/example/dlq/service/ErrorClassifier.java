package com.example.dlq.service;

import com.example.dlq.exception.ErrorType;
import com.example.dlq.exception.OrderProcessingException;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.concurrent.TimeoutException;
import java.util.regex.Pattern;

@Service
public class ErrorClassifier {

    // Patterns for permanent errors
    private static final Pattern VALIDATION_ERROR_PATTERN = Pattern.compile(
        "(?i).*(validation|invalid|malformed|schema|format).*"
    );
    
    private static final Pattern BUSINESS_LOGIC_ERROR_PATTERN = Pattern.compile(
        "(?i).*(insufficient|not found|duplicate|conflict|forbidden).*"
    );

    public ErrorType classifyError(Throwable throwable) {
        if (throwable == null) {
            return ErrorType.TRANSIENT;
        }

        // Check if it's our custom exception
        if (throwable instanceof OrderProcessingException) {
            return ((OrderProcessingException) throwable).getErrorType();
        }

        // Network/IO related - transient
        if (throwable instanceof IOException ||
            throwable instanceof SocketTimeoutException ||
            throwable instanceof TimeoutException ||
            throwable instanceof UnknownHostException) {
            return ErrorType.TRANSIENT;
        }

        // Check message content for permanent errors
        String message = throwable.getMessage();
        if (message != null) {
            if (VALIDATION_ERROR_PATTERN.matcher(message).matches() ||
                BUSINESS_LOGIC_ERROR_PATTERN.matcher(message).matches()) {
                return ErrorType.PERMANENT;
            }
        }

        // Check exception type for permanent errors
        if (throwable instanceof IllegalArgumentException ||
            throwable instanceof IllegalStateException ||
            throwable instanceof UnsupportedOperationException) {
            return ErrorType.PERMANENT;
        }

        // Default to transient for unknown errors
        return ErrorType.TRANSIENT;
    }

    public boolean shouldRetry(Throwable throwable, int currentRetryCount, int maxRetries) {
        ErrorType errorType = classifyError(throwable);
        
        if (errorType == ErrorType.PERMANENT) {
            return false;
        }
        
        return currentRetryCount < maxRetries;
    }
}
