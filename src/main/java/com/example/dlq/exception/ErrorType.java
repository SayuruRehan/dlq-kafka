package com.example.dlq.exception;

public enum ErrorType {
    TRANSIENT,    // Should retry (timeouts, 5xx errors, network issues)
    PERMANENT     // Should go to DLQ immediately (validation, schema, business logic)
}
