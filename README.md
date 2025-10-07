# DLQ Kafka Consumer

A comprehensive Kafka consumer implementation with retry logic, exponential backoff, and Dead Letter Queue (DLQ) functionality. This project demonstrates best practices for handling message processing failures in Kafka.

## Features

- **Main Consumer**: Processes orders from the main topic with failure handling
- **Retry Logic**: Exponential backoff with separate retry topics (5s, 30s, 5m)
- **Dead Letter Queue**: Stores failed messages with rich metadata
- **Error Classification**: Distinguishes between transient and permanent errors
- **Transactional Processing**: Ensures exactly-once semantics for retry/DLQ operations
- **Web UI**: Simple interface for inspecting and requeuing DLQ messages
- **Monitoring**: Metrics and health checks for retry rates and DLQ status
- **Testing**: Integration tests with embedded Kafka

## Architecture

```
Producer → orders.v1 → Worker Consumer (process & ACK)
                           ↓ (on failure)
                    Retry Topics (5s → 30s → 5m)
                           ↓ (max attempts reached)
                    Dead Letter Queue (DLQ)
                           ↓ (manual intervention)
                    Web UI for inspection/requeue
```

## Topics

- `orders.v1` - Main input topic
- `orders.v1.retry.5s` - First retry (5 second delay)
- `orders.v1.retry.30s` - Second retry (30 second delay)  
- `orders.v1.retry.5m` - Third retry (5 minute delay)
- `orders.v1.dlq` - Dead letter queue for failed messages

## Setup

### Prerequisites

- Java 17+
- Maven 3.6+
- Kafka (local or Docker)
- H2 Database (embedded)

### Running with Docker

1. Start Kafka using Docker Compose:
```bash
docker-compose up -d
```

2. Build and run the application:
```bash
mvn clean package
java -jar target/dlq-kafka-1.0.0.jar
```

### Running Locally

1. Start Kafka locally on `localhost:9092`

2. Run the application:
```bash
mvn spring-boot:run
```

The application will automatically create the required Kafka topics.

## Usage

### API Endpoints

#### Test Order Creation
```bash
# Create a normal order
curl -X POST "http://localhost:8080/api/test/orders?type=normal"

# Create an order that will fail with permanent error
curl -X POST "http://localhost:8080/api/test/orders?type=invalid"

# Create a batch of test orders
curl -X POST "http://localhost:8080/api/test/orders/batch?count=10"
```

#### DLQ Management
```bash
# Get DLQ messages
curl "http://localhost:8080/api/dlq?page=0&size=20"

# Get specific DLQ message
curl "http://localhost:8080/api/dlq/1"

# Requeue a message from DLQ
curl -X POST "http://localhost:8080/api/dlq/1/requeue?requeuedBy=admin"

# Delete a DLQ message
curl -X DELETE "http://localhost:8080/api/dlq/1"
```

#### Metrics
```bash
# Get DLQ metrics
curl "http://localhost:8080/api/metrics/dlq"

# Get health status
curl "http://localhost:8080/api/metrics/health"
```

### Web UI

Access the DLQ dashboard at: http://localhost:8080/dlq

Features:
- View all DLQ messages with pagination
- Inspect message details, errors, and stack traces
- Requeue messages (with requeue limit protection)
- Delete messages
- Filter by requeueable messages

## Configuration

### Application Properties

```yaml
kafka:
  topics:
    main: orders.v1
    retry-5s: orders.v1.retry.5s
    retry-30s: orders.v1.retry.30s
    retry-5m: orders.v1.retry.5m
    dlq: orders.v1.dlq

retry:
  max-attempts: 3
  delays:
    - 5s
    - 30s
    - 5m
```

### Error Types

The system classifies errors into two types:

**Transient Errors** (retryable):
- Network timeouts
- IO exceptions
- Service unavailable (5xx errors)

**Permanent Errors** (go to DLQ immediately):
- Validation errors
- Schema mismatches
- Business logic violations
- Malformed data

## Message Headers

The system adds rich metadata to retry and DLQ messages:

- `x-retry-count`: Current retry attempt (0, 1, 2, ...)
- `x-first-seen-ts`: Original receive timestamp
- `x-last-error`: Exception message (truncated)
- `x-stacktrace`: Stack trace (truncated)
- `x-original-topic`: Original topic name
- `x-original-partition`: Original partition
- `x-original-offset`: Original offset
- `x-next-at`: Next retry time
- `x-requeued-by`: User who requeued (for DLQ)
- `x-max-requeues`: Maximum requeue limit

## Monitoring

### Metrics

The application exposes Prometheus-compatible metrics:

- `orders.processed.total` - Successfully processed orders
- `orders.retry.total` - Orders sent to retry topics
- `orders.dlq.total` - Orders sent to DLQ
- `orders.requeued.total` - Orders requeued from DLQ
- `orders.processing.duration` - Processing time histogram
- `orders.errors.total` - Processing errors by type

### Health Checks

- DLQ rate monitoring (alerts if > 10 messages/minute)
- Oldest message age tracking
- Requeue success rate

Access metrics at: http://localhost:8080/actuator/prometheus

## Testing

### Unit Tests
```bash
mvn test
```

### Integration Tests
```bash
mvn test -Dtest=*IntegrationTest
```

The integration tests use embedded Kafka and verify:
- Normal order processing
- Retry logic for transient errors
- DLQ handling for permanent errors
- Message requeuing functionality

## Error Scenarios

### Test Order Types

The test controller supports different failure scenarios:

- `normal` - Processes successfully
- `invalid` - Permanent error (invalid format)
- `timeout` - Transient error (timeout)
- `network` - Transient error (network issue)
- `validation` - Permanent error (validation failure)

### Example Test Flow

1. Create test orders with different failure types
2. Monitor the retry topics for exponential backoff
3. Check DLQ for messages that exceeded max retries
4. Use the web UI to inspect and requeue DLQ messages
5. Monitor metrics for processing rates and error counts

## Best Practices Demonstrated

1. **Exactly-Once Semantics**: Uses Kafka transactions for atomic retry/DLQ operations
2. **Error Classification**: Distinguishes retryable vs permanent errors
3. **Exponential Backoff**: Implements progressive delays for retries
4. **Rich Metadata**: Includes comprehensive headers for debugging
5. **Monitoring**: Comprehensive metrics and health checks
6. **Manual Recovery**: Web UI for DLQ message inspection and requeuing
7. **Testing**: Integration tests with embedded Kafka

## Troubleshooting

### Common Issues

1. **Messages stuck in retry topics**: Check if retry consumers are running
2. **High DLQ rate**: Review error classification logic
3. **Requeue failures**: Verify requeue limits and message format
4. **Transaction failures**: Check Kafka transaction configuration

### Logs

Enable debug logging:
```yaml
logging:
  level:
    com.example.dlq: DEBUG
    org.springframework.kafka: INFO
```

