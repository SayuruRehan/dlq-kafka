package com.example.dlq.service;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class MetricsService {

    private static final Logger logger = LoggerFactory.getLogger(MetricsService.class);

    private final Counter orderProcessedCounter;
    private final Counter orderRetryCounter;
    private final Counter orderDlqCounter;
    private final Counter orderRequeuedCounter;
    private final Timer orderProcessingTimer;
    private final Counter errorCounter;

    public MetricsService(MeterRegistry meterRegistry) {
        this.orderProcessedCounter = Counter.builder("orders.processed")
                .description("Number of orders processed successfully")
                .register(meterRegistry);
        this.orderRetryCounter = Counter.builder("orders.retry")
                .description("Number of orders sent to retry")
                .register(meterRegistry);
        this.orderDlqCounter = Counter.builder("orders.dlq")
                .description("Number of orders sent to DLQ")
                .register(meterRegistry);
        this.orderRequeuedCounter = Counter.builder("orders.requeued")
                .description("Number of orders requeued from DLQ")
                .register(meterRegistry);
        this.orderProcessingTimer = Timer.builder("orders.processing.time")
                .description("Time taken to process orders")
                .register(meterRegistry);
        this.errorCounter = Counter.builder("orders.errors")
                .description("Number of processing errors")
                .register(meterRegistry);
    }

    public void recordOrderProcessed() {
        orderProcessedCounter.increment();
        logger.debug("Recorded order processed metric");
    }

    public void recordOrderRetry(String retryTopic) {
        orderRetryCounter.increment();
        logger.info("Recorded order retry metric for topic: {}", retryTopic);
    }

    public void recordOrderDlq() {
        orderDlqCounter.increment();
        logger.warn("Recorded order DLQ metric");
    }

    public void recordOrderRequeued() {
        orderRequeuedCounter.increment();
        logger.info("Recorded order requeued metric");
    }

    public void recordProcessingError(String errorType) {
        errorCounter.increment();
        logger.error("Recorded processing error metric: {}", errorType);
    }

    public Timer.Sample startProcessingTimer() {
        return Timer.start();
    }

    public void recordProcessingTime(Timer.Sample sample) {
        sample.stop(orderProcessingTimer);
    }
}
