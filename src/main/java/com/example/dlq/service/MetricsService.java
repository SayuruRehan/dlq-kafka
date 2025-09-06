package com.example.dlq.service;

import io.micrometer.core.instrument.Counter;
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

    public MetricsService(Counter orderProcessedCounter,
                         Counter orderRetryCounter,
                         Counter orderDlqCounter,
                         Counter orderRequeuedCounter,
                         Timer orderProcessingTimer,
                         Counter errorCounter) {
        this.orderProcessedCounter = orderProcessedCounter;
        this.orderRetryCounter = orderRetryCounter;
        this.orderDlqCounter = orderDlqCounter;
        this.orderRequeuedCounter = orderRequeuedCounter;
        this.orderProcessingTimer = orderProcessingTimer;
        this.errorCounter = errorCounter;
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
