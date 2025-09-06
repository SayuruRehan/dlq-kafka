package com.example.dlq.config;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class MetricsConfig {

    @Bean
    public Counter orderProcessedCounter(MeterRegistry meterRegistry) {
        return Counter.builder("orders.processed.total")
                .description("Total number of orders processed")
                .register(meterRegistry);
    }

    @Bean
    public Counter orderRetryCounter(MeterRegistry meterRegistry) {
        return Counter.builder("orders.retry.total")
                .description("Total number of orders sent to retry")
                .register(meterRegistry);
    }

    @Bean
    public Counter orderDlqCounter(MeterRegistry meterRegistry) {
        return Counter.builder("orders.dlq.total")
                .description("Total number of orders sent to DLQ")
                .register(meterRegistry);
    }

    @Bean
    public Counter orderRequeuedCounter(MeterRegistry meterRegistry) {
        return Counter.builder("orders.requeued.total")
                .description("Total number of orders requeued from DLQ")
                .register(meterRegistry);
    }

    @Bean
    public Timer orderProcessingTimer(MeterRegistry meterRegistry) {
        return Timer.builder("orders.processing.duration")
                .description("Time taken to process orders")
                .register(meterRegistry);
    }

    @Bean
    public Counter errorCounter(MeterRegistry meterRegistry) {
        return Counter.builder("orders.errors.total")
                .description("Total number of processing errors")
                .tag("type", "processing")
                .register(meterRegistry);
    }
}
