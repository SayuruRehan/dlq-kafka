package com.example.dlq.controller;

import com.example.dlq.service.DlqService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;

@RestController
@RequestMapping("/api/metrics")
public class MetricsController {

    private final DlqService dlqService;

    public MetricsController(DlqService dlqService) {
        this.dlqService = dlqService;
    }

    @GetMapping("/dlq")
    public ResponseEntity<Map<String, Object>> getDlqMetrics() {
        Map<String, Object> metrics = new HashMap<>();
        
        // DLQ message count in last hour
        Instant oneHourAgo = Instant.now().minus(1, ChronoUnit.HOURS);
        Long dlqCountLastHour = dlqService.getMessageCountSince(oneHourAgo);
        metrics.put("dlqMessagesLastHour", dlqCountLastHour);
        
        // DLQ message count in last day
        Instant oneDayAgo = Instant.now().minus(1, ChronoUnit.DAYS);
        Long dlqCountLastDay = dlqService.getMessageCountSince(oneDayAgo);
        metrics.put("dlqMessagesLastDay", dlqCountLastDay);
        
        // Oldest messages (older than 15 minutes)
        Instant fifteenMinutesAgo = Instant.now().minus(15, ChronoUnit.MINUTES);
        var oldestMessages = dlqService.getOldestMessages(fifteenMinutesAgo);
        metrics.put("oldestMessagesCount", oldestMessages.size());
        metrics.put("oldestMessageAge", oldestMessages.isEmpty() ? 0 : 
            ChronoUnit.MINUTES.between(oldestMessages.get(0).getCreatedAt(), Instant.now()));
        
        return ResponseEntity.ok(metrics);
    }

    @GetMapping("/health")
    public ResponseEntity<Map<String, Object>> getHealthMetrics() {
        Map<String, Object> health = new HashMap<>();
        
        // Check DLQ rate (alerts if > 10 messages per minute)
        Instant oneMinuteAgo = Instant.now().minus(1, ChronoUnit.MINUTES);
        Long dlqCountLastMinute = dlqService.getMessageCountSince(oneMinuteAgo);
        boolean dlqRateHigh = dlqCountLastMinute > 10;
        
        health.put("dlqRateHigh", dlqRateHigh);
        health.put("dlqMessagesLastMinute", dlqCountLastMinute);
        health.put("status", dlqRateHigh ? "WARNING" : "HEALTHY");
        
        return ResponseEntity.ok(health);
    }
}
