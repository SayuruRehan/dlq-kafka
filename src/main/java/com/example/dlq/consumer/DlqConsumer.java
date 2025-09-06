package com.example.dlq.consumer;

import com.example.dlq.entity.DlqMessage;
import com.example.dlq.model.RetryHeaders;
import com.example.dlq.repository.DlqMessageRepository;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
public class DlqConsumer {

    private static final Logger logger = LoggerFactory.getLogger(DlqConsumer.class);

    private final DlqMessageRepository dlqMessageRepository;

    public DlqConsumer(DlqMessageRepository dlqMessageRepository) {
        this.dlqMessageRepository = dlqMessageRepository;
    }

    @KafkaListener(topics = "${kafka.topics.dlq}", groupId = "dlq-consumer-group")
    @Transactional
    public void handleDlqMessage(ConsumerRecord<String, String> record,
                                Acknowledgment acknowledgment,
                                @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                                @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                                @Header(KafkaHeaders.OFFSET) long offset) {
        
        String key = record.key();
        String value = record.value();
        
        logger.warn("Received DLQ message: key={}, topic={}, partition={}, offset={}", 
                   key, topic, partition, offset);

        try {
            // Extract metadata from headers
            String originalTopic = RetryHeaders.getOriginalTopic(record.headers());
            int originalPartition = RetryHeaders.getOriginalPartition(record.headers());
            long originalOffset = RetryHeaders.getOriginalOffset(record.headers());
            int retryCount = RetryHeaders.getRetryCount(record.headers());
            Instant firstSeenTs = RetryHeaders.getFirstSeenTimestamp(record.headers());
            String lastError = RetryHeaders.getLastError(record.headers());
            String stacktrace = new String(record.headers().lastHeader(RetryHeaders.STACKTRACE)?.value() ?? new byte[0]);

            // Create and save DLQ message entity
            DlqMessage dlqMessage = new DlqMessage(
                key, value, originalTopic, originalPartition, originalOffset,
                retryCount, firstSeenTs, lastError, stacktrace
            );

            dlqMessageRepository.save(dlqMessage);
            
            logger.info("Saved DLQ message to database: id={}, key={}", dlqMessage.getId(), key);
            
            // Acknowledge the message
            acknowledgment.acknowledge();
            
        } catch (Exception e) {
            logger.error("Error processing DLQ message: key={}, error={}", key, e.getMessage(), e);
            // Don't acknowledge - let it retry
        }
    }
}
