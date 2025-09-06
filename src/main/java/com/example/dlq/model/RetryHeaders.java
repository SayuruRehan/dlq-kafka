package com.example.dlq.model;

import org.apache.kafka.common.header.Headers;

import java.time.Instant;
import java.util.Optional;

public class RetryHeaders {
    
    public static final String RETRY_COUNT = "x-retry-count";
    public static final String FIRST_SEEN_TS = "x-first-seen-ts";
    public static final String LAST_ERROR = "x-last-error";
    public static final String STACKTRACE = "x-stacktrace";
    public static final String ORIGINAL_TOPIC = "x-original-topic";
    public static final String ORIGINAL_PARTITION = "x-original-partition";
    public static final String ORIGINAL_OFFSET = "x-original-offset";
    public static final String RETRY_UNTIL = "x-retry-until";
    public static final String NEXT_AT = "x-next-at";
    public static final String REQUeUED_BY = "x-requeued-by";
    public static final String MAX_REQUEUES = "x-max-requeues";

    public static int getRetryCount(Headers headers) {
        return getHeaderAsInt(headers, RETRY_COUNT).orElse(0);
    }

    public static Instant getFirstSeenTimestamp(Headers headers) {
        return getHeaderAsString(headers, FIRST_SEEN_TS)
                .map(Instant::parse)
                .orElse(Instant.now());
    }

    public static String getLastError(Headers headers) {
        return getHeaderAsString(headers, LAST_ERROR).orElse("Unknown error");
    }

    public static String getOriginalTopic(Headers headers) {
        return getHeaderAsString(headers, ORIGINAL_TOPIC).orElse("unknown");
    }

    public static int getOriginalPartition(Headers headers) {
        return getHeaderAsInt(headers, ORIGINAL_PARTITION).orElse(-1);
    }

    public static long getOriginalOffset(Headers headers) {
        return getHeaderAsLong(headers, ORIGINAL_OFFSET).orElse(-1L);
    }

    public static Instant getNextAt(Headers headers) {
        return getHeaderAsString(headers, NEXT_AT)
                .map(Instant::parse)
                .orElse(null);
    }

    public static String getRequeuedBy(Headers headers) {
        return getHeaderAsString(headers, REQUeUED_BY).orElse(null);
    }

    public static int getMaxRequeues(Headers headers) {
        return getHeaderAsInt(headers, MAX_REQUEUES).orElse(3);
    }

    private static Optional<String> getHeaderAsString(Headers headers, String key) {
        if (headers == null) return Optional.empty();
        byte[] value = headers.lastHeader(key)?.value();
        return value != null ? Optional.of(new String(value)) : Optional.empty();
    }

    private static Optional<Integer> getHeaderAsInt(Headers headers, String key) {
        return getHeaderAsString(headers, key)
                .map(Integer::parseInt)
                .or(() -> Optional.empty());
    }

    private static Optional<Long> getHeaderAsLong(Headers headers, String key) {
        return getHeaderAsString(headers, key)
                .map(Long::parseLong)
                .or(() -> Optional.empty());
    }
}
