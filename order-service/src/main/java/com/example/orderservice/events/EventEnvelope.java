package com.example.orderservice.events;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.time.Instant;
import java.util.UUID;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record EventEnvelope<T>(
        String eventId,
        String eventType,
        String eventVersion,
        String traceId,
        String correlationId,
        String occurredAt,
        T payload
) {
    public static <T> EventEnvelope<T> of(String eventType, String traceId, T payload) {
        return new EventEnvelope<>(
                "evt-" + UUID.randomUUID(),
                eventType,
                "1.0",
                traceId,
                null,
                Instant.now().toString(),
                payload
        );
    }
}

