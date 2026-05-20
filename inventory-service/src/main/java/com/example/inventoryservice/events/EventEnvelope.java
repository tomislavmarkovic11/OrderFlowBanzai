package com.example.inventoryservice.events;

import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record EventEnvelope<T>(
        String eventId,
        String eventType,
        String eventVersion,
        String traceId,
        String correlationId,
        String occurredAt,
        T payload
) {}

