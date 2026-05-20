package com.example.inventoryservice.kafka;

import com.example.inventoryservice.events.EventEnvelope;
import com.example.inventoryservice.events.InventoryRejectedEvent;
import com.example.inventoryservice.events.InventoryReservedEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.UUID;

@Component
public class InventoryEventPublisher {

    private static final Logger log = LoggerFactory.getLogger(InventoryEventPublisher.class);

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final String reservedTopic;
    private final String rejectedTopic;

    public InventoryEventPublisher(
            KafkaTemplate<String, String> kafkaTemplate,
            ObjectMapper objectMapper,
            @Value("${app.kafka.topics.inventory-reserved}") String reservedTopic,
            @Value("${app.kafka.topics.inventory-rejected}") String rejectedTopic) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.reservedTopic = reservedTopic;
        this.rejectedTopic = rejectedTopic;
    }

    public void publishReserved(String correlationEventId, String traceId, InventoryReservedEvent payload) {
        publish(reservedTopic, payload.orderId(), correlationEventId, traceId, "InventoryReserved", payload);
    }

    public void publishRejected(String correlationEventId, String traceId, InventoryRejectedEvent payload) {
        publish(rejectedTopic, payload.orderId(), correlationEventId, traceId, "InventoryRejected", payload);
    }

    private void publish(String topic, String key, String correlationId, String traceId, String eventType, Object payload) {
        EventEnvelope<Object> envelope = new EventEnvelope<>(
                "evt-" + UUID.randomUUID(),
                eventType,
                "1.0",
                traceId,
                correlationId,
                Instant.now().toString(),
                payload
        );
        try {
            String json = objectMapper.writeValueAsString(envelope);
            kafkaTemplate.send(topic, key, json);
            log.info("Published {} for orderId={}", eventType, key);
        } catch (JsonProcessingException e) {
            log.error("Failed to serialize {} event", eventType, e);
        }
    }
}

