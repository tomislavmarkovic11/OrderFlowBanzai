package com.example.orderservice.kafka;

import com.example.orderservice.events.EventEnvelope;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Component
public class OrderEventPublisher {

    private static final Logger log = LoggerFactory.getLogger(OrderEventPublisher.class);

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final String topic;

    public OrderEventPublisher(
            KafkaTemplate<String, String> kafkaTemplate,
            ObjectMapper objectMapper,
            @Value("${app.kafka.topics.orders-created}") String topic) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.topic = topic;
    }

    public void publish(String key, EventEnvelope<?> event) {
        try {
            String json = objectMapper.writeValueAsString(event);
            // Block with a 5-second timeout so Kafka unavailability surfaces as a 503
            kafkaTemplate.send(topic, key, json)
                    .get(5, TimeUnit.SECONDS);
            log.debug("Event published eventId={} topic={}", event.eventId(), topic);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize event", e);
        } catch (TimeoutException e) {
            log.error("Kafka publish timed out eventId={}", event.eventId());
            throw new KafkaException("Kafka unavailable: publish timed out", e);
        } catch (ExecutionException e) {
            log.error("Kafka publish failed eventId={} error={}", event.eventId(), e.getCause().getMessage());
            throw new KafkaException("Kafka publish failed", e.getCause());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new KafkaException("Kafka publish interrupted", e);
        }
    }
}

