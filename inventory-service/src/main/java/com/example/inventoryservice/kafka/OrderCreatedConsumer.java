package com.example.inventoryservice.kafka;

import com.example.inventoryservice.domain.InventoryService;
import com.example.inventoryservice.domain.ReservationResult;
import com.example.inventoryservice.events.EventEnvelope;
import com.example.inventoryservice.events.InventoryRejectedEvent;
import com.example.inventoryservice.events.InventoryReservedEvent;
import com.example.inventoryservice.events.OrderCreatedEvent;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class OrderCreatedConsumer {

    private static final Logger log = LoggerFactory.getLogger(OrderCreatedConsumer.class);

    private final InventoryService inventoryService;
    private final InventoryEventPublisher eventPublisher;
    private final ObjectMapper objectMapper;

    public OrderCreatedConsumer(InventoryService inventoryService,
                                InventoryEventPublisher eventPublisher,
                                ObjectMapper objectMapper) {
        this.inventoryService = inventoryService;
        this.eventPublisher = eventPublisher;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(
            topics = "${app.kafka.topics.orders-created}",
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void onOrderCreated(ConsumerRecord<String, String> record) {
        String raw = record.value();
        EventEnvelope<OrderCreatedEvent> envelope;
        try {
            envelope = objectMapper.readValue(raw, new TypeReference<>() {});
        } catch (Exception e) {
            log.error("Failed to deserialize OrderCreated message, routing error upstream. payload={}", raw, e);
            throw new RuntimeException("Deserialization failure", e);
        }

        String traceId = envelope.traceId() != null ? envelope.traceId() : "unknown";
        MDC.put("traceId", traceId);
        MDC.put("eventId", envelope.eventId());
        try {
            OrderCreatedEvent order = envelope.payload();
            log.info("Received OrderCreated eventId={} orderId={} itemId={} qty={}",
                    envelope.eventId(), order.orderId(), order.itemId(), order.quantity());

            boolean processed = inventoryService.processOrder(
                    envelope.eventId(), order.orderId(), order.itemId(), order.quantity());

            if (!processed) {
                // Duplicate — already handled
                return;
            }

            ReservationResult result = inventoryService.getResultForOrder(order.orderId());
            if ("RESERVED".equals(result.status())) {
                eventPublisher.publishReserved(
                        envelope.eventId(),
                        traceId,
                        new InventoryReservedEvent(
                                order.orderId(), order.itemId(), order.quantity(),
                                inventoryService.getStockForItem(order.itemId())
                        )
                );
            } else {
                eventPublisher.publishRejected(
                        envelope.eventId(),
                        traceId,
                        new InventoryRejectedEvent(
                                order.orderId(), order.itemId(), order.quantity(),
                                inventoryService.getStockForItem(order.itemId()),
                                result.reason()
                        )
                );
            }
        } finally {
            MDC.clear();
        }
    }
}

