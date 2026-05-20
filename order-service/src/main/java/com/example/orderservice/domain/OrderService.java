package com.example.orderservice.domain;

import com.example.orderservice.events.EventEnvelope;
import com.example.orderservice.events.OrderCreatedEvent;
import com.example.orderservice.kafka.OrderEventPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.stereotype.Service;

@Service
public class OrderService {

    private static final Logger log = LoggerFactory.getLogger(OrderService.class);

    private final OrderEventPublisher publisher;

    public OrderService(OrderEventPublisher publisher) {
        this.publisher = publisher;
    }

    public void placeOrder(String orderId, String itemId, int quantity, String traceId) {
        MDC.put("traceId", traceId);
        MDC.put("orderId", orderId);
        try {
            log.info("Placing order orderId={} itemId={} quantity={}", orderId, itemId, quantity);
            EventEnvelope<OrderCreatedEvent> event = EventEnvelope.of(
                    "OrderCreated",
                    traceId,
                    new OrderCreatedEvent(orderId, itemId, quantity)
            );
            publisher.publish(orderId, event);
            log.info("Order event published successfully eventId={}", event.eventId());
        } finally {
            MDC.remove("traceId");
            MDC.remove("orderId");
        }
    }
}

