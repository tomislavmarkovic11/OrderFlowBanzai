package com.example.orderservice.events;

public record OrderCreatedEvent(
        String orderId,
        String itemId,
        int quantity
) {}

