package com.example.inventoryservice.events;

public record OrderCreatedEvent(
        String orderId,
        String itemId,
        int quantity
) {}

