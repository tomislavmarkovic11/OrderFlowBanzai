package com.example.inventoryservice.events;

public record InventoryRejectedEvent(
        String orderId,
        String itemId,
        int requestedQuantity,
        int availableStock,
        String reason
) {}

