package com.example.inventoryservice.events;

public record InventoryReservedEvent(
        String orderId,
        String itemId,
        int quantityReserved,
        int remainingStock
) {}

