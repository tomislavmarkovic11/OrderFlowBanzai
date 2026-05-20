package com.example.inventoryservice.domain;

import java.time.Instant;

public record ReservationResult(
        String orderId,
        String itemId,
        String status,       // RESERVED or REJECTED
        Integer quantityReserved,
        String reason,
        String processedAt
) {
    public static ReservationResult reserved(String orderId, String itemId, int qty) {
        return new ReservationResult(orderId, itemId, "RESERVED", qty, null, Instant.now().toString());
    }

    public static ReservationResult rejected(String orderId, String itemId, String reason) {
        return new ReservationResult(orderId, itemId, "REJECTED", null, reason, Instant.now().toString());
    }
}

