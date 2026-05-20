package com.example.orderservice.api.dto;

public record CreateOrderResponse(
        String traceId,
        String status,
        String message
) {}

