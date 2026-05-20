package com.example.orderservice.api.dto;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;

public record CreateOrderRequest(
        @NotBlank(message = "orderId must not be blank")
        @Size(max = 64, message = "orderId must not exceed 64 characters")
        @Pattern(regexp = "^[a-zA-Z0-9\\-]+$", message = "orderId must be alphanumeric with hyphens only")
        String orderId,

        @NotBlank(message = "itemId must not be blank")
        @Size(max = 64, message = "itemId must not exceed 64 characters")
        String itemId,

        @Min(value = 1, message = "quantity must be at least 1")
        @Max(value = 10000, message = "quantity must not exceed 10000")
        int quantity
) {}

