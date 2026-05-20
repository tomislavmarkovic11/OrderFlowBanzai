package com.example.orderservice.api;

import com.example.orderservice.api.dto.CreateOrderRequest;
import com.example.orderservice.api.dto.CreateOrderResponse;
import com.example.orderservice.domain.OrderService;
import jakarta.validation.Valid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

@RestController
@RequestMapping("/orders")
public class OrderController {

    private static final Logger log = LoggerFactory.getLogger(OrderController.class);

    private final OrderService orderService;
    // Simple in-process counter; real metrics via Actuator /actuator/metrics
    private final AtomicLong ordersPublished = new AtomicLong(0);

    public OrderController(OrderService orderService) {
        this.orderService = orderService;
    }

    @PostMapping
    public ResponseEntity<CreateOrderResponse> createOrder(
            @Valid @RequestBody CreateOrderRequest request,
            @RequestHeader(value = "X-Trace-Id", required = false) String headerTraceId) {

        String traceId = headerTraceId != null ? headerTraceId : "trace-" + UUID.randomUUID();
        MDC.put("traceId", traceId);
        try {
            log.info("Received order request orderId={} itemId={} quantity={}",
                    request.orderId(), request.itemId(), request.quantity());

            orderService.placeOrder(request.orderId(), request.itemId(), request.quantity(), traceId);
            ordersPublished.incrementAndGet();

            return ResponseEntity.status(HttpStatus.ACCEPTED)
                    .body(new CreateOrderResponse(traceId, "ACCEPTED", "Order received"));
        } finally {
            MDC.clear();
        }
    }
}

