package com.example.inventoryservice.domain;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

@Service
public class InventoryService {

    private static final Logger log = LoggerFactory.getLogger(InventoryService.class);

    // itemId -> stock
    private final Map<String, AtomicInteger> inventory = new ConcurrentHashMap<>();

    // orderId -> ReservationResult
    private final Map<String, ReservationResult> results = new ConcurrentHashMap<>();

    // Idempotency: orderIds already processed
    private final Set<String> processedOrderIds = Collections.newSetFromMap(new ConcurrentHashMap<>());

    // Skipped duplicates: orderId -> list of skipped eventIds
    private final Map<String, List<String>> skippedOrders = new ConcurrentHashMap<>();

    private final Counter reservedCounter;
    private final Counter rejectedCounter;

    public InventoryService(MeterRegistry meterRegistry) {
        this.reservedCounter = Counter.builder("inventory.reservations.success")
                .description("Successful inventory reservations")
                .register(meterRegistry);
        this.rejectedCounter = Counter.builder("inventory.reservations.rejected")
                .description("Rejected inventory reservations")
                .register(meterRegistry);
    }

    /** Called at startup to seed initial stock. */
    public void seedStock(String itemId, int quantity) {
        inventory.put(itemId, new AtomicInteger(quantity));
        log.info("Stock seeded itemId={} quantity={}", itemId, quantity);
    }

    /**
     * Attempt to reserve stock for a given order.
     * Returns true if reserved, false if already processed (idempotent).
     */
    public boolean processOrder(String eventId, String orderId, String itemId, int quantity) {
        // Idempotency check on orderId (not eventId — each publish generates a new eventId)
        if (!processedOrderIds.add(orderId)) {
            log.warn("Duplicate order detected, skipping orderId={} eventId={}", orderId, eventId);
            skippedOrders.computeIfAbsent(orderId, k -> new CopyOnWriteArrayList<>()).add(eventId);
            return false;
        }

        AtomicInteger stock = inventory.computeIfAbsent(itemId, k -> new AtomicInteger(0));

        // CAS-based lock-free reservation
        int current;
        while (true) {
            current = stock.get();
            if (current < quantity) {
                // Not enough stock
                ReservationResult result = ReservationResult.rejected(orderId, itemId, "INSUFFICIENT_STOCK");
                results.put(orderId, result);
                log.warn("Inventory rejected orderId={} itemId={} requested={} available={}",
                        orderId, itemId, quantity, current);
                rejectedCounter.increment();
                return true;
            }
            if (stock.compareAndSet(current, current - quantity)) {
                // Successfully reserved
                ReservationResult result = ReservationResult.reserved(orderId, itemId, quantity);
                results.put(orderId, result);
                log.info("Inventory reserved orderId={} itemId={} qty={} remaining={}",
                        orderId, itemId, quantity, current - quantity);
                reservedCounter.increment();
                return true;
            }
            // CAS failed — another thread modified stock, retry
        }
    }

    public Map<String, ReservationResult> getResults() {
        return Collections.unmodifiableMap(results);
    }

    public Map<String, List<String>> getSkipped() {
        return Collections.unmodifiableMap(skippedOrders);
    }

    public Map<String, Integer> getStock() {
        Map<String, Integer> snapshot = new ConcurrentHashMap<>();
        inventory.forEach((k, v) -> snapshot.put(k, v.get()));
        return Collections.unmodifiableMap(snapshot);
    }

    public ReservationResult getResultForOrder(String orderId) {
        return results.get(orderId);
    }

    public int getStockForItem(String itemId) {
        AtomicInteger stock = inventory.get(itemId);
        return stock != null ? stock.get() : 0;
    }
}

