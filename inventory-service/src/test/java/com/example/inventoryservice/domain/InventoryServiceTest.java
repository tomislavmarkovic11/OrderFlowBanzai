package com.example.inventoryservice.domain;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class InventoryServiceTest {

    private InventoryService inventoryService;

    @BeforeEach
    void setUp() {
        inventoryService = new InventoryService(new SimpleMeterRegistry());
        inventoryService.seedStock("item-1", 50);
        inventoryService.seedStock("item-2", 20);
        inventoryService.seedStock("item-3", 5);
    }

    @Test
    void reserve_sufficientStock_returnsReserved() {
        inventoryService.processOrder("evt-1", "ord-1", "item-1", 10);
        ReservationResult result = inventoryService.getResultForOrder("ord-1");
        assertThat(result.status()).isEqualTo("RESERVED");
        assertThat(result.quantityReserved()).isEqualTo(10);
        assertThat(inventoryService.getStockForItem("item-1")).isEqualTo(40);
    }

    @Test
    void reserve_insufficientStock_returnsRejected() {
        inventoryService.processOrder("evt-2", "ord-2", "item-3", 10);
        ReservationResult result = inventoryService.getResultForOrder("ord-2");
        assertThat(result.status()).isEqualTo("REJECTED");
        assertThat(result.reason()).isEqualTo("INSUFFICIENT_STOCK");
        assertThat(inventoryService.getStockForItem("item-3")).isEqualTo(5); // unchanged
    }

    @Test
    void reserve_unknownItem_returnsRejected() {
        inventoryService.processOrder("evt-3", "ord-3", "item-unknown", 1);
        ReservationResult result = inventoryService.getResultForOrder("ord-3");
        assertThat(result.status()).isEqualTo("REJECTED");
    }

    @Test
    void idempotency_duplicateEventId_processedOnce() {
        inventoryService.processOrder("evt-4", "ord-4", "item-1", 5);
        inventoryService.processOrder("evt-4", "ord-4", "item-1", 5); // duplicate
        assertThat(inventoryService.getStockForItem("item-1")).isEqualTo(45); // deducted only once
    }

    @Test
    void concurrency_noOverReservation() throws InterruptedException {
        // 20 threads each try to reserve 10 units from item-1 (50 stock)
        // Exactly 5 should succeed, 15 should be rejected
        int threads = 20;
        int qtyPerOrder = 10;
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        CountDownLatch latch = new CountDownLatch(threads);

        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger rejectCount = new AtomicInteger(0);

        for (int i = 0; i < threads; i++) {
            final int idx = i;
            executor.submit(() -> {
                try {
                    inventoryService.processOrder("evt-c-" + idx, "ord-c-" + idx, "item-1", qtyPerOrder);
                    ReservationResult r = inventoryService.getResultForOrder("ord-c-" + idx);
                    if ("RESERVED".equals(r.status())) successCount.incrementAndGet();
                    else rejectCount.incrementAndGet();
                } finally {
                    latch.countDown();
                }
            });
        }
        latch.await();
        executor.shutdown();

        assertThat(successCount.get()).isEqualTo(5);
        assertThat(rejectCount.get()).isEqualTo(15);
        assertThat(inventoryService.getStockForItem("item-1")).isEqualTo(0);
    }
}

