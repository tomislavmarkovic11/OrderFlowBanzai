package com.example.inventoryservice.domain;

import jakarta.annotation.PostConstruct;
import org.springframework.stereotype.Component;

@Component
public class StockInitializer {

    private final InventoryService inventoryService;

    public StockInitializer(InventoryService inventoryService) {
        this.inventoryService = inventoryService;
    }

    @PostConstruct
    public void seed() {
        inventoryService.seedStock("item-1", 50);
        inventoryService.seedStock("item-2", 20);
        inventoryService.seedStock("item-3", 5);
    }
}

