package com.example.inventoryservice.api;

import com.example.inventoryservice.domain.InventoryService;
import com.example.inventoryservice.domain.ReservationResult;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Collection;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/inventory")
public class InventoryQueryController {

    private final InventoryService inventoryService;

    public InventoryQueryController(InventoryService inventoryService) {
        this.inventoryService = inventoryService;
    }

    @GetMapping("/results")
    public ResponseEntity<Collection<ReservationResult>> getResults() {
        return ResponseEntity.ok(inventoryService.getResults().values());
    }

    @GetMapping("/results/{orderId}")
    public ResponseEntity<ReservationResult> getResult(@PathVariable String orderId) {
        ReservationResult result = inventoryService.getResultForOrder(orderId);
        if (result == null) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(result);
    }

    @GetMapping("/skipped")
    public ResponseEntity<Map<String, List<String>>> getSkipped() {
        return ResponseEntity.ok(inventoryService.getSkipped());
    }

    @GetMapping("/stock")
    public ResponseEntity<Map<String, Integer>> getStock() {
        return ResponseEntity.ok(inventoryService.getStock());
    }
}



