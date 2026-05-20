package com.example.inventoryservice.integration;

import com.example.inventoryservice.domain.InventoryService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest
@Testcontainers
class InventoryServiceIT {

    @Container
    static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"));

    @DynamicPropertySource
    static void kafkaProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
    }

    @Autowired
    KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    InventoryService inventoryService;

    @Autowired
    ObjectMapper objectMapper;

    @Test
    void consumeOrderCreated_reservesInventory() throws Exception {
        String eventJson = """
                {
                  "eventId": "evt-it-001",
                  "eventType": "OrderCreated",
                  "eventVersion": "1.0",
                  "traceId": "trace-it-001",
                  "occurredAt": "2026-05-18T10:00:00Z",
                  "payload": {
                    "orderId": "it-order-1",
                    "itemId": "item-1",
                    "quantity": 5
                  }
                }
                """;

        kafkaTemplate.send("orders.created", "it-order-1", eventJson);

        await().atMost(15, TimeUnit.SECONDS).untilAsserted(() -> {
            var result = inventoryService.getResultForOrder("it-order-1");
            assertThat(result).isNotNull();
            assertThat(result.status()).isEqualTo("RESERVED");
            assertThat(result.quantityReserved()).isEqualTo(5);
        });

        assertThat(inventoryService.getStockForItem("item-1")).isEqualTo(45);
    }

    @Test
    void consumeOrderCreated_rejectsWhenInsufficientStock() throws Exception {
        String eventJson = """
                {
                  "eventId": "evt-it-002",
                  "eventType": "OrderCreated",
                  "eventVersion": "1.0",
                  "traceId": "trace-it-002",
                  "occurredAt": "2026-05-18T10:00:00Z",
                  "payload": {
                    "orderId": "it-order-2",
                    "itemId": "item-3",
                    "quantity": 100
                  }
                }
                """;

        kafkaTemplate.send("orders.created", "it-order-2", eventJson);

        await().atMost(15, TimeUnit.SECONDS).untilAsserted(() -> {
            var result = inventoryService.getResultForOrder("it-order-2");
            assertThat(result).isNotNull();
            assertThat(result.status()).isEqualTo("REJECTED");
            assertThat(result.reason()).isEqualTo("INSUFFICIENT_STOCK");
        });
    }
}



