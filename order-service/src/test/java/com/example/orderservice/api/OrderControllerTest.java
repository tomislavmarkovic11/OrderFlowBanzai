package com.example.orderservice.api;

import com.example.orderservice.domain.OrderService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import java.util.Map;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(OrderController.class)
class OrderControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @MockBean
    private OrderService orderService;

    @Test
    void postOrder_validRequest_returns202() throws Exception {
        Map<String, Object> body = Map.of("orderId", "ord-1", "itemId", "item-1", "quantity", 5);
        mockMvc.perform(post("/orders")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(body)))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.status").value("ACCEPTED"))
                .andExpect(jsonPath("$.traceId").isNotEmpty());

        verify(orderService).placeOrder(anyString(), anyString(), anyInt(), anyString());
    }

    @Test
    void postOrder_missingOrderId_returns400() throws Exception {
        Map<String, Object> body = Map.of("itemId", "item-1", "quantity", 5);
        mockMvc.perform(post("/orders")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(body)))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error").value("VALIDATION_ERROR"));
    }

    @Test
    void postOrder_quantityZero_returns400() throws Exception {
        Map<String, Object> body = Map.of("orderId", "ord-1", "itemId", "item-1", "quantity", 0);
        mockMvc.perform(post("/orders")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(body)))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error").value("VALIDATION_ERROR"));
    }

    @Test
    void postOrder_kafkaDown_returns503() throws Exception {
        doThrow(new RuntimeException("Kafka broker unavailable"))
                .when(orderService).placeOrder(anyString(), anyString(), anyInt(), anyString());

        Map<String, Object> body = Map.of("orderId", "ord-1", "itemId", "item-1", "quantity", 5);
        mockMvc.perform(post("/orders")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(body)))
                .andExpect(status().isServiceUnavailable())
                .andExpect(jsonPath("$.error").value("BROKER_UNAVAILABLE"));
    }
}
