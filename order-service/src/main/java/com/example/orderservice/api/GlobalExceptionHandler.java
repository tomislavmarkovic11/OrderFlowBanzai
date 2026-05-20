package com.example.orderservice.api;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.KafkaException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.time.Instant;
import java.util.List;
import java.util.Map;

@RestControllerAdvice
public class GlobalExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(GlobalExceptionHandler.class);

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<Map<String, Object>> handleValidation(MethodArgumentNotValidException ex) {
        List<Map<String, String>> details = ex.getBindingResult().getFieldErrors().stream()
                .map(fe -> Map.of("field", fe.getField(), "message", fe.getDefaultMessage()))
                .toList();
        log.warn("Validation error: {}", details);
        return ResponseEntity.badRequest().body(Map.of(
                "error", "VALIDATION_ERROR",
                "timestamp", Instant.now().toString(),
                "details", details
        ));
    }

    @ExceptionHandler({KafkaException.class, RuntimeException.class})
    public ResponseEntity<Map<String, Object>> handleKafkaError(RuntimeException ex) {
        log.error("Broker error: {}", ex.getMessage(), ex);
        if (ex.getMessage() != null && ex.getMessage().toLowerCase().contains("kafka")) {
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(Map.of(
                    "error", "BROKER_UNAVAILABLE",
                    "message", "Kafka broker is unavailable. Please try again later.",
                    "timestamp", Instant.now().toString()
            ));
        }
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of(
                "error", "INTERNAL_ERROR",
                "message", "An unexpected error occurred.",
                "timestamp", Instant.now().toString()
        ));
    }
}

