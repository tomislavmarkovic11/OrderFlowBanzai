# OrderFlowBanzai

Event-driven microservices system built with Spring Boot 3.5, Apache Kafka (KRaft), and Docker Compose.

> **Project:** OrderFlowBanzai
---

## Architecture

```
+---------------------------------------------------------------------+
|                        Docker Compose Network                        |
|                                                                      |
|  +--------------+    POST /orders    +-------------------------+    |
|  |              | <------------------+                         |    |
|  |  Order API   |                    |     HTTP Client         |    |
|  |  Service     |                    |  (curl / Postman / test)|    |
|  |  :8080       | -----------------> +-------------------------+    |
|  +------+-------+  202 Accepted                                     |
|         |                                                            |
|         |  Publish: orders.created                                   |
|         v                                                            |
|  +--------------+                                                    |
|  |              |                                                    |
|  |    Kafka     |  Topic: orders.created  (3 partitions)            |
|  |  Broker      |  Topic: inventory.reserved                        |
|  |  :9092       |  Topic: inventory.rejected                        |
|  |              |  Topic: orders.created.DLT                        |
|  +------+-------+                                                    |
|         |                                                            |
|         |  Consume: orders.created                                   |
|         v                                                            |
|  +------------------------------------------------------------------+|
|  |              Inventory Processing Service  :8081                 ||
|  |                                                                   ||
|  |  +-----------------+    +------------------------------+        ||
|  |  | Kafka Consumer  |--->|  InventoryService (in-memory)|        ||
|  |  | (@KafkaListener)|    |  ConcurrentHashMap<itemId,   |        ||
|  |  +-----------------+    |  AtomicInteger qty>          |        ||
|  |                         +---------------+--------------+        ||
|  |                                         |                        ||
|  |                         +---------------v--------------+        ||
|  |                         |  Result Store (in-memory)    |        ||
|  |                         |  GET /inventory/results      |        ||
|  |                         |  GET /inventory/results/{id} |        ||
|  |                         |  GET /inventory/skipped      |        ||
|  |                         +------------------------------+        ||
|  +------------------------------------------------------------------+|
|                                                                      |
+---------------------------------------------------------------------+
```

### Request Flow (Happy Path)

```
1.  Client  →  POST /orders {orderId, itemId, quantity}
2.  Order API validates the payload (Bean Validation)
3.  Order API generates a traceId, builds an OrderCreated event envelope
4.  Order API publishes to orders.created, blocking up to 5 s for broker ack
5.  Order API returns 202 Accepted  ← only after confirmed broker receipt
6.  Kafka delivers the message to inventory-service-group
7.  Inventory Service checks idempotency: has this orderId been seen before?
8.  If new → CAS-based stock deduction (lock-free, thread-safe)
9.  Publishes InventoryReserved or InventoryRejected result event
10. Consumer commits offset → Kafka advances
```

---

## Services

| Service | Port | Role |
|---|---|---|
| `order-service` | `8080` | REST API — accepts orders, publishes to Kafka |
| `inventory-service` | `8081` | Kafka consumer — manages stock, stores results |
| `kafka` | `9092` | Apache Kafka 3.8.0 in KRaft mode (no Zookeeper) |

---

## Quick Start

**Prerequisites**: Docker Desktop (or Docker Engine + Compose v2)

```bash
git clone https://github.com/tomislavmarkovic11/OrderFlowBanzai.git
cd OrderFlowBanzai
docker compose up --build
```

Wait for all three containers to pass their health checks (~60 s on first build). Then:

```bash
# Place an order
curl -X POST http://localhost:8080/orders \
  -H "Content-Type: application/json" \
  -d '{"orderId":"ord-1","itemId":"item-1","quantity":5}'

# Check reservation result
curl http://localhost:8081/inventory/results/ord-1

# View all results
curl http://localhost:8081/inventory/results

# View current stock
curl http://localhost:8081/inventory/stock

# View idempotency report (orders skipped as duplicates)
curl http://localhost:8081/inventory/skipped
```

---

## API Reference

### Order Service — `POST /orders`

Validates the request and publishes an `OrderCreated` event to Kafka. Returns only after the broker has acknowledged receipt.

**Request**

```http
POST /orders
Content-Type: application/json
X-Trace-Id: my-trace-123   (optional — generated if absent)
```

```json
{
  "orderId": "ord-1",
  "itemId":  "item-1",
  "quantity": 5
}
```

| Field | Constraint |
|---|---|
| `orderId` | Required · non-blank · max 64 chars · alphanumeric + hyphens |
| `itemId` | Required · non-blank · max 64 chars |
| `quantity` | Required · integer · min 1 · max 10 000 |

**Responses**

| Status | When | Body |
|---|---|---|
| `202 Accepted` | Event confirmed by Kafka broker | `{"traceId":"…","status":"ACCEPTED","message":"Order received"}` |
| `400 Bad Request` | Validation failure | `{"error":"VALIDATION_ERROR","details":[{"field":"…","message":"…"}],"timestamp":"…"}` |
| `503 Service Unavailable` | Kafka down or publish timeout > 5 s | `{"error":"BROKER_UNAVAILABLE","message":"…"}` |
| `500 Internal Server Error` | Unexpected error | `{"error":"INTERNAL_ERROR","message":"…"}` |

> **Why 202?** Processing is asynchronous. 202 signals "accepted for processing" — the inventory outcome is not yet known at publish time.

---

### Inventory Service — Query Endpoints

| Method | Path | Description |
|---|---|---|
| `GET` | `/inventory/results` | All reservation results |
| `GET` | `/inventory/results/{orderId}` | Result for a specific order · `404` if not found |
| `GET` | `/inventory/stock` | Current in-memory stock for all items |
| `GET` | `/inventory/skipped` | Orders skipped by the idempotency check, with every duplicate `eventId` |

**Example — `GET /inventory/results/ord-1`**

```json
{
  "orderId": "ord-1",
  "itemId": "item-1",
  "status": "RESERVED",
  "quantityReserved": 5,
  "reason": null,
  "processedAt": "2026-05-20T10:30:01.123Z"
}
```

**Example — `GET /inventory/skipped`**

```json
{
  "ord-1": ["evt-uuid-2", "evt-uuid-3"]
}
```

---

## Pre-seeded Inventory

| Item | Initial Stock |
|---|---|
| `item-1` | 50 units |
| `item-2` | 20 units |
| `item-3` | 5 units |
| Any unknown item | 0 units (auto-rejected) |

---

## Kafka Topics

| Topic | Partitions | Retention | Purpose |
|---|---|---|---|
| `orders.created` | 3 | 7 days | `OrderCreated` events from the API |
| `inventory.reserved` | 1 | 7 days | Successful stock reservations |
| `inventory.rejected` | 1 | 7 days | Failed reservations (insufficient stock) |
| `orders.created-dlt` | 1 | 30 days | Poison / unprocessable messages (Spring DLT suffix) |

Message key = `orderId` → same order always routes to the same partition → ordering per order guaranteed.

---

## Event Schema

All events share a common envelope:

```json
{
  "eventId":       "evt-550e8400-…",
  "eventType":     "OrderCreated",
  "eventVersion":  "1.0",
  "traceId":       "trace-7f3d8b2a-…",
  "correlationId": null,
  "occurredAt":    "2026-05-20T10:30:00.000Z",
  "payload": { … }
}
```

| Field | Purpose |
|---|---|
| `eventId` | Unique per publish (new UUID each time) |
| `eventType` | Discriminator for consumers |
| `eventVersion` | Schema evolution — non-breaking: add optional fields; breaking: bump major version |
| `traceId` | End-to-end trace identifier propagated through the system |
| `correlationId` | Points to the triggering `eventId` (set in result events) |
| `occurredAt` | Event creation time in ISO-8601 UTC |

---

## Testing

### Unit Tests (no infrastructure required)

```bash
# Inventory domain logic: reservation algorithm, CAS concurrency, idempotency
cd inventory-service && mvn test -Dtest=InventoryServiceTest

# Order API: MockMvc validation, 202 response, 503 on Kafka failure
cd order-service && mvn test -Dtest=OrderControllerTest
```

### Integration Tests (requires Docker — Testcontainers spins up a real Kafka broker)

```bash
cd order-service && mvn verify
cd inventory-service && mvn verify
```

### Run All Tests

```bash
# From the repo root — runs unit + integration tests for both services
(cd order-service && mvn verify) && (cd inventory-service && mvn verify)
```

### Key Test Scenarios Covered

| Test | What It Verifies |
|---|---|
| `InventoryServiceTest` | Stock reservation, rejection, CAS concurrency (20 threads), orderId idempotency |
| `OrderControllerTest` | `POST /orders` validation errors → 400, happy path → 202, Kafka failure → 503 |
| `InventoryServiceIT` (Testcontainers) | Full flow: consume `OrderCreated` → stock updated → result stored |
| `OrderServiceIT` (Testcontainers) | `POST /orders` → message confirmed on `orders.created` Kafka topic |

---

## Technical Decisions

### Honest 202 / 503 Semantics

The Kafka publish blocks with a **5-second timeout**:

```java
kafkaTemplate.send(topic, key, json).get(5, TimeUnit.SECONDS);
```

| Scenario | Response |
|---|---|
| Kafka up, broker acks | ✅ `202 Accepted` |
| Kafka down | ✅ `503 BROKER_UNAVAILABLE` |
| Kafka slow > 5 s | ✅ `503 BROKER_UNAVAILABLE` |

### Idempotency (orderId-keyed)

Every publish generates a **new** `eventId`. Using it as the idempotency key would let every retry pass the check. The correct key is **`orderId`** — the stable business identifier:

```java
if (!processedOrderIds.add(orderId)) {
    skippedOrders.computeIfAbsent(orderId, k -> new CopyOnWriteArrayList<>()).add(eventId);
    return false;   // duplicate — skip silently
}
```

Skipped orders and their discarded `eventId`s are queryable via `GET /inventory/skipped`.

### Thread-safe Stock Deduction (CAS Loop)

```java
while (true) {
    int current = stock.get();
    if (current < quantity) { /* reject */ break; }
    if (stock.compareAndSet(current, current - quantity)) { /* reserved */ break; }
    // CAS lost race — retry
}
```

Lock-free and correct under any concurrency level. Exactly one thread wins for the last available units; all others are rejected.

### Retry + Dead Letter Queue

`DefaultErrorHandler` with exponential backoff:

| Attempt | Delay before retry |
|---|---|
| 1st retry | 1 s |
| 2nd retry | 2 s |
| 3rd retry | 4 s |
| Exhausted | → `orders.created-dlt` |

`JsonParseException` (malformed JSON) is non-retryable — goes straight to DLQ without wasting retry budget.

### At-Least-Once Delivery

`AckMode.RECORD` — offset committed only after the record is fully processed or routed to DLQ. A crash before the commit causes Kafka to redeliver; idempotency handles the duplicate transparently.

---

## Observability

### Structured JSON Logs

Both services emit JSON via `logstash-logback-encoder`. Every log line includes:

```json
{
  "timestamp": "2026-05-20T10:30:01.123Z",
  "level":     "INFO",
  "service":   "orderflowbanzai-inventory-service",
  "traceId":   "trace-7f3d8b2a-…",
  "message":   "Inventory reserved orderId=ord-1 itemId=item-1 qty=5 remaining=45"
}
```

`traceId` is set in MDC at the REST controller (order-service) and re-extracted from the event envelope at the Kafka consumer (inventory-service). MDC is always cleared in a `finally` block.

### Actuator Health & Metrics

| Endpoint | Service | What It Shows |
|---|---|---|
| `GET /actuator/health` | Both | App status — `diskSpace`, `ping`, `ssl` components |
| `GET /actuator/metrics` | Both | JVM, HTTP, Kafka template/listener metrics |
| `GET /actuator/metrics/inventory.reservations.success` | Inventory | Custom counter — successful reservations |
| `GET /actuator/metrics/inventory.reservations.rejected` | Inventory | Custom counter — rejected reservations |

### Logs

```bash
docker compose logs -f order-service
docker compose logs -f inventory-service
```

---

## Health Checks & Observability Curls

### Liveness / Readiness

```bash
# order-service — overall health (UP / DOWN)
curl -s http://localhost:8080/actuator/health | jq .

# inventory-service — overall health
curl -s http://localhost:8081/actuator/health | jq .
```

**Example response (healthy)**:
```json
{
  "status": "UP",
  "components": {
    "diskSpace": { "status": "UP" },
    "ping":      { "status": "UP" },
    "ssl":       { "status": "UP" }
  }
}
```

### Kafka Connectivity

There is no dedicated Kafka health component — use the `spring.kafka.template` and `spring.kafka.listener` metrics to verify Kafka is being used successfully (see Metrics section below).

```bash
# All health components — both services return: diskSpace, ping, ssl
curl -s http://localhost:8080/actuator/health | jq '.components | keys'
curl -s http://localhost:8081/actuator/health | jq '.components | keys'
```

### Metrics

```bash
# All available metric names — order-service
curl -s http://localhost:8080/actuator/metrics | jq '.names[]'

# All available metric names — inventory-service
curl -s http://localhost:8081/actuator/metrics | jq '.names[]'

# JVM memory usage
curl -s http://localhost:8080/actuator/metrics/jvm.memory.used | jq .
curl -s http://localhost:8081/actuator/metrics/jvm.memory.used | jq .

# HTTP request count and latency — order-service
curl -s http://localhost:8080/actuator/metrics/http.server.requests | jq .

# Kafka producer template metrics — order-service (publish count, latency)
curl -s http://localhost:8080/actuator/metrics/spring.kafka.template | jq .

# Kafka listener metrics — inventory-service (consume count, success/failure)
curl -s http://localhost:8081/actuator/metrics/spring.kafka.listener | jq .

# Successful inventory reservations (custom Micrometer counter)
curl -s http://localhost:8081/actuator/metrics/inventory.reservations.success | jq .

# Rejected inventory reservations (custom Micrometer counter)
curl -s http://localhost:8081/actuator/metrics/inventory.reservations.rejected | jq .
```

### Application Info

```bash
# Returns {} — no build info configured (acceptable for demo scope)
curl -s http://localhost:8080/actuator/info | jq .
curl -s http://localhost:8081/actuator/info | jq .
```

### All Exposed Actuator Endpoints

```bash
# List all available actuator links — order-service
curl -s http://localhost:8080/actuator | jq '."_links" | keys'

# List all available actuator links — inventory-service
curl -s http://localhost:8081/actuator | jq '."_links" | keys'
```

Both services expose: `health`, `info`, `metrics`, `prometheus`.

### Quick System Status (one-liner)

```bash
# Hit all key health and query endpoints at once
echo "=== order-service health ===" && curl -s http://localhost:8080/actuator/health | jq .status && \
echo "=== inventory-service health ===" && curl -s http://localhost:8081/actuator/health | jq .status && \
echo "=== stock ===" && curl -s http://localhost:8081/inventory/stock | jq . && \
echo "=== results ===" && curl -s http://localhost:8081/inventory/results | jq . && \
echo "=== skipped ===" && curl -s http://localhost:8081/inventory/skipped | jq .
```

---

## Scenario Walkthrough

### Duplicate Order (Idempotency)

```bash
# Send ord-1 four times
for i in 1 2 3 4; do
  curl -s -X POST http://localhost:8080/orders \
    -H "Content-Type: application/json" \
    -d '{"orderId":"ord-1","itemId":"item-1","quantity":5}'
done

# Stock should decrease by 5 (once only)
curl http://localhost:8081/inventory/stock

# Confirm 3 eventIds were skipped
curl http://localhost:8081/inventory/skipped
```

### Insufficient Stock

```bash
# item-3 has only 5 units
curl -X POST http://localhost:8080/orders \
  -H "Content-Type: application/json" \
  -d '{"orderId":"big-order","itemId":"item-3","quantity":100}'

curl http://localhost:8081/inventory/results/big-order
# → {"status":"REJECTED","reason":"INSUFFICIENT_STOCK",…}
```

### Kafka Down Simulation

```bash
docker compose stop kafka

curl -X POST http://localhost:8080/orders \
  -H "Content-Type: application/json" \
  -d '{"orderId":"test","itemId":"item-1","quantity":1}'
# → HTTP 503 {"error":"BROKER_UNAVAILABLE",…}

docker compose start kafka
```

### Poison Message (DLT — Dead Letter Topic)

```bash
# Send invalid JSON directly to the topic — no extra tools needed
echo "THIS IS NOT VALID JSON" | docker exec -i kafka \
  /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic orders.created

# After ~1 s the error appears in logs (JsonParseException → retry → DLT)
docker compose logs inventory-service | grep "Failed to deserialize\|Retry attempt\|dlt"

# Read all messages sitting on the dead letter topic (exits after reading all existing messages)
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic orders.created-dlt \
  --from-beginning \
  --max-messages 10 \
  --property print.timestamp=true \
  --property print.headers=true

# List all topics (confirms orders.created-dlt exists)
docker exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --list
```

---


## Technical Decisions

### Why 202 and not 201 or 200?

`202 Accepted` explicitly models the async contract: the order has been handed off for processing, but the inventory outcome is not yet known. `200`/`201` would imply synchronous completion.

### Why block on Kafka publish instead of fire-and-forget?

Async `.whenComplete()` only logs failures — the controller always returns `202` even when Kafka is completely unreachable. Blocking with a 5-second timeout surfaces broker failures as honest `503` responses. In production the **Outbox Pattern** replaces this: write to DB atomically, relay to Kafka asynchronously — no blocking, no dual-write risk.

### Why `orderId` as the idempotency key and not `eventId`?

Each call to `POST /orders` generates a **new** `eventId` UUID at publish time. Using `eventId` would let every retry (and every Kafka redelivery) pass the duplicate check. `orderId` is the stable business identifier chosen by the caller — it remains the same across all retries.

### Why not exactly-once Kafka semantics?

Kafka transactions (producer + consumer atomic) add significant complexity and latency. Idempotency on the consumer side achieves the same correctness guarantee without it.

### Why `apache/kafka:3.8.0` (KRaft) instead of a Bitnami image?

KRaft mode eliminates Zookeeper — the compose file stays at 3 services. The official `apache/kafka` image uses the standard `KAFKA_*` environment variable prefix and is always available on Docker Hub.

### Why `AckMode.RECORD` and not `BATCH`?

Per-record offset commits give precise DLQ routing. With `BATCH`, a single failure could cause the entire batch to redeliver and all successfully-processed records to be re-consumed.

---

## Production Considerations

The following are intentional simplifications for this demo scope. A production system would add:

| Gap | Production Solution |
|---|---|
| In-memory inventory (lost on restart) | Shared state via Redis or PostgreSQL |
| In-memory idempotency set | Redis SET with TTL |
| No circuit breaker | Resilience4j `CircuitBreaker` around Kafka publish |
| Manual `traceId` propagation | Micrometer Tracing + OpenTelemetry auto-instrumentation |
| Single broker, replication factor 1 | 3-broker cluster, `replication.factor=3`, `min.insync.replicas=2` |
| JSON serialization | Avro + Schema Registry for contract enforcement |
| No auth on REST API | Spring Security + OAuth2 / API key |
| No DLQ replay tooling | Replay service + monitoring alerts on DLQ lag |
| Blocking publish (5 s max) | Outbox Pattern (DB-first, async relay) |
| Single inventory-service instance | Multiple instances require shared state for correctness |

