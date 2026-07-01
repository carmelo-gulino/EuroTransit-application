# EuroTransit API and Service Design

Based on the EuroTransit project requirements and the `EuroTransit-application` structure, the system is decomposed into five core services. This document outlines the main APIs, responsibilities, and communication methods for each service.

## 1. Catalog Service
**Responsibility:** Lists products, routes, and offers. Mostly reads. Tolerant of staleness.
**Interaction Style:** Synchronous API.

### Endpoints (Synchronous)
* `GET /api/catalog/routes`
  * **Description:** Retrieves a list of available train routes and schedules.
* `GET /api/catalog/routes/{routeId}`
  * **Description:** Retrieves details for a specific route.
* `GET /api/catalog/offers`
  * **Description:** Retrieves available pricing and ticket offers.

---

## 2. Orders Service
**Responsibility:** Orchestrates the purchase workflow. Provides the synchronous entry point for clients and manages the asynchronous order pipeline.
**Interaction Style:** Synchronous entry API + Asynchronous pipeline (Kafka & Kotlin Coroutines/Flows) + Synchronous calls to Inventory/Payments.

### Endpoints (Synchronous)
* `POST /api/orders`
  * **Description:** Places a new train ticket order. Returns quickly with an order status (e.g., `202 Accepted`) while the reservation and payment proceed.
* `GET /api/orders/{orderId}`
  * **Description:** Polls or retrieves the current status of an order.

### Events (Kafka)
* **Produces:** `order-placed`, `order-confirmed`, `notification-requested`
* **Consumes:** `inventory-reserved`, `payment-authorized` (depending on pipeline implementation)

### Outbound Synchronous Calls (Circuit Broken)
* `POST /api/inventory/reservations` (Inventory)
* `POST /api/payments/authorize` (Payments)

---

## 3. Inventory Service
**Responsibility:** Tracks finite seats (the contended resource). Prevents overselling.
**Interaction Style:** Synchronous reservation API + Asynchronous events.

### Endpoints (Synchronous)
* `POST /api/inventory/reservations`
  * **Description:** Attempts to reserve seats for a specific route. Must be idempotent (requires an idempotency key) to handle retries safely.
* `DELETE /api/inventory/reservations/{reservationId}`
  * **Description:** Releases a reservation (compensating action if the order ultimately fails).

### Events (Kafka)
* **Produces:** `inventory-reserved`, `inventory-failed`

---

## 4. Payments Service
**Responsibility:** Authorizes payments. Must not double-charge.
**Interaction Style:** Synchronous call with strict idempotency.

### Endpoints (Synchronous)
* `POST /api/payments/authorize`
  * **Description:** Authorizes a payment for an order. Requires an idempotency key (e.g., the `orderId` or a dedicated token) to prevent double-charging on retried requests.

### Events (Kafka)
* **Produces:** `payment-authorized`, `payment-declined`

---

## 5. Notifications Service
**Responsibility:** Sends booking confirmations and updates to the user.
**Interaction Style:** Fully asynchronous. Failure must degrade gracefully (checkout still succeeds even if notifications are down).

### Endpoints
* *No public synchronous APIs exposed.*

### Events (Kafka)
* **Consumes:** `notification-requested` or `order-confirmed`
* **Action:** Sends email/SMS confirmations based on the consumed events.

---

## Communication & Resilience Summary
* **North-South Traffic:** Traefik API Gateway exposes `/api/catalog` and `/api/orders` to the outside world.
* **East-West Sync Traffic:** `Orders` -> `Inventory` and `Orders` -> `Payments`. These calls must be wrapped in **Circuit Breakers** with defined open/half-open policies, timeouts, and bounded retries with jitter.
* **Async Eventing:** Kafka (via Strimzi) handles the event pipeline (`order-placed`, `inventory-reserved`, `payment-authorized`, `order-confirmed`, `notification-requested`).
