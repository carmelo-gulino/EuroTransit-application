# EuroTransit API and Service Design

This document defines the service boundaries and communication contracts for EuroTransit. The design intentionally separates synchronous decisions that must be made during checkout from asynchronous work that can be retried, observed, and recovered through Kafka.

For the rationale behind the selected consistency, money-path, and delivery decisions, see [architecture-decisions.md](architecture-decisions.md).

## Service Boundaries

| Service | Responsibility | Interaction Style | Why this boundary exists |
| --- | --- | --- | --- |
| Catalog | Exposes routes, schedules, products, and offers. | Synchronous read API. | Catalog is read-heavy and tolerant of stale data, so it can be scaled and deployed independently from checkout. |
| Orders | Owns the customer-facing purchase workflow and order state. | Synchronous entry API plus asynchronous pipeline orchestration. | Orders is the money-path coordinator: it accepts checkout requests quickly and records/reports order progress. |
| Inventory | Owns finite seat availability and reservations. | Synchronous reservation decision plus reservation events. | Inventory is the contended resource. It must make a strongly consistent decision before a seat is considered reserved. |
| Payments | Owns payment authorization state. | Synchronous idempotent authorization plus payment events. | Payment authorization must not double-charge under retries; an immediate success/failure decision is needed before order confirmation. |
| Notifications | Sends confirmations and customer updates. | Fully asynchronous event consumer. | Notification failure must not fail checkout. Kafka buffers the work until the service recovers. |

## Public APIs

### Catalog

- `GET /api/catalog/routes`
  - Lists available routes and schedules.
- `GET /api/catalog/routes/{routeId}`
  - Returns details for one route.
- `GET /api/catalog/offers`
  - Lists currently available pricing and ticket offers.

### Orders

- `POST /api/orders`
  - Accepts a checkout request.
  - Returns `202 Accepted` with an `orderId` and initial status after the request has been accepted for processing.
  - Requires an idempotency key so client retries do not create duplicate orders.
- `GET /api/orders/{orderId}`
  - Returns the current order status: accepted, reserving, payment-pending, confirmed, failed, or cancelled.

### Inventory

- `POST /api/inventory/reservations`
  - Attempts to reserve requested seats.
  - Requires an idempotency key derived from the order id and reservation attempt.
  - Uses a strong consistency decision: the same seat cannot be reserved twice.
- `DELETE /api/inventory/reservations/{reservationId}`
  - Releases a reservation when payment fails or the order is cancelled.

### Payments

- `POST /api/payments/authorize`
  - Authorizes payment for one order.
  - Requires an idempotency key so retries return the original authorization result instead of charging again.

### Notifications

- No public synchronous API is exposed.
- The service consumes events and sends email/SMS confirmations or failure updates.

## Kafka Events

| Event | Producer | Consumers | Purpose |
| --- | --- | --- | --- |
| `order-placed` | Orders | Orders pipeline observers, optional analytics | Durable record that checkout was accepted. |
| `inventory-reserved` | Inventory | Orders | Indicates that the requested seats were reserved. |
| `inventory-failed` | Inventory | Orders | Indicates that reservation failed and the order must not proceed to confirmation. |
| `payment-authorized` | Payments | Orders | Indicates payment authorization succeeded. |
| `payment-declined` | Payments | Orders | Indicates payment failed and compensation may be needed. |
| `order-confirmed` | Orders | Notifications, observability consumers | Indicates the order is complete and customer notification can be sent. |
| `notification-requested` | Orders | Notifications | Optional explicit notification command if separated from `order-confirmed`. |

## Money Path

The selected money path is hybrid sync+async:

1. Client submits `POST /api/orders`.
2. Orders validates the request, stores an accepted order, emits `order-placed`, and returns `202 Accepted`.
3. Orders executes the reservation/payment pipeline with Kotlin coroutines/flows and structured cancellation.
4. Orders calls Inventory synchronously for the seat reservation because this is the consistency boundary.
5. Orders calls Payments synchronously for authorization because payment must be idempotent and immediately known before confirmation.
6. Orders emits final events such as `order-confirmed` and `notification-requested`.
7. Notifications consumes events asynchronously; if it is down, checkout remains successful and Kafka retains the work.

## Resilience Requirements

- All synchronous service calls must have timeouts, bounded retries with backoff/jitter, circuit breakers, and bulkheads.
- Orders must flip readiness to refuse new traffic during shutdown while in-flight coroutine work drains or is cancelled cooperatively.
- Inventory and Payments must persist idempotency records so duplicate events or HTTP retries return the same outcome.
- Notification failure is a degraded mode, not a checkout failure.
- The checkout SLOs, alerts, and tracing requirements are defined in [slo-observability.md](slo-observability.md).
