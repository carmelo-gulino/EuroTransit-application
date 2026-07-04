# EuroTransit API and Service Design

This document defines the service boundaries and communication contracts for EuroTransit. The design intentionally separates synchronous decisions that must be made during checkout from asynchronous work that can be retried, observed, and recovered through Kafka.

For the rationale behind the selected consistency, money-path, and delivery decisions, see [architecture-decisions.md](architecture-decisions.md).

## Security Boundary

EuroTransit is designed as an enterprise-grade system. Public APIs must assume an authenticated user identity. Local development may use fixed development JWTs or a mock OIDC issuer, but the cluster/proof target uses Keycloak as the OIDC identity provider.

- Traefik is the only north-south entry point and terminates TLS in the cluster environment.
- Public APIs expect OAuth2/OIDC bearer JWTs. Tokens must carry a stable subject (`sub`), tenant or organization context when applicable, and role/scope claims.
- The production-like cluster target uses Keycloak for OIDC. Mock identity providers and fixed JWTs are local-development substitutes only.
- Customer-facing write APIs, especially checkout, require an authenticated customer identity.
- `GET /api/orders/{orderId}` must only return orders owned by the authenticated customer, unless the caller has an operational/admin role.
- Inventory and Payments APIs are internal service-to-service APIs and are not exposed directly to public clients.
- Service-to-service requests must propagate correlation IDs, the authenticated principal where required, and a trusted service identity. The production target should use mTLS or an equivalent workload-identity control.
- Gateway authentication is not a substitute for service-level authorization. Each service must enforce local ownership, role, and scope decisions for resources it owns.
- Money-path actions must emit audit-ready logs with correlation ID, principal ID, order ID, and outcome. Logs must not contain bearer tokens, secrets, raw payment data, or card details.
- Secrets such as database credentials, signing keys, and payment-provider credentials belong in the configuration repository as SealedSecrets, not in application source.

### Endpoint Security Summary

| Endpoint Group | Exposure | Authentication | Authorization |
| --- | --- | --- | --- |
| `GET /api/catalog/**` | Public or authenticated depending on offer visibility | Optional for public catalog; required for personalized offers | Public read or customer scope |
| `POST /api/orders` | External | Required | Customer scope; principal becomes order owner |
| `GET /api/orders/{orderId}` | External | Required | Order owner or privileged operations scope |
| `POST /api/inventory/reservations` | Internal | Required service credential plus propagated user context | Orders service only; order owner context required |
| `DELETE /api/inventory/reservations/{reservationId}` | Internal | Required service credential plus propagated user context | Orders service only; compensation for owned order |
| `POST /api/payments/authorize` | Internal | Required service credential plus propagated user context | Orders service only; payment belongs to owned order |
| `GET /api/notifications` | External | Required | Authenticated customer can read only their own notifications |
| `GET /api/notifications/stream` | External | Required | Authenticated customer can receive only their own live notification events |

## Service Boundaries

| Service | Responsibility | Interaction Style | Why this boundary exists |
| --- | --- | --- | --- |
| Catalog | Exposes routes, schedules, products, and offers. | Synchronous read API. | Catalog is read-heavy and tolerant of stale data, so it can be scaled and deployed independently from checkout. |
| Orders | Owns the customer-facing purchase workflow, order state, and the required PostgreSQL order database managed through CloudNativePG. | Synchronous entry API plus asynchronous pipeline orchestration. | Orders is the money-path coordinator: it accepts checkout requests quickly and records/reports order progress. |
| Inventory | Owns finite seat availability and time-limited seat holds. | Synchronous hold decision plus reservation events. | Inventory is the contended resource. It must create one strongly consistent hold before a seat can proceed to payment. Its reservation store is separate from Orders' order-state ownership. |
| Payments | Owns payment authorization state and integrates with a provider sandbox/test API. | Synchronous idempotent authorization plus payment events. | Payment authorization must not double-charge under retries; an immediate success/failure decision is needed before order confirmation. |
| Notifications | Sends confirmations and customer updates. | Fully asynchronous event consumer. | Notification failure must not fail checkout. Kafka buffers the work until the service recovers. |

## Public APIs

### Catalog

- `GET /api/catalog/routes`
  - Lists available routes and schedules. Public data may be served without authentication; personalized pricing requires an authenticated user.
- `GET /api/catalog/routes/{routeId}`
  - Returns details for one route.
- `GET /api/catalog/offers`
  - Lists currently available pricing and ticket offers. Personalized offers require a customer principal.

### Orders

- `POST /api/orders`
  - Accepts a checkout request.
  - Requires an authenticated customer identity.
  - Returns `202 Accepted` with an `orderId` and initial status after the request has been accepted for processing.
  - Requires an idempotency key so client retries do not create duplicate orders.
- `GET /api/orders/{orderId}`
  - Returns the current order status: accepted, reserving, payment-pending, confirmed, failed, or cancelled.
  - Requires the authenticated customer to own the order, unless an operator/admin role is used.
- Orders persists order state in its PostgreSQL database managed by CloudNativePG.

### Inventory

- `POST /api/inventory/reservations`
  - Attempts to create a 10-minute hold for the requested seats.
  - Internal API; callers must be trusted services such as Orders.
  - Requires an idempotency key derived from the order id and reservation attempt.
  - Uses a strong consistency decision: the same seat cannot have two active holds.
  - Owns reservation/seat availability state for the no-oversell invariant; this does not replace Orders' ownership of the order-state database.
- `DELETE /api/inventory/reservations/{reservationId}`
  - Releases a hold when payment fails, the order is cancelled, or the 10-minute hold expires.

### Payments

- `POST /api/payments/authorize`
  - Authorizes payment for one order.
  - Internal API; callers must be trusted services such as Orders.
  - Requires an idempotency key so retries return the original authorization result instead of charging again.
  - Calls a configured provider sandbox/test API through a Payments-owned adapter; raw card data and live charges are out of scope.

### Notifications

- The service consumes events and sends or records email/SMS confirmations and failure updates.
- `GET /api/notifications`
  - Returns the authenticated customer's notification history.
  - Requires a normal bearer JWT and only returns notifications owned by the authenticated principal.
- `GET /api/notifications/stream`
  - Exposes live notification updates to the frontend using Server-Sent Events with `text/event-stream`.
  - Sends only notifications owned by the authenticated principal.
  - Is an enhancement over the stored notification history; if the stream disconnects, the frontend can fall back to `GET /api/notifications`.

## Kafka Events

| Event | Producer | Consumers | Purpose |
| --- | --- | --- | --- |
| `order-placed` | Orders | Orders pipeline observers, optional analytics | Durable record that checkout was accepted. |
| `inventory-reserved` | Inventory | Orders | Indicates that the requested seats are held for 10 minutes and can proceed to payment. |
| `inventory-failed` | Inventory | Orders | Indicates that the hold failed and the order must not proceed to payment. |
| `payment-authorized` | Payments | Orders | Indicates payment authorization succeeded. |
| `payment-declined` | Payments | Orders | Indicates payment failed and compensation may be needed. |
| `order-confirmed` | Orders | Notifications, observability consumers | Indicates the order is complete and customer notification can be sent. |
| `notification-requested` | Orders | Notifications | Optional explicit notification command if separated from `order-confirmed`. |

## Money Path

The selected money path is hybrid sync+async:

1. Client submits `POST /api/orders`.
2. Orders validates the request, stores an accepted order, emits `order-placed`, and returns `202 Accepted`.
3. Orders executes the reservation/payment pipeline with Kotlin coroutines/flows and structured cancellation.
4. Orders calls Inventory synchronously to create a 10-minute seat hold because this is the consistency boundary.
5. Orders calls Payments synchronously only after the hold succeeds, because payment must be idempotent and immediately known before confirmation.
6. Orders emits final events such as `order-confirmed` and `notification-requested`.
7. Notifications consumes events asynchronously; if it is down, checkout remains successful and Kafka retains the work.

## Resilience Requirements

- All synchronous service calls must have timeouts, bounded retries with backoff/jitter, circuit breakers, and bulkheads.
- Orders must flip readiness to refuse new traffic during shutdown while in-flight coroutine work drains or is cancelled cooperatively.
- Inventory and Payments must persist idempotency records so duplicate events or HTTP retries return the same outcome.
- Inventory holds must expire after 10 minutes if payment does not complete, returning the seat to available state.
- Notification failure is a degraded mode, not a checkout failure.
- The checkout SLOs, alerts, and tracing requirements are defined in [slo-observability.md](../operations/slo-observability.md).
