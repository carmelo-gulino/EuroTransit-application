# Architecture Decisions

This document records the team-owned decisions required by the assignment specification. The selected options are intentionally explicit because these are judgment points, not implementation details to outsource to tooling.

## ADR-001: Inventory Consistency Model

**Decision:** Use Strong Consistency, modeled as CP under CAP and PC/EC under PACELC.

Inventory owns the finite seat resource. A seat must never be sold twice, even if requests are duplicated, messages are replayed, or an Inventory pod dies mid-reservation.

### Selected Option: Strong CP / PC-EC

- Use PostgreSQL atomic updates or a reservation state machine as the source of truth.
- Seat selection in the UI is tentative and does not reserve inventory.
- Checkout creates a strongly consistent 10-minute hold only if the seat is available.
- Payment is attempted only after the hold succeeds.
- If payment succeeds before the hold expires, the seat transitions to sold.
- If payment fails, the order is cancelled, or the 10-minute hold expires, the seat returns to available.
- Store idempotency keys for hold/reservation attempts so retries return the original result.
- During a database partition or primary failover, checkout writes may fail temporarily rather than accept inconsistent holds.

The intended state model is:

```text
AVAILABLE -> HELD -> SOLD
AVAILABLE -> HELD -> RELEASED/EXPIRED -> AVAILABLE
```

### Why This Is Better

This option protects the core business invariant. EuroTransit can tolerate a temporary checkout failure more easily than selling the same seat to two customers. It is also easier to prove under chaos testing: the expected invariant is binary and observable.

### Alternatives Considered

- **Eventual Saga reservation:** Accept orders asynchronously and compensate later if the same seat is assigned twice. This increases availability but violates the "never oversell" invariant temporarily and requires refund/cancellation flows that are harder to defend in the assignment proof.
- **Short-lived hold without strong consistency:** Create a hold with expiration but without a strongly consistent database transition. This still risks two active holds for the same seat under concurrency, so it does not protect the core invariant.

## ADR-002: Money Path Interaction Model

**Decision:** Use a hybrid sync+async checkout model.

### Selected Option: Hybrid Sync+Async

- `POST /api/orders` returns `202 Accepted` quickly with an `orderId`.
- Orders persists the accepted order and emits `order-placed`.
- Orders runs the checkout pipeline with Kotlin coroutines/flows.
- Inventory hold creation is a synchronous idempotent decision because seat consistency must be known before payment.
- Payment authorization is a synchronous idempotent decision because retries must not double-charge.
- Kafka carries durable pipeline events and decouples notifications from checkout success.

### Why This Is Better

This option matches the assignment's async requirement without pretending every decision can safely be deferred. The costly/slow parts are observable and recoverable through Kafka, while the two correctness boundaries, seat hold creation and payment authorization, remain explicit synchronous decisions protected by resilience policies.

### Alternative Considered

- **Fully asynchronous checkout:** Orders would only publish events and all downstream work would happen through Kafka. This maximizes decoupling, but it makes immediate reservation/payment outcomes less clear and complicates the user-facing order state. It is harder to prove "no oversell" and "no double-charge" without building a more complex saga and reconciliation model.

## ADR-003: Progressive Delivery Mapping

**Decision:** Demonstrate canary on Orders and blue/green on Catalog.

### Selected Option: Orders Canary + Catalog Blue/Green

- **Orders canary:** Route a small traffic share, for example 5%, to the new Orders version. Promote only if checkout success rate, latency, error rate, and circuit-breaker metrics stay within the defined SLO guardrails.
- **Catalog blue/green:** Deploy the new Catalog version beside the old one, validate health and read endpoints, then switch traffic. Keep the previous version available for fast rollback.

### Why This Is Better

Orders is the critical money path, so changes must be exposed gradually and judged with production-like symptoms. Catalog is stateless and read-heavy, so it is a better fit for blue/green: both versions can coexist, validation is simple, and rollback is low risk.

### Alternative Considered

- **Inventory canary + Payments blue/green:** Inventory contains critical consistency logic, so a canary sounds attractive. However, canarying state-sensitive reservation behavior can make proof harder because two versions may compete over one invariant. Payments blue/green is reasonable, but it does not demonstrate progressive protection of the central orchestration path as clearly as Orders canary.

## ADR-004: Notification Failure Mode

**Decision:** Notification failure is graceful degradation.

Checkout is complete once the 10-minute seat hold is converted to sold, payment succeeds, and Orders records confirmation. Notifications consumes `order-confirmed` or `notification-requested` asynchronously. If Notifications is down, Kafka retains the event and checkout remains successful.

## ADR-005: Authentication and Security Scope

**Decision:** Treat EuroTransit as enterprise-grade. Authentication and authorization are part of the design, not optional polish.

### Selected Option: Authenticated Public APIs with Internal Service Trust

- Public traffic enters only through Traefik with TLS configured in the cluster configuration repository.
- Customer-facing APIs require an authenticated user identity. In local development this may be a mock identity provider, but the API shape must still carry a user principal.
- Checkout uses the authenticated customer identity as part of the order ownership and audit trail.
- Order reads are authorized by ownership, with separate operational/admin access for support use cases.
- Inventory and Payments remain internal APIs and are not exposed directly outside the cluster.
- Secrets are managed through SealedSecrets or equivalent GitOps-safe secret mechanisms.
- The payment service is a mock authorization service for this project and must not handle real card data.

### Why This Is Better

This keeps the project aligned with an enterprise-grade target while allowing a pragmatic implementation path. A mock or simplified provider can be used first, but the architecture still defines where identity is enforced and how authorization affects the money path.

### Alternative Considered

- **No authentication:** This would reduce implementation work, but it leaves a major enterprise concern unspecified and weakens the design of order ownership, auditability, and payment authorization.
