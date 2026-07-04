# Architecture Decisions

This document records the team-owned decisions required by the assignment specification. The selected options are intentionally explicit because these are judgment points, not implementation details to outsource to tooling.

## ADR-001: Inventory Consistency Model

**Decision:** Use Strong Consistency, modeled as CP under CAP and PC/EC under PACELC.

Orders owns the customer order state and the required PostgreSQL order database managed through CloudNativePG. Inventory separately owns the finite seat resource and the reservation/hold state needed to enforce the no-oversell invariant. A seat must never be sold twice, even if requests are duplicated, messages are replayed, or an Inventory pod dies mid-reservation.

### Selected Option: Strong CP / PC-EC

- Use PostgreSQL atomic updates or a reservation state machine as the Inventory source of truth for seat holds. This is separate from Orders' order-state database ownership.
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
- Customer-facing APIs require an authenticated user identity. The cluster/proof target uses Keycloak as the OIDC identity provider. In local development only, this may be replaced by fixed development JWTs or a mock OIDC issuer, but the API shape must still carry a user principal.
- Checkout uses the authenticated customer identity as part of the order ownership and audit trail.
- Order reads are authorized by ownership, with separate operational/admin access for support use cases.
- Inventory and Payments remain internal APIs and are not exposed directly outside the cluster.
- Secrets are managed through SealedSecrets or equivalent GitOps-safe secret mechanisms.
- The payment service integrates with a provider sandbox/test API, such as Stripe test mode or PayPal Sandbox. It must not handle real card data or live charges.

### Why This Is Better

This keeps the project aligned with an enterprise-grade target while allowing a pragmatic implementation path. A mock or simplified identity provider can be used only for local development, while the cluster/proof target must use Keycloak and still define where identity is enforced and how authorization affects the money path.

### Alternative Considered

- **No authentication:** This would reduce implementation work, but it leaves a major enterprise concern unspecified and weakens the design of order ownership, auditability, and payment authorization.

## ADR-006: Payment Provider Integration

**Decision:** Implement Payments as an adapter around a real provider sandbox/test API rather than a pure internal mock.

### Selected Option: Sandbox Provider Adapter

- Use a test-mode payment provider, for example Stripe test mode or PayPal Sandbox.
- Keep provider-specific code inside the Payments service behind a narrow authorization adapter.
- Use sandbox credentials only, loaded from environment/configuration and represented as secrets in GitOps.
- Persist idempotency records before/around provider calls so retries do not create duplicate authorization effects.
- Store only provider references, authorization status, idempotency key, amount, currency, order ID, and audit metadata.
- Do not store raw card data, live credentials, bearer tokens, or provider secrets in application logs or source code.

### Why This Is Better

This gives the project a realistic external dependency, secret-management requirement, latency/failure mode, and audit trail without handling real money or card data. It also makes the Payments resilience and chaos tests meaningful because the service must isolate provider timeouts, declines, retries, and sandbox outages.

### Alternatives Considered

- **Pure internal payment mock:** Easy to implement, but too weak for an enterprise-grade target because it avoids provider credentials, external latency, provider idempotency behavior, and realistic failure handling.
- **Live payment integration:** More realistic, but inappropriate for the project because it introduces real financial risk, compliance concerns, and unnecessary secret exposure.

## ADR-007: Async Cost and Scaling Model

**Decision:** Use Kotlin coroutines/flows for I/O-bound and event-driven checkout work, while keeping correctness boundaries explicit and synchronous where an immediate business decision is required.

### Where Suspending Work Helps

- Orders can accept checkout quickly and run reservation/payment/confirmation stages without dedicating one platform thread to each in-flight order.
- HTTP calls from Orders to Inventory and Payments are I/O-bound. Suspending while waiting for those responses reduces blocked threads under latency or provider slowdown.
- Kafka publishing and consuming are I/O-bound and benefit from coroutine-based backpressure, structured cancellation, and bounded concurrency.
- Notification processing is naturally asynchronous because customer notification must not determine checkout success.
- Graceful shutdown is easier to reason about when the Orders pipeline has an explicit coroutine scope as a failure domain and can stop accepting new work while draining or cancelling in-flight tasks.

### Where Async Does Not Help

- Async does not reduce CPU cost for CPU-bound work such as cryptographic verification, serialization hot spots, compression, or large in-memory transformations. Those need efficient code, bounded worker pools, or horizontal scaling.
- Async does not remove the need for database consistency. Inventory hold creation remains a synchronous decision because the system must know whether a finite seat is held before payment proceeds.
- Async does not make retries safe by itself. Idempotency records are still required for Orders, Inventory, and Payments.
- Async does not replace overload control. The system still needs timeouts, circuit breakers, bulkheads, and load shedding so queues do not grow until the service collapses.

### Operational Consequences

- Bound coroutine concurrency for the checkout pipeline instead of allowing unbounded fan-out.
- Propagate cancellation on SIGTERM and flip readiness before draining.
- Track queue depth, coroutine failures, Kafka lag, and terminal order states so async work is observable.
- Treat blocking calls inside coroutine paths as defects unless they are explicitly isolated on a bounded dispatcher.
