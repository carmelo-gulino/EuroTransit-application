# EuroTransit Team Agentic Implementation Plan

This plan defines how five people should work in parallel on EuroTransit Marketplace while using AI coding agents. The target is an enterprise-grade implementation ready by **2026-07-14**.

The roles below are **ownership boundaries**, not fully independent work streams. The team can work in parallel only after agreeing on shared contracts for APIs, events, authentication, idempotency, and observability.

## Operating Principles

- Treat authentication, authorization, idempotency, resilience, observability, and secure secret handling as core requirements, not polish.
- Keep service ownership clear. Each owner can use an AI coding agent inside their owned module, but cross-service contracts require team agreement.
- Prefer mockable contracts early so each owner can implement and test independently before full integration.
- Do not use academic assignment wording in product-facing code, documentation, or demo material. Use **EuroTransit** or **EuroTransit Marketplace**.
- Every pull request must include verification evidence: tests run, manual checks, or a clear reason when a check is not applicable.
- Any agent-generated change affecting authentication, payments, inventory consistency, delivery, or secrets requires human review by another teammate.

## Role Ownership

| Person | Role | Primary Ownership | Main Deliverables |
| --- | --- | --- | --- |
| Person 1 | Delivery + Security Owner | CI/CD, Docker Compose, config repo integration, Traefik, auth boundary, secret handling | Build pipeline, local infrastructure, gateway auth, internal routing, secure config baseline |
| Person 2 | Domain Owner | `orders` service and checkout orchestration | Order APIs, order lifecycle, ownership checks, orchestration clients, order events |
| Person 3 | Data Owner | `inventory` service and data consistency | Seat/reservation model, PostgreSQL schema, no-oversell guarantees, reservation idempotency |
| Person 4 | Resilience Owner | `payments` service and money-path resilience | Payment authorization mock, payment idempotency, Resilience4j policies, failure behavior |
| Person 5 | Observability + Async Proof Owner | `notifications`, Kafka proof, telemetry, chaos/demo evidence | Notification consumer, correlation IDs, metrics/logging/tracing, dashboards, chaos experiment evidence |

## Dependency Map

The roles are parallelizable but not independent.

| Role | Can Start Immediately | Critical Dependencies |
| --- | --- | --- |
| Delivery + Security Owner | Yes | Must define auth/routing/config baseline early so all services implement the same assumptions |
| Domain Owner | Partially | Depends on Inventory, Payments, authentication model, event contracts, and idempotency semantics |
| Data Owner | Yes | Depends on reservation contract and idempotency key semantics agreed with Orders |
| Resilience Owner | Yes | Depends on payment authorization contract and failure semantics agreed with Orders |
| Observability + Async Proof Owner | Partially | Depends on event payloads, correlation ID propagation, and metric/log conventions from all services |

## Phase 0: Contract Freeze

Before feature implementation starts, the whole team must agree on these contracts. This should be short and concrete; the output should be committed under `docs/contracts/` or directly in the relevant service test fixtures.

### Shared HTTP Headers

- `Authorization`: OAuth2/OIDC bearer JWT for external APIs.
- `X-Correlation-Id`: required on inbound requests; generated at the gateway if missing.
- `Idempotency-Key`: required for order placement, inventory reservation, and payment authorization.
- `X-Service-Name` or equivalent service identity mechanism for internal calls in local development.

### Authentication and Authorization Contract

- Public catalog reads may be unauthenticated unless personalized offers are requested.
- `POST /api/orders` requires an authenticated customer principal.
- `GET /api/orders/{orderId}` allows only the order owner or a privileged operations scope.
- Inventory and Payments APIs are internal-only and require service credentials plus propagated user context.
- Services must enforce local authorization; gateway checks are not sufficient.

### Money-Path API Contracts

- `POST /api/orders`
  - Creates an order for the authenticated principal.
  - Requires `Idempotency-Key`.
  - Returns an order ID and current status.
- `GET /api/orders/{orderId}`
  - Returns order status only to the owner or privileged operations users.
- `POST /api/inventory/reservations`
  - Reserves seats atomically.
  - Requires idempotency key and order ID.
  - Never oversells.
- `DELETE /api/inventory/reservations/{reservationId}`
  - Releases a reservation as a compensating action.
- `POST /api/payments/authorize`
  - Authorizes payment for an order.
  - Requires idempotency key.
  - Never double-charges on retries.

### Kafka Event Contracts

- `order-placed`
- `inventory-reserved`
- `inventory-failed`
- `payment-authorized`
- `payment-declined`
- `order-confirmed`
- `notification-requested`

Every event must include:

- `eventId`
- `eventType`
- `occurredAt`
- `correlationId`
- `orderId`
- `principalId` when user context is needed downstream
- payload version

Events must not include secrets, raw payment details, or bearer tokens.

### Error Model

- `400`: malformed request.
- `401`: missing or invalid authentication.
- `403`: authenticated but not authorized.
- `404`: resource not found or not visible to the caller.
- `409`: idempotency conflict or reservation conflict.
- `422`: valid request shape but invalid business action.
- `503`: dependency unavailable or circuit breaker open.

## Phase 1: Parallel Service Foundations

Each owner implements their service skeleton and contract tests using mocks where needed.

### Delivery + Security Owner

- Add CI jobs for build and tests.
- Keep Docker Compose usable for local infrastructure.
- Define local auth strategy: mock OIDC issuer or fixed development JWTs.
- Configure Traefik routes:
  - external: Catalog and Orders only;
  - internal: Inventory, Payments, Notifications not publicly exposed.
- Document secret handling and required environment variables.

### Domain Owner

- Implement Orders API controllers and service layer.
- Persist or model order lifecycle states.
- Enforce order ownership from authenticated principal.
- Emit `order-placed`, `order-confirmed`, and `notification-requested`.
- Use mock clients for Inventory and Payments until real clients are integrated.

### Data Owner

- Implement Inventory reservation API.
- Define database schema for routes, seats, reservations, and idempotency records.
- Use strong consistency for finite seats through conditional updates or equivalent transactional logic.
- Emit `inventory-reserved` and `inventory-failed`.
- Add concurrency tests proving no oversell.

### Resilience Owner

- Implement Payments authorization API.
- Add idempotency records so retrying the same authorization does not double-charge.
- Define payment outcomes for success, decline, duplicate request, and dependency failure.
- Configure Resilience4j policies used by Orders clients:
  - timeout;
  - bounded retry with jitter;
  - circuit breaker;
  - bulkhead.

### Observability + Async Proof Owner

- Implement Notifications consumer.
- Add graceful degradation behavior: checkout must succeed even when Notifications is down.
- Define correlation ID logging conventions.
- Add Micrometer metrics and structured logs for all service owners to copy.
- Start dashboard and alert templates.

## Phase 2: Money-Path Integration

The team integrates the checkout flow end to end.

Integration target:

1. Authenticated customer calls `POST /api/orders`.
2. Orders creates or reuses an idempotent order.
3. Orders reserves inventory.
4. Orders authorizes payment.
5. Orders confirms or fails the order with clear compensation behavior.
6. Kafka carries confirmation/notification events.
7. Notifications sends or records a confirmation without blocking checkout.

Required checks:

- Duplicate `POST /api/orders` with the same `Idempotency-Key` returns the same logical result.
- Duplicate Inventory reservation does not double-reserve.
- Duplicate Payment authorization does not double-charge.
- Unauthorized order reads return `403` or `404` according to the agreed policy.
- Payments failure releases or marks inventory reservation according to the agreed compensation flow.

## Phase 3: Enterprise Hardening

This phase turns the integrated flow into an enterprise-grade system.

### Security

- Reject unauthenticated protected requests at Traefik.
- Recheck principal and scope inside each service.
- Prevent external routing to Inventory and Payments.
- Ensure secrets are externalized and not committed.
- Verify logs do not contain tokens, secrets, or payment details.

### Resilience

- Configure Resilience4j for Orders -> Inventory and Orders -> Payments.
- Add timeouts to every remote call.
- Add safe fallbacks for dependency failures.
- Confirm graceful shutdown drains in-flight work.
- Add readiness/liveness/startup probes in deployment manifests.

### Observability

- Propagate `X-Correlation-Id` through HTTP and Kafka.
- Add RED metrics per service.
- Add success-rate and latency SLOs for checkout.
- Add symptom-based alerts.
- Ensure a single order can be traced across the money path.

## Phase 4: Delivery Proof and Final Evidence

Final delivery is not just code complete. The team must produce proof.

- Demonstrate GitOps delivery with immutable images and config repo updates.
- Demonstrate at least two progressive delivery strategies:
  - Canary for Orders;
  - Blue/Green for Catalog.
- Run and document chaos experiments:
  - latency injection into Payments;
  - Inventory pod kill mid-reservation;
  - Kafka network partition;
  - database primary failover.
- Record a short demo showing:
  - checkout flow;
  - authentication and authorization behavior;
  - dashboards;
  - progressive delivery;
  - alert firing under injected failure.
- Write a blameless postmortem for one injected incident.

## Agent Coding Governance

Each person may use an AI coding agent, but ownership and review rules remain human responsibilities.

### Allowed Agent Tasks

- Generate service boilerplate following existing project conventions.
- Create focused tests for owned modules.
- Implement DTOs, controllers, services, repositories, and configuration within the owner's module.
- Draft documentation, runbooks, dashboards, and chaos experiment manifests.
- Refactor local code when tests and contracts remain stable.

### Restricted Agent Tasks

- Do not let an agent silently change shared contracts without team review.
- Do not accept generated authentication, payment, inventory, CI/CD, or secret-handling changes without another human reviewer.
- Do not allow generated code to log tokens, secrets, personal data, or payment details.
- Do not use generated shortcuts that bypass authentication, idempotency, consistency, or observability requirements.

### Required Agent Log

Maintain `docs/agent-log.md` with at least three real cases where the agent produced incorrect, unsafe, incomplete, or non-idempotent output.

Each entry should include:

- date;
- owner;
- prompt or task summary;
- what the agent produced;
- why it was wrong or risky;
- how the team detected it;
- final correction.

## Branching and Pull Request Rules

- Use one branch per owner or per feature slice.
- Keep cross-service contract changes in small PRs and merge them before implementation PRs depend on them.
- Every PR must list:
  - files or modules touched;
  - tests run;
  - contract changes;
  - security impact;
  - operational impact.
- Do not merge changes that break another owner's module without coordinating a same-day fix.

## Daily Integration Routine

- Start each day by rebasing or merging the latest main branch.
- Spend 10 minutes reviewing contract changes and integration blockers.
- Keep mocks aligned with real contracts.
- Integrate at least once per day, even if one dependency is still mocked.
- End each day with a short status:
  - done;
  - blocked;
  - next integration risk;
  - agent issues worth logging.

## Minimum Done Criteria by Role

### Delivery + Security Owner

- CI builds all modules and runs tests.
- Local infrastructure starts with Docker Compose.
- Gateway exposes only intended public APIs.
- Protected APIs reject unauthenticated requests.
- Secret handling is documented and no secrets are committed.

### Domain Owner

- Orders APIs are implemented.
- Order ownership is enforced.
- Checkout orchestration works with real Inventory and Payments.
- Order idempotency is tested.
- Order events are emitted with correlation IDs.

### Data Owner

- Inventory reservations are transactional.
- Concurrent reservation tests prove no oversell.
- Reservation idempotency is tested.
- Compensation endpoint releases reservations safely.
- Inventory events are emitted with correlation IDs.

### Resilience Owner

- Payments authorization is idempotent.
- Duplicate retries do not double-charge.
- Orders clients use timeout, retry, circuit breaker, and bulkhead policies.
- Failure behavior is documented and tested.

### Observability + Async Proof Owner

- Notifications consumes confirmation events.
- Checkout succeeds when Notifications is unavailable.
- Logs, metrics, and traces include correlation IDs.
- Dashboards and alerts cover the checkout path.
- Chaos experiments are documented with hypotheses and conclusions.
