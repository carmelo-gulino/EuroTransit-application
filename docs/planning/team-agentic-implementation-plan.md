# EuroTransit Team Agentic Implementation Plan

This plan defines how five people should work in parallel on EuroTransit Marketplace while using AI coding agents. The target is an enterprise-grade implementation ready by **2026-07-14**.

The roles below are **vertical ownership boundaries**, not isolated specialist lanes. Every person owns application code plus the related Kubernetes/GitOps, test, observability, and operational proof for their slice. The team can work in parallel only after agreeing on shared contracts for APIs, events, authentication, idempotency, and observability.

## Operating Principles

- Treat authentication, authorization, idempotency, resilience, observability, and secure secret handling as core requirements, not polish.
- Keep vertical ownership clear. Each owner can use an AI coding agent inside their owned slice, but cross-service contracts require team agreement.
- Avoid "backend-only" ownership. Every slice includes service code, container/build concerns, Kubernetes manifests or Helm values, GitOps integration, metrics, and proof artifacts.
- The coordination role owns alignment, sequencing, and review quality, but must not become the only person able to change platform or GitOps assets.
- Prefer mockable contracts early so each owner can implement and test independently before full integration.
- Do not carry mocks into the final cluster/demo proof. Mock clients, fixed JWTs, and fake providers are local-development aids only.
- Do not use academic assignment wording in product-facing code, documentation, or demo material. Use **EuroTransit** or **EuroTransit Marketplace**.
- Every pull request must include verification evidence: tests run, manual checks, or a clear reason when a check is not applicable.
- Any agent-generated change affecting authentication, payments, inventory consistency, delivery, or secrets requires human review by another teammate.

## Role Ownership

| Person | Role | Primary Slice | Required Platform/GitOps Ownership | Main Deliverables |
| --- | --- | --- | --- | --- |
| Person 1 | Technical Coordinator + Gateway/Security Slice Owner | Cross-team coordination, public API contracts, auth boundary, gateway behavior | Traefik routes/middleware, Keycloak/OIDC configuration, shared CI conventions, PR/release sequencing | Contract freeze, secure routing, local dev JWT/mock OIDC strategy, cluster Keycloak proof, review gates, integration calendar |
| Person 2 | Orders Checkout Slice Owner | `orders` service, checkout orchestration, and order-state database ownership | Orders image/build config, Orders deployment values/manifests, CloudNativePG order database requirements, readiness/liveness, canary rollout | Order APIs, order lifecycle, ownership checks, orchestration clients, order events, Orders canary proof |
| Person 3 | Inventory Consistency Slice Owner | `inventory` service and finite-seat consistency | Inventory reservation-store config, migration/init strategy, Inventory deployment values/manifests | Seat/reservation model, no-oversell guarantee, reservation idempotency, compensation endpoint, failover proof input |
| Person 4 | Payments Resilience Slice Owner | `payments` service, provider sandbox integration, and money-path resilience | Payments deployment values/manifests, Resilience4j config exposure, service routing, payment provider sandbox secrets | Payment authorization adapter, payment idempotency, timeout/retry/circuit-breaker behavior, latency chaos proof |
| Person 5 | Customer Experience + Observability Slice Owner | React frontend, `catalog`, `notifications`, Kafka consumer proof, telemetry | Frontend image/deployment values, Catalog blue/green deployment, Notifications deployment values/manifests, Kafka topic config, dashboards/alerts/tracing | Customer UI, Catalog reads, order-status view, notification consumer, graceful degradation, RED metrics, traces, alert/demo evidence |

The coordinator is a real delivery role, but not a platform gatekeeper. Each slice owner must be able to ship their service from source code to GitOps-controlled runtime with review from the coordinator and one peer.

## Dependency Map

The roles are parallelizable but not independent. The main dependency is the shared contract layer, followed by cross-slice integration through Orders.

| Role | Can Start Immediately | Critical Dependencies |
| --- | --- | --- |
| Technical Coordinator + Gateway/Security | Yes | Must freeze contracts, auth/routing assumptions, CI/GitOps conventions, and review rules early |
| Orders Checkout | Partially | Depends on Inventory, Payments, authentication model, event contracts, and idempotency semantics |
| Inventory Consistency | Yes | Depends on reservation contract, database assumptions, and idempotency key semantics agreed with Orders |
| Payments Resilience | Yes | Depends on payment authorization contract and failure semantics agreed with Orders |
| Customer Experience + Observability | Partially | Depends on public API contracts, auth/token handling, event payloads, correlation ID propagation, topic names, and metric/log conventions from all services |

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
- The cluster/proof target uses Keycloak as the OIDC identity provider.
- Fixed development JWTs or mock OIDC issuers are allowed only for local development.

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
  - Uses a configured provider sandbox/test API such as Stripe test mode or PayPal Sandbox through the Payments service adapter.

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

## Phase 1: Parallel Vertical Foundations

Each owner implements their slice skeleton and contract tests using mocks where needed. Every slice must include enough Kubernetes/GitOps work to be deployable, observable, and reviewable in the configuration repository.

### Technical Coordinator + Gateway/Security Slice

- Drive the contract freeze and keep the integration board current.
- Add or validate CI jobs for build and tests.
- Keep Docker Compose usable for local infrastructure.
- Define local auth strategy: mock OIDC issuer or fixed development JWTs.
- Define cluster/proof auth strategy: Keycloak realm, clients, roles/scopes, issuer URL, and JWKS validation.
- Configure or specify Traefik routes:
  - external: Catalog and Orders only;
  - internal: Inventory, Payments, Notifications not publicly exposed.
- Define shared deployment conventions: image names, ports, health endpoints, resource defaults, labels, and environment variable naming.
- Document secret handling and required environment variables.

### Orders Checkout Slice

- Implement Orders API controllers and service layer.
- Persist or model order lifecycle states.
- Own Orders PostgreSQL order-state schema and CloudNativePG configuration requirements.
- Enforce order ownership from authenticated principal.
- Emit `order-placed`, `order-confirmed`, and `notification-requested`.
- Use mock clients for Inventory and Payments until real clients are integrated.
- Own Orders container/build concerns and deployment values/manifests.
- Configure Orders startup/readiness/liveness behavior.
- Prepare Orders canary proof and SLO guardrails with the Observability owner.

### Inventory Consistency Slice

- Implement Inventory reservation API.
- Define database schema for routes, seats, reservations, and idempotency records.
- Use strong consistency for finite seats through conditional updates or equivalent transactional logic.
- Emit `inventory-reserved` and `inventory-failed`.
- Add concurrency tests proving no oversell.
- Own Inventory container/build concerns and deployment values/manifests.
- Define Inventory reservation-store configuration needs for the configuration repository without confusing them with Orders' order-state database ownership.
- Provide failover and pod-kill proof inputs for chaos experiments.

### Payments Resilience Slice

- Implement Payments authorization API.
- Integrate with a provider sandbox/test API such as Stripe test mode or PayPal Sandbox.
- Keep provider-specific code behind a narrow Payments adapter.
- Add idempotency records so retrying the same authorization does not double-charge.
- Define payment outcomes for success, decline, duplicate request, and dependency failure.
- Configure Resilience4j policies used by Orders clients:
  - timeout;
  - bounded retry with jitter;
  - circuit breaker;
  - bulkhead.
- Own Payments container/build concerns and deployment values/manifests.
- Define payment provider sandbox secret placeholders and local test credentials configuration.
- Provide latency-injection proof inputs for chaos experiments.

### Customer Experience + Observability Slice

- Implement a minimal React frontend for the customer flow:
  - route/offer browsing;
  - checkout/order creation;
  - order status polling or refresh;
  - clear success/failure states.
- Implement Catalog read APIs and own Catalog blue/green proof.
- Implement Notifications consumer.
- Expose authenticated notification history with `GET /api/notifications` and live frontend updates with SSE at `GET /api/notifications/stream`.
- Add graceful degradation behavior: checkout must succeed even when Notifications is down.
- Define correlation ID logging conventions.
- Add Micrometer metrics and structured logs for all service owners to copy.
- Own frontend container/build concerns and deployment values/manifests.
- Own Catalog and Notifications container/build concerns and deployment values/manifests.
- Define Kafka topic configuration needs for the configuration repository.
- Start dashboard, tracing, and alert templates.

## Phase 2: Money-Path Integration

The team integrates the checkout flow end to end.

Integration target:

1. Customer uses the React frontend to browse catalog data and start checkout.
2. The frontend calls `POST /api/orders` with authentication, correlation ID, and idempotency key handling.
3. Orders creates or reuses an idempotent order.
4. Orders reserves inventory.
5. Orders authorizes payment.
6. Orders confirms or fails the order with clear compensation behavior.
7. The frontend can show order status by calling `GET /api/orders/{orderId}`.
8. Kafka carries confirmation/notification events.
9. Notifications sends or records a confirmation without blocking checkout.

Required checks:

- Duplicate `POST /api/orders` with the same `Idempotency-Key` returns the same logical result.
- Duplicate Inventory reservation does not double-reserve.
- Duplicate Payment authorization does not double-charge.
- Unauthorized order reads return `403` or `404` according to the agreed policy.
- Payments failure releases or marks inventory reservation according to the agreed compensation flow.
- The frontend can demonstrate the happy path, an order failure state, and unauthorized access handling without bypassing the real APIs.

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

- Demonstrate that final cluster/demo proof uses real deployed services, Keycloak OIDC, Kafka, PostgreSQL, and the configured payment provider sandbox/test API rather than mocks.
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
  - React frontend customer flow;
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

Every role has the same baseline done criteria:

- Owned service code is implemented and tested.
- Owned image/build configuration is present.
- Owned Kubernetes/GitOps configuration is ready or explicitly specified for the configuration repository.
- Health endpoints, probes, resource assumptions, and environment variables are documented.
- Logs and metrics include correlation IDs where request or event flow is involved.
- The owner can demonstrate the slice locally and explain how it runs in the cluster.

### Technical Coordinator + Gateway/Security Slice

- CI builds all modules and runs tests.
- Local infrastructure starts with Docker Compose.
- Gateway exposes only intended public APIs.
- Protected APIs reject unauthenticated requests.
- Internal-only APIs are not externally routable.
- Secret handling is documented and no secrets are committed.
- Contract changes are reviewed before dependent implementation PRs merge.

### Orders Checkout Slice

- Orders APIs are implemented.
- Order ownership is enforced.
- Checkout orchestration works with real Inventory and Payments.
- Order idempotency is tested.
- Order events are emitted with correlation IDs.
- Orders deployment configuration includes probes, resources, env vars, and rollout strategy.
- Orders canary proof is prepared with success-rate and latency guardrails.

### Inventory Consistency Slice

- Inventory reservations are transactional.
- Concurrent reservation tests prove no oversell.
- Reservation idempotency is tested.
- Compensation endpoint releases reservations safely.
- Inventory events are emitted with correlation IDs.
- Inventory deployment/reservation-store configuration is ready for the configuration repository.
- Inventory pod-kill and database-failover proof expectations are documented.

### Payments Resilience Slice

- Payments authorization is idempotent.
- Payments uses a provider sandbox/test API through an adapter, not a pure internal mock.
- Duplicate retries do not double-charge.
- Orders clients use timeout, retry, circuit breaker, and bulkhead policies.
- Failure behavior is documented and tested.
- Payments deployment configuration includes probes, resources, env vars, and provider sandbox secret placeholders.
- Payments latency-injection proof expectations are documented.

### Customer Experience + Observability Slice

- React frontend is implemented for catalog browsing, checkout submission, and order status display.
- Frontend calls real gateway APIs and does not bypass authentication, idempotency, or authorization behavior.
- Frontend image/build and deployment configuration are ready for the configuration repository.
- Catalog read APIs are implemented and ready for blue/green proof.
- Notifications consumes confirmation events.
- Checkout succeeds when Notifications is unavailable.
- Logs, metrics, and traces include correlation IDs.
- Dashboards and alerts cover the checkout path.
- Kafka topic needs are documented for the configuration repository.
- Chaos experiments are documented with hypotheses and conclusions.
