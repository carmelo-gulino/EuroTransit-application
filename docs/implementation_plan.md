# EuroTransit Capstone Work Plan

This document outlines the technical design and team work plan for the **EuroTransit Marketplace** capstone project. The focus is on resilience, asynchronous execution, and observability under failure, delivered via a two-repository GitOps model.

## Resolved Decisions
- **Kotlin Framework:** Spring Boot (WebFlux)
- **CI Platform:** GitHub Actions
- **Resilience Tooling:** Resilience4j
- **Application Repository Layout:** Monorepo (Gradle Multi-module)
- **Deployment Strategy:** **Docker Compose** for local dev loops (spinning up DBs/Kafka/services locally). Kubernetes manifests/Helm charts in the **Configuration Repository** for cluster deployments via ArgoCD.
- **Graceful Degradation for Notifications:** Fully decoupled via Kafka. The checkout API returns `200 OK` as soon as Payments and Inventory are secured. The `order-confirmed` event waits in Kafka, allowing the checkout to succeed even if the Notifications service is completely down.

## Pending Team Review

> [!IMPORTANT]
> Please review the following strategies with your team and finalize your choices before moving into Phase 3 (Data & Resilience).

### 1. Consistency Model (CAP/PACELC) for Inventory
The assignment requires you to justify a consistency model for the inventory (to avoid overselling). Here are the two main strategies to discuss with your team:
- **Strategy A (Recommended): Strong Consistency (CP / PC/EC)**
  - *How it works:* Use PostgreSQL atomic updates (`UPDATE seats SET available = false WHERE id = X AND available = true`) or a reservation state machine.
  - *Trade-off:* We guarantee we **never oversell a seat**. However, if the CloudNativePG primary database fails over (e.g., a node dies), checkout writes will temporarily fail until the new primary is elected. We sacrifice Availability for strict Consistency.
- **Strategy B: Eventual Consistency (AP / PA/EL)**
  - *How it works:* Accept the reservation asynchronously via Kafka without checking the database synchronously. If two people buy the same seat, a Saga pattern is triggered later to refund one customer and send a cancellation email.
  - *Trade-off:* High availability (we always accept the order request), but we temporarily violate the core invariant and have to write complex compensating logic to fix it.

### 2. Progressive Delivery Strategies
The assignment requires demonstrating both **Canary** and **Blue/Green** deployments. 
- *Strategy Option 1 (Recommended Mapping):* 
  - **Orders (Canary):** It's the critical money path. We expose only a small fraction (e.g., 5%) of traffic to the new version using TraefikService, watch the SLOs, and then promote.
  - **Catalog (Blue/Green):** It is stateless and read-heavy. We can stand up the new version side-by-side and safely flip the route all at once.
- *Strategy Option 2 (Alternative Mapping):*
  - **Inventory (Canary):** The inventory reservation logic is complex and state-sensitive. We route a small portion of traffic (e.g., 5%) to a new inventory version to verify the state machine without risking mass double-booking.
  - **Payments (Blue/Green):** The payments API is highly isolated. We can deploy the new version, validate the external mock gateway connection internally, and cut over all traffic instantly.

---

## 1-Month Timeline

### Week 1: Foundation, GitOps & CI/CD
- **Goal:** Get the "walking skeleton" deployed all the way to Kubernetes.
- Scaffold the 5 Spring Boot WebFlux applications in a Gradle Multi-module Monorepo.
- Set up local `docker-compose.yml` for fast iteration.
- Set up the GitHub Actions CI pipeline to build images and push to the config repo.
- Deploy ArgoCD, Traefik, Strimzi (Kafka), and CloudNativePG to the cluster.
- **Team Focus:** Delivery Owner & Domain Owner

### Week 2: Core Domain & Asynchronous Execution
- **Goal:** Implement the "money path" (checkout flow) end-to-end.
- Develop the synchronous APIs (Catalog, Orders).
- Implement the Kafka-driven order pipeline using Kotlin Coroutines/Flows.
- Handle graceful shutdown (`SIGTERM`) and structured concurrency.
- **Team Focus:** Domain Owner & Data Owner

### Week 3: Resilience, Idempotency & Observability
- **Goal:** Harden the system against failure and instrument it.
- Implement the chosen inventory consistency model.
- Add idempotency keys to the Payments and Inventory flows.
- Implement Resilience4j circuit breakers, bulkheads, and timeouts on cross-service calls.
- Configure `kube-prometheus-stack` and build RED (Rate, Errors, Duration) dashboards.
- **Team Focus:** Resilience Owner & Observability Owner

### Week 4: Chaos Engineering, Polish & Presentation
- **Goal:** Prove the system's resilience and finalize deliverables.
- Write Chaos Mesh experiments (Latency injection, Pod kills, Network partitions).
- Ensure Graceful Degradation works under failure.
- Finalize `docs/capstone-dod.md`, `agent-log.md`, and the blameless postmortem.
- Record the 5-minute demo video.
- **Team Focus:** All Hands (Rotating Roles)

## Verification Plan

We will validate the system's resilience using **Chaos Mesh** in our testing environment:
1. **Latency Injection:** Inject latency into `Payments` and verify the `Orders` circuit breaker opens and falls back safely while `Catalog` remains unaffected.
2. **Pod Kill (Mid-Reservation):** Kill an `Inventory` pod mid-reservation to verify idempotency and consistency models prevent double-charging.
3. **Network Partition:** Isolate Kafka and verify the async pipeline recovers without losing or duplicating messages once healed.
4. **Database Failover:** Failover the CloudNativePG primary and observe the RTO and impact on the checkout flow.
