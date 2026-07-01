# Definition of Done: EuroTransit Marketplace

This document defines the operational criteria that must be met for the EuroTransit Capstone project to be considered completely finished and ready for presentation. 

### 1. Design & Async (Pillar A)
- [ ] Service boundaries are clearly mapped with synchronous vs. asynchronous boundaries explicitly justified.
- [ ] The `Orders` pipeline is implemented asynchronously using Kotlin Coroutines / Flows.
- [ ] The system demonstrates correct async lifecycle behavior: structured concurrency is used, and pods undergo cooperative cancellation on `SIGTERM` (draining in-flight work safely).

### 2. Consistency & Idempotency (Pillar B)
- [ ] A specific Consistency Model (e.g., PC/EC Strong Consistency) is chosen for the `Inventory` service, justified in writing, and implemented (e.g., via PostgreSQL conditional updates).
- [ ] Idempotency keys are implemented across the entire "money path" (Orders → Payments / Inventory) to guarantee no double-charging or double-reserving occurs during retries.

### 3. Resilience Engineering (Pillar C)
- [ ] **Circuit Breakers** (e.g., Resilience4j) are configured on cross-service synchronous calls with safe fallback strategies (not unbounded hangs).
- [ ] **Bulkheads** are implemented to isolate resource pools.
- [ ] **Timeouts and Retries** (with backoff and jitter) are enforced on every remote call.
- [ ] **Graceful Degradation** is proven (e.g., the `Notifications` service can be killed, and checkout still succeeds).
- [ ] Kubernetes resilience is deliberately configured (Startup/Readiness/Liveness probes, PodDisruptionBudgets).

### 4. Delivery, Observability & Proof (Pillar D)
- [ ] **GitOps Delivery:** CI builds immutable images and updates the config repo; ArgoCD reconciles the cluster state. CI holds no cluster credentials.
- [ ] **Progressive Delivery:** At least two strategies are fully demonstrated in Traefik/Argo (e.g., Canary for `Orders`, Blue/Green for `Catalog`).
- [ ] **Observability:** Per-service RED dashboards and infrastructure USE dashboards are live. 
- [ ] **SLOs & Alerts:** Latency and Success-Rate SLOs are defined for the critical checkout path, with symptom-based alerts tied to them.
- [ ] **Distributed Tracing:** A single order request can be traced across the money path (gateway → Orders → Inventory/Payments → Kafka).

### 5. Chaos Experiments
- [ ] Four chaos experiments are defined, run, and documented with hypotheses and conclusions:
  - [ ] Latency injection into Payments (verify Circuit Breaker opens).
  - [ ] Pod kill on Inventory mid-reservation (verify idempotency).
  - [ ] Network partition of Kafka (verify pipeline recovery).
  - [ ] Database primary failover (verify checkout RTO).

### 6. Agentic Coding & Governance
- [ ] The threat-model and blast radius of the AI agent within the delivery loop are documented.
- [ ] **`agent-log.md`** contains at least three documented cases where the AI agent generated incorrect, unsafe, or non-idempotent artifacts, along with how the team caught and fixed them.

### 7. Final Deliverables
- [ ] The two repositories (Application and Configuration) are populated and pushed.
- [ ] A 5-minute recorded demo is committed, showing the running system, dashboards, progressive delivery, and an alert firing under injected failure.
- [ ] A blameless postmortem for an injected incident is written.
