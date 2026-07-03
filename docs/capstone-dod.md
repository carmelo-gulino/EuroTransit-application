# Definition of Done: EuroTransit Marketplace

This DoD is the operational release gate for the capstone. Each item must be backed by code, configuration, screenshots/logs, dashboard links, or written experiment results.

## 1. Design and Async

- [ ] Service boundaries and sync/async communication choices are documented in `design/api-design.md`.
- [ ] Architecture decisions are documented in `design/architecture-decisions.md`, including rejected alternatives and trade-offs.
- [ ] Orders implements an asynchronous pipeline with Kotlin coroutines/flows and Kafka-visible stages.
- [ ] Shutdown behavior is demonstrated: readiness refuses new traffic, in-flight work drains or cancels cooperatively, and no duplicate processing occurs after restart.
- [ ] The async design explains where suspending work reduces cost/scaling pressure and where it does not help CPU-bound work.

## 2. Consistency and Idempotency

- [ ] Inventory uses the selected Strong CP / PC-EC consistency model.
- [ ] The "never oversell" invariant is implemented with atomic 10-minute seat holds or an equivalent reservation state machine.
- [ ] Payment is attempted only after Inventory creates a valid hold.
- [ ] Holds expire after 10 minutes or are released when payment fails/cancels.
- [ ] Orders, Inventory, and Payments persist idempotency keys and return stable outcomes for duplicate requests/events.
- [ ] Duplicate `order-placed` events and retried payment authorizations cannot double-reserve or double-charge.

## 3. Resilience Engineering

- [ ] Cross-service synchronous calls use Resilience4j circuit breakers with documented open/half-open settings and safe fallback behavior.
- [ ] Bulkheads isolate checkout resources from unrelated traffic.
- [ ] Every remote call has timeout and bounded retry policy with backoff and jitter.
- [ ] Overload is handled with controlled refusal or load shedding rather than unbounded queues.
- [ ] Notifications can be killed while checkout still succeeds after reservation and payment.
- [ ] Kubernetes startup/readiness/liveness probes and PodDisruptionBudgets are deliberately configured.
- [ ] Liveness probes do not fail because of transient downstream dependency failures.

## 4. Delivery, Observability, and Proof

- [ ] Two repositories are populated: application and configuration.
- [ ] CI builds/tests/publishes immutable images and updates the configuration repository.
- [ ] ArgoCD reconciles the cluster state; CI holds no cluster credentials.
- [ ] Orders canary is demonstrated with metric-gated promote/abort.
- [ ] Catalog blue/green is demonstrated with fast rollback.
- [ ] The team explains why rolling and all-at-once are not used for the critical path.
- [ ] Checkout latency and success-rate SLOs are defined in `operations/slo-observability.md`.
- [ ] RED dashboards, infrastructure USE/Golden Signals dashboards, and symptom-based alerts are live.
- [ ] A single order can be traced through gateway, Orders, Inventory, Payments, and Kafka stages.

## 5. Chaos Experiments

- [ ] The chaos plan in `operations/chaos-experiments.md` is complete.
- [ ] Latency injection into Payments proves the Orders circuit breaker opens and Catalog remains healthy.
- [ ] Inventory pod kill mid-hold proves idempotency and no oversell/double-charge.
- [ ] Node or AZ-style disruption proves PDBs/topology spread protect the critical path.
- [ ] Kafka disruption proves the async pipeline recovers without lost or duplicated business effects.
- [ ] CloudNativePG primary failover records checkout impact and observed RTO.
- [ ] Each report includes hypothesis, steady state, injected fault, observations, conclusion, and follow-up change.

## 6. Agentic Coding and Governance

- [ ] `governance/agent-governance.md` documents credentials, permissions, blast radius, review gates, and worst-case failure.
- [ ] `agent-log.md` contains at least three concrete agent mistakes and how the team detected and corrected them.
- [ ] Agent-generated delivery artifacts require human review or policy-as-code before merge to the configuration repository.

## 7. Final Deliverables

- [ ] `docs/` contains design/consistency justification, SLO definitions, chaos reports, postmortem, agent threat model, and agent log.
- [ ] A blameless postmortem is completed using `templates/postmortem-template.md`.
- [ ] A 5-minute recorded demo link is committed.
- [ ] The demo shows the running system, dashboards, canary, blue/green, one injected failure, and one alert firing.
