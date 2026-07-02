# Chaos Experiment Plan

Each experiment must be run as a hypothesis-driven test. Reports should keep the same structure: steady state, hypothesis, injected failure, observations, conclusion, and follow-up change.

## Experiment 1: Payments Latency Injection

- **Steady state:** Checkout success rate is within SLO, p95 checkout latency is below 2 seconds, Catalog requests are healthy.
- **Hypothesis:** If Payments latency increases, Orders opens the payment circuit breaker, returns controlled checkout failures or queued/degraded status, and Catalog remains unaffected.
- **Injection:** Add latency to Payments traffic using Chaos Mesh or equivalent.
- **Observe:** Orders circuit-breaker state, checkout latency/error rate, Catalog RED metrics, traces showing Payments delay.
- **Success condition:** No unbounded hangs, no retry storm, Catalog remains inside SLO.

## Experiment 2: Inventory Pod Kill Mid-Reservation

- **Steady state:** Reservation success/failure metrics are stable and no oversell invariant violations are reported.
- **Hypothesis:** Killing Inventory during reservation does not oversell seats and does not double-charge payments because idempotency records and atomic reservation transitions are used.
- **Injection:** Kill an Inventory pod during concurrent checkout load.
- **Observe:** Duplicate idempotency hits, reservation state transitions, order terminal states, payment authorization count per order.
- **Success condition:** Each order has at most one reservation result and at most one payment authorization result.

## Experiment 3: Node or AZ-Style Disruption

- **Steady state:** Critical path pods are spread according to topology rules and PDBs allow controlled disruption.
- **Hypothesis:** A node/AZ-style disruption does not take down the entire critical path.
- **Injection:** Drain or disrupt one node/failure domain in the test cluster.
- **Observe:** Pod rescheduling, readiness, PDB behavior, checkout SLOs, infrastructure dashboard.
- **Success condition:** The system remains available or degrades within the stated SLO/error budget.

## Experiment 4: Kafka Disruption

- **Steady state:** Kafka consumer lag is near zero and order pipeline events converge to terminal status.
- **Hypothesis:** If Kafka is partitioned or temporarily unavailable, no business effects are lost or duplicated; the pipeline catches up after recovery.
- **Injection:** Partition or disrupt Kafka brokers/listeners using Chaos Mesh or equivalent.
- **Observe:** Producer errors, consumer lag, duplicate event handling, order terminal state convergence.
- **Success condition:** After healing, accepted orders either reach a valid terminal state or are explicitly marked failed without duplicate reservation/payment effects.

## Experiment 5: CloudNativePG Primary Failover

- **Steady state:** Inventory reservation writes and Orders state writes are healthy.
- **Hypothesis:** During PostgreSQL primary failover, checkout write operations may fail temporarily, but the system recovers within the stated RTO and never oversells.
- **Injection:** Trigger CloudNativePG primary failover.
- **Observe:** Checkout errors, database failover time, RTO, reservation invariant metrics, traces for failed writes.
- **Success condition:** No oversell occurs, and checkout recovers within the documented RTO.
