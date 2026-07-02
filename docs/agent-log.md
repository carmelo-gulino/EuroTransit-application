# Agent Mistake Log

This log is intentionally required by the capstone. It records concrete cases where an agent-generated artifact was wrong, unsafe, or incomplete, and how the team corrected it.

## Case 1: Liveness Probe Checked Downstream Dependency

- **Agent output:** A liveness probe failed when Kafka or Postgres was unavailable.
- **Why it was wrong:** Liveness should indicate whether the process is stuck, not whether a dependency is transiently down. This could cause restart loops during dependency incidents.
- **How the team caught it:** Review against the resilience requirements and probe design checklist.
- **Correction:** Move dependency checks to readiness or custom health indicators. Keep liveness local to process health.

## Case 2: Cause-Based CPU Alert Instead of User Symptom Alert

- **Agent output:** An alert fired on high CPU usage for Orders.
- **Why it was wrong:** The assignment requires symptom-based alerts tied to SLOs. CPU alone does not prove user-visible failure.
- **How the team caught it:** SLO review against `slo-observability.md`.
- **Correction:** Replace with checkout success-rate burn and checkout latency alerts. Keep CPU on dashboards for diagnosis.

## Case 3: Non-Idempotent Kafka Consumer

- **Agent output:** A consumer applied reservation/payment side effects every time an event was delivered.
- **Why it was wrong:** Kafka delivery may duplicate messages. The money path must not double-reserve or double-charge.
- **How the team caught it:** Chaos experiment design for duplicate events and pod kill mid-reservation.
- **Correction:** Add idempotency records keyed by order/reservation/payment attempt and return the original result for duplicate deliveries.

## Case 4: Over-Permissive Delivery ServiceAccount

- **Agent output:** A generated ServiceAccount had broad namespace write privileges.
- **Why it was wrong:** The agent is inside the delivery loop and must have limited blast radius.
- **How the team caught it:** RBAC review in `agent-governance.md`.
- **Correction:** Scope permissions to the minimum resources required and require human review before config repo merge.
