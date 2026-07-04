# Agentic Coding Governance

Agentic coding tools may generate artifacts, but the team owns the decisions and the operational outcome.

## Allowed Agent Work

- Service scaffolding.
- Helm/manifests and CI drafts.
- Dashboard and alert rule drafts.
- Test harnesses and chaos experiment templates.
- Documentation drafts.

## Decisions Agents Must Not Own

- Service decomposition.
- Inventory consistency model.
- SLO definitions.
- Failure-mode mapping.
- Chaos hypotheses and conclusions.
- Postmortem analysis.
- Security and delivery-loop governance.

## Permissions and Blast Radius

- Application repository access should be limited to pull-request creation.
- Configuration repository changes must require human review or policy-as-code checks before merge.
- Agents must not hold direct cluster credentials.
- Agents must not be able to bypass ArgoCD, SealedSecrets, or CI policy gates.

Worst case: an agent proposes a bad configuration change that ArgoCD would reconcile into the cluster. The control is mandatory review plus policy checks for privileged workloads, secrets, broad RBAC, unsafe probes, and destructive manifests.

## Required Review Checks

- Probes must not depend on transient downstream dependencies.
- Alerts must be symptom-based and tied to SLOs, not just CPU or cause metrics.
- Handlers that process Kafka events must be idempotent.
- ServiceAccounts and RBAC must use least privilege.
- Retry settings must be bounded and include backoff/jitter.
- Generated manifests must not include plaintext secrets.
