# Blameless Postmortem Template

## Incident Summary

- **Date/time:**
- **Injected failure:**
- **User-visible impact:**
- **Detected by:**
- **Duration:**

## Steady State Before Injection

Describe the SLO/SLI values and dashboard signals before the failure.

## Timeline

| Time | Event |
| --- | --- |
| T+0 | Failure injected. |
| T+ | Alert fired. |
| T+ | Diagnosis made. |
| T+ | Mitigation applied. |
| T+ | System recovered. |

## What Happened

Explain the technical sequence using traces, logs, metrics, and Kubernetes events.

## What Worked

List controls that contained the failure, such as circuit breakers, readiness, retries, PDBs, or idempotency.

## What Failed or Surprised Us

List incorrect assumptions, missing dashboards, noisy alerts, unsafe defaults, or agent-generated mistakes.

## Follow-Up Actions

| Action | Owner | Due Date | Status |
| --- | --- | --- | --- |
|  |  |  |  |

## Lessons Learned

Document what the team will change in design, implementation, delivery, observability, or agent governance.
