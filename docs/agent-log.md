# Agent Mistake Log

This log is intentionally required by the assignment. It records concrete cases where an agent-generated artifact was wrong, unsafe, or incomplete, and how the team corrected it.

## Case 1: Missing Versioned Database Migrations Strategy

- **Agent output:** A custom `R2dbcConfiguration` that ran a plain, unversioned `schema.sql` using Spring's `ConnectionFactoryInitializer`.
- **Why it was wrong:** A single idempotent `schema.sql` cannot handle ordered, incremental schema evolutions (e.g., `V1`, `V2`) needed for long-term production maintenance.
- **How the team caught it:** The developer realized that maintaining 10+ sequential migrations with a single file technique was unscalable.
- **Correction:** Dropped the custom R2DBC initializer and integrated embedded Flyway with the PostgreSQL JDBC driver. Externalized DB credentials via environment variables so both Flyway and R2DBC can connect seamlessly.