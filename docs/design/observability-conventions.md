# Observability Conventions

These conventions make every EuroTransit service observable the same way: one correlation id per request flow, structured JSON logs, RED metrics with histogram buckets, OTLP traces, and Kubernetes-ready health probes. Catalog is the reference implementation; the shared code lives in the `:observability` Gradle module.

## Adoption Checklist (3 actions per service)

1. Add the shared module to your service build file:

   ```kotlin
   dependencies {
       implementation(project(":observability"))
   }
   ```

   Because the module shares the `it.polito.cpo` root package, its components are picked up by component scan automatically. **This activates the correlation-id filter and the global error handler in your service** — your error responses switch to the shared `{code, message, correlationId}` model.

2. Copy the configuration block below into your `application.yaml`, changing nothing but what is service-specific (`spring.application.name` and `server.port` stay as you already have them):

   ```yaml
   spring:
     reactor:
       context-propagation: auto   # correlationId from Reactor context into logging MDC

   logging:
     structured:
       format:
         console: ecs              # JSON logs; includes MDC and trace/span ids

   management:
     endpoints:
       web:
         exposure:
           include: health,info,prometheus,metrics
     endpoint:
       health:
         probes:
           enabled: true           # /actuator/health/readiness and /liveness
     metrics:
       tags:
         application: ${spring.application.name}
       distribution:
         percentiles-histogram:
           http.server.requests: true   # buckets for p95/p99 SLO queries
     tracing:
       sampling:
         probability: 1.0
     opentelemetry:
       tracing:
         export:
           otlp:
             endpoint: http://localhost:4318/v1/traces
     otlp:
       metrics:
         export:
           enabled: false
   ```

   In tests, put overrides in `src/test/resources/application-test.yaml` (NOT `application.yaml`, which would shadow your main config on the test classpath) and annotate test classes with `@ActiveProfiles("test")`. See `catalog/src/test/resources/application-test.yaml`.

3. Verify (with `docker compose up -d prometheus grafana tempo` and your service running):

   ```bash
   curl -i localhost:<port>/<any-endpoint>                 # response carries X-Correlation-Id
   curl -i -H "X-Correlation-Id: demo-123" localhost:<port>/<any-endpoint>   # echoed back; visible in the JSON log line
   curl localhost:<port>/actuator/health/readiness         # {"status":"UP"}
   curl localhost:<port>/actuator/health/liveness          # {"status":"UP"}
   curl -s localhost:<port>/actuator/prometheus | grep http_server_requests
   ```

## Correlation ID Lifecycle

- Header: `X-Correlation-Id`. The gateway generates it for public inbound requests when missing; every service also accepts-or-generates (`CorrelationIdWebFilter`), so services are safe to call directly in local development.
- The filter stores the id on the response header, the exchange attribute `CorrelationId.EXCHANGE_ATTRIBUTE`, and the Reactor context key `correlationId`, which is bridged into the SLF4J MDC — every log line inside the request chain carries it automatically.
- Outgoing `WebClient` calls built from the auto-configured `WebClient.Builder` propagate `X-Correlation-Id` and add `X-Service-Name` (your `spring.application.name`) via `CorrelationWebClientCustomizer`. Do not construct `WebClient.create(...)` directly — inject the builder.
- Kafka events must carry `correlationId` in the event envelope (frozen contract in `docs/design/api-design.md`); the producing service copies it from the request context.

## Structured Logging

Boot-native ECS JSON on stdout. Example line (abridged):

```json
{"@timestamp":"2026-07-06T15:40:12.345Z","log.level":"INFO","message":"Request rejected: ROUTE_NOT_FOUND Route nope does not exist","correlationId":"demo-123","trace.id":"6f9a…","span.id":"12ab…","service.name":"catalog","log.logger":"it.polito.cpo.observability.GlobalErrorHandler"}
```

Must NOT be logged, ever: bearer tokens, `Authorization` header values, secrets, passwords, payment card data, provider API keys, full personal data. Money-path audit logging (correlation id, principal id, order id, outcome) is a separate deliverable and must respect the same exclusions.

If JSON logs bother you during local development, disable them in your personal run configuration (`SPRING_APPLICATION_JSON` or a local profile) — do not remove the setting from `application.yaml`.

## Metrics (RED)

- Source: Spring's built-in `http_server_requests_seconds_*` (count/sum/buckets), tagged with `application`, `uri`, `status`, `outcome`.
- Rate: `sum(rate(http_server_requests_seconds_count{application="catalog"}[5m]))`
- Errors: `sum(rate(http_server_requests_seconds_count{application="catalog", status=~"5.."}[5m]))`
- Duration p95: `histogram_quantile(0.95, sum by (le) (rate(http_server_requests_seconds_bucket{application="orders", uri="/api/orders"}[5m])))`

These queries are the building blocks for the checkout SLOs in `docs/operations/slo-observability.md`. Custom business metrics (hold attempts, duplicate idempotency hits, circuit-breaker state) come later per that document; register them through the injected `MeterRegistry`.

## Tracing

- Export: OTLP HTTP to Tempo (`http://localhost:4318/v1/traces` locally; the cluster endpoint will be set per environment in the configuration repository).
- Property names are the Spring Boot 4 canonical ones (`management.opentelemetry.tracing.export.otlp.endpoint`); the Boot 3 `management.otlp.tracing.*` names are deprecated aliases.
- Sampling is 1.0 for development; production sampling is decided with the SLO owner before the demo.
- View traces in Grafana (`localhost:3000` → Explore → Tempo → search `service.name`).

## Health Probes

- `management.endpoint.health.probes.enabled: true` exposes `/actuator/health/readiness` and `/actuator/health/liveness`; these are now configured in `application.yaml`, so the env vars in `k8s/smoke/eurotransit-smoke.yaml` are redundant (kept for now, harmless).
- Rule from the resilience plan: liveness must never depend on downstream services (no DB/Kafka checks in liveness); readiness may.

## Error Model

Throw `ApiException(status, code, message)` from `it.polito.cpo.observability` for business failures; `GlobalErrorHandler` renders the frozen error body and logs it with the correlation id. Codes are SCREAMING_SNAKE_CASE (`ROUTE_NOT_FOUND`, `IDEMPOTENCY_CONFLICT`, …) per `docs/design/api-design.md`.

## Local Observability Stack

`docker compose up -d prometheus grafana tempo`:

| Tool | URL | Purpose |
| --- | --- | --- |
| Prometheus | http://localhost:9090 | scrapes `/actuator/prometheus` on ports 8081–8085 every 5s |
| Grafana | http://localhost:3000 | anonymous admin; Prometheus + Tempo pre-provisioned |
| Tempo | http://localhost:3200 | OTLP trace ingest on 4318 |

Prometheus reaches services running on your host via `host.docker.internal` (works on macOS/Windows; on Linux it is mapped through `extra_hosts: host-gateway`).
