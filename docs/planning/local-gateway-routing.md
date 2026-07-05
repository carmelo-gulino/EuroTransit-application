# Local Gateway Routing

This document describes the local Docker Compose gateway boundary for EuroTransit Marketplace.

Traefik is the only local north-south gateway exposed on `localhost:80`. The dashboard is available on `localhost:8080` for local development only.

## Public Routes

- `/api/catalog/**` routes to the Catalog service.
- `/api/orders/**` routes to the Orders service.
- The React frontend route will be added when the frontend service exists.
- Notification read/SSE routes will be added when the customer-facing notification API exists.

## Internal-Only Services

Inventory, Payments, and the Notifications worker are not exposed through Traefik. Their Compose services use `traefik.enable=false` and expose only container-network ports for service-to-service calls.

## Authentication Boundary

The local Orders route attaches a Traefik middleware named `local-auth-boundary`. Today this middleware only marks the request with `X-EuroTransit-Auth-Boundary=service-enforced-local-keycloak`.

This is intentional:

- Keycloak local development is owned by the local auth setup.
- Services must still validate JWTs and enforce ownership locally.
- A future gateway-level OIDC/forward-auth middleware can replace or extend this marker once Keycloak and the chosen forward-auth component are integrated.

The marker must not be treated as authentication. It only documents that protected local routes are expected to be service-enforced until real gateway auth is wired.
