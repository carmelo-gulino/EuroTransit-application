# Local Authentication Guide (Keycloak)

This document explains how local authentication works in our Docker Compose environment. We use Keycloak in development mode to simulate our final OIDC environment, ensuring all JWT claims, scopes, and roles are properly tested before reaching the cluster.

## Setup

Keycloak is automatically started when you run `docker-compose up` and is available at:
- **Admin Console**: `http://localhost:8081` (Username: `admin`, Password: `admin`)
- **OIDC Issuer URL**: `http://localhost:8081/realms/eurotransit`

The `eurotransit` realm is automatically imported on startup.

## Available Test Users

| Username | Password | Role | Purpose |
|----------|----------|------|---------|
| `alice`  | `alice`  | `customer` | Use this user to test checkout and order placement. |
| `admin`  | `admin`  | `operations` | Use this user to test operational tasks and reading all orders. |

## How to Get a Token for Local Testing

To call protected endpoints on the Gateway or Services during local development, you need a Bearer token.

### Using cURL

You can get a token for the user `alice` using the `eurotransit-frontend` public client:

```bash
export TOKEN=$(curl -s -X POST http://localhost:8081/realms/eurotransit/protocol/openid-connect/token \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "username=alice" \
  -d "password=alice" \
  -d "grant_type=password" \
  -d "client_id=eurotransit-frontend" | jq -r .access_token)

echo $TOKEN
```

### Using Postman / Insomnia

Create a new OAuth 2.0 authorization helper:
- **Grant Type**: Password Credentials
- **Access Token URL**: `http://localhost:8081/realms/eurotransit/protocol/openid-connect/token`
- **Client ID**: `eurotransit-frontend`
- **Username**: `alice`
- **Password**: `alice`

## Integrating with Spring Boot

In your local `application-local.yml`, configure your OAuth2 resource server to use the Keycloak issuer:

```yaml
spring:
  security:
    oauth2:
      resourceserver:
        jwt:
          issuer-uri: http://localhost:8081/realms/eurotransit
```
