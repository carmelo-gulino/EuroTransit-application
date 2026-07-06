package it.polito.cpo

import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.boot.webtestclient.autoconfigure.AutoConfigureWebTestClient
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Primary
import org.springframework.context.annotation.Import
import org.springframework.security.oauth2.jwt.Jwt
import org.springframework.security.oauth2.jwt.ReactiveJwtDecoder
import org.springframework.test.web.reactive.server.WebTestClient
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RestController
import reactor.core.publisher.Mono
import java.time.Instant

@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
    properties = [
        "management.endpoint.health.probes.enabled=true",
        "management.health.r2dbc.enabled=false",
    ],
)
@AutoConfigureWebTestClient
@Import(OrdersSecurityTestConfiguration::class)
class OrdersSecurityConfigTests @Autowired constructor(
    private val webTestClient: WebTestClient,
) {
    @Test
    fun `health endpoint is public`() {
        webTestClient.get()
            .uri("/actuator/health/liveness")
            .exchange()
            .expectStatus().isOk
    }

    @Test
    fun `order creation rejects anonymous requests`() {
        webTestClient.post()
            .uri("/api/orders")
            .exchange()
            .expectStatus().isUnauthorized
    }

    @Test
    fun `order creation accepts customer role`() {
        webTestClient.post()
            .uri("/api/orders")
            .headers { it.setBearerAuth("customer-token") }
            .exchange()
            .expectStatus().isOk
    }

    @Test
    fun `order reads accept operations role for future support access`() {
        webTestClient.get()
            .uri("/api/orders/order-123")
            .headers { it.setBearerAuth("operations-token") }
            .exchange()
            .expectStatus().isOk
    }

    @Test
    fun `order creation rejects tokens without the customer role`() {
        webTestClient.post()
            .uri("/api/orders")
            .headers { it.setBearerAuth("operations-token") }
            .exchange()
            .expectStatus().isForbidden
    }

    @Test
    fun `unlisted paths are denied even for authenticated users`() {
        webTestClient.get()
            .uri("/internal/debug")
            .headers { it.setBearerAuth("customer-token") }
            .exchange()
            .expectStatus().isForbidden
    }
}

@TestConfiguration
class OrdersSecurityTestConfiguration {
    @Bean
    @Primary
    fun testJwtDecoder(): ReactiveJwtDecoder =
        ReactiveJwtDecoder { token -> Mono.just(testJwt(token)) }

    @Bean
    fun ordersSecurityProbeController(): OrdersSecurityProbeController =
        OrdersSecurityProbeController()
}

@RestController
class OrdersSecurityProbeController {
    @PostMapping("/api/orders")
    fun createOrder(): Map<String, String> = mapOf("status" to "accepted")

    @GetMapping("/api/orders/{orderId}")
    fun readOrder(@PathVariable orderId: String): Map<String, String> = mapOf("orderId" to orderId)
}

private fun testJwt(token: String): Jwt {
    val roles = when (token) {
        "customer-token" -> listOf("customer")
        "operations-token" -> listOf("operations")
        "service-token" -> listOf("service")
        else -> emptyList()
    }
    val now = Instant.now()

    return Jwt.withTokenValue(token)
        .header("alg", "none")
        .subject(token.removeSuffix("-token"))
        .issuedAt(now)
        .expiresAt(now.plusSeconds(300))
        .claim("realm_access", mapOf("roles" to roles))
        .build()
}
