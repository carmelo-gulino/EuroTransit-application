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
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RestController
import reactor.core.publisher.Mono
import java.time.Instant

@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
    properties = ["management.endpoint.health.probes.enabled=true"],
)
@AutoConfigureWebTestClient
@Import(PaymentsSecurityTestConfiguration::class)
class PaymentsSecurityConfigTests @Autowired constructor(
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
    fun `payment APIs reject anonymous requests`() {
        webTestClient.post()
            .uri("/api/payments/authorize")
            .exchange()
            .expectStatus().isUnauthorized
    }

    @Test
    fun `payment APIs require service role`() {
        webTestClient.post()
            .uri("/api/payments/authorize")
            .headers { it.setBearerAuth("service-token") }
            .exchange()
            .expectStatus().isOk
    }

    @Test
    fun `payment APIs reject customer tokens`() {
        webTestClient.post()
            .uri("/api/payments/authorize")
            .headers { it.setBearerAuth("customer-token") }
            .exchange()
            .expectStatus().isForbidden
    }
}

@TestConfiguration
class PaymentsSecurityTestConfiguration {
    @Bean
    @Primary
    fun testJwtDecoder(): ReactiveJwtDecoder =
        ReactiveJwtDecoder { token -> Mono.just(testJwt(token)) }

    @Bean
    fun paymentsSecurityProbeController(): PaymentsSecurityProbeController =
        PaymentsSecurityProbeController()
}

@RestController
class PaymentsSecurityProbeController {
    @PostMapping("/api/payments/authorize")
    fun authorize(): Map<String, String> = mapOf("status" to "authorized")
}

private fun testJwt(token: String): Jwt {
    val roles = when (token) {
        "customer-token" -> listOf("customer")
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
