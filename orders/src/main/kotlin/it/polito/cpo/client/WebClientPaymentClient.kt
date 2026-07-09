package it.polito.cpo.client

import io.github.resilience4j.bulkhead.BulkheadRegistry
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry
import io.github.resilience4j.reactor.bulkhead.operator.BulkheadOperator
import io.github.resilience4j.reactor.circuitbreaker.operator.CircuitBreakerOperator
import io.github.resilience4j.reactor.retry.RetryOperator
import io.github.resilience4j.retry.RetryRegistry
import it.polito.cpo.contracts.payments.PaymentRequest
import it.polito.cpo.contracts.payments.PaymentResponse
import kotlinx.coroutines.reactor.awaitSingle
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Profile
import org.springframework.http.MediaType
import org.springframework.security.oauth2.client.web.reactive.function.client.ServerOAuth2AuthorizedClientExchangeFilterFunction
import org.springframework.stereotype.Component
import org.springframework.web.reactive.function.client.WebClient
import java.time.Duration

@Component
@Profile("!dev")
class WebClientPaymentClient(
    @Value("\${eurotransit.services.payments.url}") private val paymentsUrl: String,
    webClientBuilder: WebClient.Builder,
    serviceAuthExchangeFilter: ServerOAuth2AuthorizedClientExchangeFilterFunction,
    circuitBreakerRegistry: CircuitBreakerRegistry,
    retryRegistry: RetryRegistry,
    bulkheadRegistry: BulkheadRegistry,
) : PaymentClient {

    // Use the auto-configured builder: it already applies the CorrelationWebClientCustomizer
    // (X-Correlation-Id + X-Service-Name) and client-side metrics/tracing. We add only the
    // ROLE_service Bearer filter (client-credentials, for /api/payments/** security).
    private val webClient = webClientBuilder
        .baseUrl(paymentsUrl)
        .filter(serviceAuthExchangeFilter)
        .build()

    private val circuitBreaker = circuitBreakerRegistry.circuitBreaker("payments")
    private val retry = retryRegistry.retry("payments")
    private val bulkhead = bulkheadRegistry.bulkhead("payments")
    // Payments integrates with an external provider: allow a slightly longer timeout.
    private val timeout = Duration.ofSeconds(3)

    override suspend fun authorizePayment(request: PaymentRequest, idempotencyKey: String): PaymentResponse {
        return webClient.post()
            .uri("/api/payments/authorize")
            .contentType(MediaType.APPLICATION_JSON)
            .header("Idempotency-Key", idempotencyKey)
            .bodyValue(request)
            .retrieve()
            .bodyToMono(PaymentResponse::class.java)
            .timeout(timeout)
            .transformDeferred(BulkheadOperator.of(bulkhead))
            .transformDeferred(CircuitBreakerOperator.of(circuitBreaker))
            .transformDeferred(RetryOperator.of(retry))
            .awaitSingle()
    }
}
