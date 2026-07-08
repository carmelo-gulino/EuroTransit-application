package it.polito.cpo.client

import io.github.resilience4j.bulkhead.BulkheadRegistry
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry
import io.github.resilience4j.reactor.bulkhead.operator.BulkheadOperator
import io.github.resilience4j.reactor.circuitbreaker.operator.CircuitBreakerOperator
import io.github.resilience4j.reactor.retry.RetryOperator
import io.github.resilience4j.retry.RetryRegistry
import it.polito.cpo.contracts.inventory.ReservationRequest
import it.polito.cpo.contracts.inventory.ReservationResponse
import it.polito.cpo.observability.CorrelationWebClientCustomizer
import kotlinx.coroutines.reactor.awaitSingle
import kotlinx.coroutines.reactor.awaitSingleOrNull
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Profile
import org.springframework.http.MediaType
import org.springframework.stereotype.Component
import org.springframework.web.reactive.function.client.WebClient
import java.time.Duration

@Component
@Profile("!dev")
class WebClientInventoryClient(
    @Value("\${eurotransit.services.inventory.url}") private val inventoryUrl: String,
    correlationCustomizer: CorrelationWebClientCustomizer,
    circuitBreakerRegistry: CircuitBreakerRegistry,
    retryRegistry: RetryRegistry,
    bulkheadRegistry: BulkheadRegistry,
) : InventoryClient {

    // Apply the shared customizer so every call carries X-Correlation-Id (from the Reactor
    // context) and X-Service-Name, per the frozen internal-call contract.
    private val webClient = WebClient.builder()
        .baseUrl(inventoryUrl)
        .apply(correlationCustomizer::customize)
        .build()

    private val circuitBreaker = circuitBreakerRegistry.circuitBreaker("inventory")
    private val retry = retryRegistry.retry("inventory")
    private val bulkhead = bulkheadRegistry.bulkhead("inventory")
    // Inventory is the consistency boundary: keep the timeout short.
    private val timeout = Duration.ofSeconds(2)

    override suspend fun reserveSeats(request: ReservationRequest, idempotencyKey: String): ReservationResponse {
        return webClient.post()
            .uri("/api/inventory/reservations")
            .contentType(MediaType.APPLICATION_JSON)
            .header("Idempotency-Key", idempotencyKey)
            .bodyValue(request)
            .retrieve()
            .bodyToMono(ReservationResponse::class.java)
            .timeout(timeout)
            .transformDeferred(BulkheadOperator.of(bulkhead))
            .transformDeferred(CircuitBreakerOperator.of(circuitBreaker))
            .transformDeferred(RetryOperator.of(retry))
            .awaitSingle()
    }

    override suspend fun releaseSeats(reservationId: String, idempotencyKey: String) {
        webClient.delete()
            .uri("/api/inventory/reservations/{reservationId}", reservationId)
            .header("Idempotency-Key", idempotencyKey)
            .retrieve()
            .bodyToMono(Void::class.java)
            .timeout(timeout)
            .transformDeferred(BulkheadOperator.of(bulkhead))
            .transformDeferred(CircuitBreakerOperator.of(circuitBreaker))
            .transformDeferred(RetryOperator.of(retry))
            .awaitSingleOrNull()
    }
}
