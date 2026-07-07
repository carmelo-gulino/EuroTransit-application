package it.polito.cpo.client

import it.polito.cpo.contracts.payments.PaymentRequest
import it.polito.cpo.contracts.payments.PaymentResponse
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Profile
import org.springframework.http.MediaType
import org.springframework.stereotype.Component
import org.springframework.web.reactive.function.client.WebClient
import org.springframework.web.reactive.function.client.awaitBody

@Component
@Profile("!dev")
class WebClientPaymentClient(
    @Value("\${eurotransit.services.payments.url}") private val paymentsUrl: String
) : PaymentClient {

    private val webClient = WebClient.builder().baseUrl(paymentsUrl).build()

    override suspend fun authorizePayment(request: PaymentRequest, idempotencyKey: String): PaymentResponse {
        return webClient.post()
            .uri("/api/payments/authorize")
            .contentType(MediaType.APPLICATION_JSON)
            .header("Idempotency-Key", idempotencyKey)
            .bodyValue(request)
            .retrieve()
            .awaitBody()
    }
}
