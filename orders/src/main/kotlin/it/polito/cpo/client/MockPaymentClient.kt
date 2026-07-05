package it.polito.cpo.client

import it.polito.cpo.client.dtos.PaymentRequest
import it.polito.cpo.client.dtos.PaymentResponse
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Component
import java.time.LocalDateTime
import java.util.UUID

@Component
@Profile("dev")
class MockPaymentClient : PaymentClient {
    override suspend fun authorizePayment(request: PaymentRequest, idempotencyKey: String): PaymentResponse {
        return PaymentResponse(
            paymentId = UUID.randomUUID().toString(),
            status = "AUTHORIZED",
            authorizedAt = LocalDateTime.now()
        )
    }
}
