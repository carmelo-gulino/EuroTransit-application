package it.polito.cpo.client

import it.polito.cpo.contracts.payments.PaymentRequest
import it.polito.cpo.contracts.payments.PaymentResponse
import it.polito.cpo.contracts.payments.PaymentStatus
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Component
import java.time.LocalDateTime
import java.util.UUID

@Component
@Profile("dev")
class MockPaymentClient : PaymentClient {
    override suspend fun authorizePayment(request: PaymentRequest, idempotencyKey: String, correlationId: String): PaymentResponse {
        return PaymentResponse(
            status = it.polito.cpo.contracts.payments.PaymentStatus.AUTHORIZED,
            providerReference = "mock_auth_id",
            errorCode = null
        )
    }
}
