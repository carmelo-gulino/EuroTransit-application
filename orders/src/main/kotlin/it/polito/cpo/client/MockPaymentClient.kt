package it.polito.cpo.client

import it.polito.cpo.contracts.payments.PaymentRefundResponse
import it.polito.cpo.contracts.payments.PaymentRequest
import it.polito.cpo.contracts.payments.PaymentResponse
import it.polito.cpo.contracts.payments.PaymentStatus
import it.polito.cpo.contracts.payments.RefundStatus
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Component
import java.util.UUID

@Component
@Profile("dev")
class MockPaymentClient : PaymentClient {
    override suspend fun authorizePayment(request: PaymentRequest, idempotencyKey: String, correlationId: String): PaymentResponse {
        return PaymentResponse(
            status = PaymentStatus.AUTHORIZED,
            providerReference = "mock_auth_id",
            errorCode = null
        )
    }

    override suspend fun refund(orderId: UUID, idempotencyKey: String): PaymentRefundResponse {
        return PaymentRefundResponse(
            status = RefundStatus.REFUNDED,
            refundReference = "mock_refund_id",
            errorCode = null
        )
    }
}
