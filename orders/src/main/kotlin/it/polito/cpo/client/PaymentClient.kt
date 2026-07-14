package it.polito.cpo.client

import it.polito.cpo.contracts.payments.PaymentRefundResponse
import it.polito.cpo.contracts.payments.PaymentRequest
import it.polito.cpo.contracts.payments.PaymentResponse
import java.util.UUID

interface PaymentClient {
    suspend fun authorizePayment(request: PaymentRequest, idempotencyKey: String, correlationId: String): PaymentResponse

    /**
     * Full refund of the authorization made for [orderId]. Payments resolves the provider reference
     * from its own authorization record, so orders passes only the order id. Idempotent on
     * [idempotencyKey].
     */
    suspend fun refund(orderId: UUID, idempotencyKey: String): PaymentRefundResponse
}
