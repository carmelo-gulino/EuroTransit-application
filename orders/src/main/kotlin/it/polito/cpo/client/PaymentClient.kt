package it.polito.cpo.client

import it.polito.cpo.contracts.payments.PaymentRequest
import it.polito.cpo.contracts.payments.PaymentResponse

interface PaymentClient {
    suspend fun authorizePayment(request: PaymentRequest, idempotencyKey: String): PaymentResponse
}
