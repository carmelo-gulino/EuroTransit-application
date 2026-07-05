package it.polito.cpo.client

import it.polito.cpo.client.dtos.PaymentRequest
import it.polito.cpo.client.dtos.PaymentResponse

interface PaymentClient {
    suspend fun authorizePayment(request: PaymentRequest, idempotencyKey: String): PaymentResponse
}
