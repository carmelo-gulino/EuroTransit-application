package it.polito.cpo.client

import it.polito.cpo.contracts.payments.PaymentRequest
import it.polito.cpo.contracts.payments.PaymentResponse
import it.polito.cpo.contracts.payments.PaymentCaptureRequest

interface PaymentClient {
    suspend fun authorizePayment(request: PaymentRequest, idempotencyKey: String, correlationId: String): PaymentResponse
    suspend fun capturePayment(request: PaymentCaptureRequest, idempotencyKey: String, correlationId: String): PaymentResponse
}
