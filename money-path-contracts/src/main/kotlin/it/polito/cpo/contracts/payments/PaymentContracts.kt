package it.polito.cpo.contracts.payments

import java.math.BigDecimal
import java.time.LocalDateTime
import java.util.UUID

// orders -> payments : POST /api/payments/authorize (Idempotency-Key header)
data class PaymentRequest(
    val orderId: UUID,
    val amount: BigDecimal,
    val idempotencyKey: String
)

enum class PaymentStatus { AUTHORIZED, DECLINED }

data class PaymentResponse(
    val paymentId: String,
    val status: PaymentStatus,
    val authorizedAt: LocalDateTime
)
