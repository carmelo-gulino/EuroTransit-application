package it.polito.cpo.contracts.payments

import java.math.BigDecimal
import java.time.LocalDateTime
import java.util.UUID

// orders -> payments : POST /api/payments/authorize (Idempotency-Key header)
data class PaymentRequest(
    val orderId: UUID,
    val principalId: String,
    val amount: BigDecimal,
    val currency: String,
    val paymentMethodToken: String
)

enum class PaymentStatus { AUTHORIZED, DECLINED, CONFLICT, DEPENDENCY_FAILED }

data class PaymentResponse(
    val status: PaymentStatus,
    val providerReference: String?,
    val errorCode: String? = null
)
