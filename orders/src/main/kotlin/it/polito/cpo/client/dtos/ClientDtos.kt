package it.polito.cpo.client.dtos

import java.math.BigDecimal
import java.time.LocalDateTime
import java.util.UUID

data class ReservationRequest(
    val orderId: UUID,
    val seats: List<String>,
    val routeId: String
)

data class ReservationResponse(
    val reservationId: String,
    val status: String, // HELD, FAILED
    val expiresAt: LocalDateTime
)

data class PaymentRequest(
    val orderId: UUID,
    val amount: BigDecimal,
    val idempotencyKey: String
)

data class PaymentResponse(
    val paymentId: String,
    val status: String, // AUTHORIZED, DECLINED
    val authorizedAt: LocalDateTime
)
