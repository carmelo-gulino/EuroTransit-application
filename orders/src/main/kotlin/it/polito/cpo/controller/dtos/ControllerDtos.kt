package it.polito.cpo.controller.dtos

import it.polito.cpo.model.OrderStatus
import java.math.BigDecimal
import java.time.LocalDateTime
import java.util.UUID

data class CheckoutRequest(
    val routeId: String,
    val seats: List<String>,
    val totalAmount: BigDecimal,
    val paymentMethodToken: String
)

data class CheckoutResponse(
    val orderId: UUID,
    val status: OrderStatus
)

data class OrderView(
    val orderId: UUID,
    val userId: String,
    val status: OrderStatus,
    val routeId: String,
    val seats: List<String>,
    val totalAmount: BigDecimal,
    val createdAt: LocalDateTime
)
