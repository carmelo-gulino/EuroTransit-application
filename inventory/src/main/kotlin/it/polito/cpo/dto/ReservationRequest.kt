package it.polito.cpo.dto

import java.util.UUID

data class ReservationRequest(
    val orderId: UUID,
    val seats: List<String>,
    val routeId: String
)
