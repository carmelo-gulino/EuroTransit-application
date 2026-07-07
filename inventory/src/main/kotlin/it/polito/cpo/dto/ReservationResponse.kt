package it.polito.cpo.dto

import java.time.LocalDateTime

data class ReservationResponse(
    val reservationId: String,
    val status: String, // HELD, FAILED
    val expiresAt: LocalDateTime
)
