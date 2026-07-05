package it.polito.cpo.client

import it.polito.cpo.client.dtos.ReservationRequest
import it.polito.cpo.client.dtos.ReservationResponse

interface InventoryClient {
    suspend fun reserveSeats(request: ReservationRequest, idempotencyKey: String): ReservationResponse
    suspend fun releaseSeats(reservationId: String, idempotencyKey: String)
}
