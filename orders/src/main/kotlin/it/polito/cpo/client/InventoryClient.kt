package it.polito.cpo.client

import it.polito.cpo.contracts.inventory.ReservationRequest
import it.polito.cpo.contracts.inventory.ReservationResponse

interface InventoryClient {
    suspend fun reserveSeats(request: ReservationRequest, idempotencyKey: String): ReservationResponse
    suspend fun releaseSeats(reservationId: String, idempotencyKey: String)
}
