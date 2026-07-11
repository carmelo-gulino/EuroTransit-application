package it.polito.cpo.client

import it.polito.cpo.contracts.inventory.ReservationRequest
import it.polito.cpo.contracts.inventory.ReservationResponse

interface InventoryClient {
    // userId is the end customer, propagated as X-User-Id so inventory records the real principal
    // (the service token identifies the caller as a service, not the customer).
    suspend fun reserveSeats(request: ReservationRequest, idempotencyKey: String, userId: String): ReservationResponse
    suspend fun releaseSeats(reservationId: String, idempotencyKey: String)
}
