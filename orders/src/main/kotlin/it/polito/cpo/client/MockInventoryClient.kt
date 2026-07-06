package it.polito.cpo.client

import it.polito.cpo.client.dtos.ReservationRequest
import it.polito.cpo.client.dtos.ReservationResponse
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Component
import java.time.LocalDateTime
import java.util.UUID

@Component
@Profile("dev")
class MockInventoryClient : InventoryClient {
    override suspend fun reserveSeats(request: ReservationRequest, idempotencyKey: String): ReservationResponse {
        return ReservationResponse(
            reservationId = UUID.randomUUID().toString(),
            status = "HELD",
            expiresAt = LocalDateTime.now().plusMinutes(10)
        )
    }

    override suspend fun releaseSeats(reservationId: String, idempotencyKey: String) {
        // Mock release - no operational work needed
    }
}
