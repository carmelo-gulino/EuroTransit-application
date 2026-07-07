package it.polito.cpo.controller

import it.polito.cpo.contracts.inventory.ReservationRequest
import it.polito.cpo.contracts.inventory.ReservationResponse
import it.polito.cpo.contracts.inventory.ReservationStatus
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.*
import java.time.LocalDateTime
import java.util.UUID

@RestController
@RequestMapping("/api/inventory/reservations")
class InventoryController {

    @PostMapping
    suspend fun createReservation(
        @RequestHeader("Idempotency-Key") idempotencyKey: String,
        @RequestBody request: ReservationRequest
    ): ResponseEntity<ReservationResponse> {
        // Stub implementation: always returns a successful hold
        // Federico's Orders service will use this to test its pipeline.
        val response = ReservationResponse(
            reservationId = UUID.randomUUID().toString(),
            status = ReservationStatus.HELD,
            expiresAt = LocalDateTime.now().plusMinutes(10)
        )
        return ResponseEntity.status(HttpStatus.CREATED).body(response)
    }

    @DeleteMapping("/{reservationId}")
    suspend fun releaseReservation(
        @PathVariable reservationId: String
    ): ResponseEntity<Void> {
        // Stub implementation: acknowledges the release
        return ResponseEntity.noContent().build()
    }
}
