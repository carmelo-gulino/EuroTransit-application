package it.polito.cpo.controller

import it.polito.cpo.contracts.inventory.ReservationRequest
import it.polito.cpo.contracts.inventory.ReservationResponse
import it.polito.cpo.contracts.inventory.ReservationStatus
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.*
import java.time.LocalDateTime
import java.util.UUID

import it.polito.cpo.service.ReservationService

import org.springframework.security.core.annotation.AuthenticationPrincipal
import org.springframework.security.oauth2.jwt.Jwt

@RestController
@RequestMapping("/api/inventory/reservations")
class InventoryController(
    private val reservationService: ReservationService
) {

    @PostMapping
    suspend fun createReservation(
        @RequestHeader("Idempotency-Key") idempotencyKey: String,
        @AuthenticationPrincipal jwt: Jwt,
        @RequestBody request: ReservationRequest
    ): ResponseEntity<ReservationResponse> {
        val response = reservationService.reserveSeats(idempotencyKey, jwt.subject, request)
        return ResponseEntity.status(HttpStatus.CREATED).body(response)
    }

    @DeleteMapping("/{reservationId}")
    suspend fun releaseReservation(
        @PathVariable reservationId: String
    ): ResponseEntity<Void> {
        reservationService.releaseReservation(reservationId)
        return ResponseEntity.noContent().build()
    }
}
