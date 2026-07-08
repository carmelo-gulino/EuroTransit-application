package it.polito.cpo.service

import com.fasterxml.jackson.databind.ObjectMapper
import it.polito.cpo.contracts.events.InventoryFailedEvent
import it.polito.cpo.contracts.events.InventoryFailedPayload
import it.polito.cpo.contracts.events.InventoryReservedEvent
import it.polito.cpo.contracts.events.InventoryReservedPayload
import it.polito.cpo.contracts.inventory.ReservationRequest
import it.polito.cpo.contracts.inventory.ReservationResponse
import it.polito.cpo.contracts.inventory.ReservationStatus
import it.polito.cpo.model.IdempotencyRecord
import it.polito.cpo.model.Reservation
import it.polito.cpo.observability.ApiException
import it.polito.cpo.repository.IdempotencyRecordRepository
import it.polito.cpo.repository.ReservationRepository
import it.polito.cpo.repository.SeatRepository
import org.springframework.http.HttpStatus
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import java.security.MessageDigest
import java.time.OffsetDateTime
import java.util.UUID
import it.polito.cpo.event.KafkaEventPublisher

@Service
class ReservationService(
    private val seatRepository: SeatRepository,
    private val reservationRepository: ReservationRepository,
    private val idempotencyRecordRepository: IdempotencyRecordRepository,
    private val objectMapper: ObjectMapper,
    private val eventPublisher: KafkaEventPublisher
) {

    /**
     * Generates an SHA-256 fingerprint of the request payload to detect idempotency collisions.
     */
    private fun generateFingerprint(request: ReservationRequest): String {
        val json = objectMapper.writeValueAsString(request)
        val digest = MessageDigest.getInstance("SHA-256").digest(json.toByteArray())
        return digest.joinToString("") { "%02x".format(it) }
    }

    /**
     * Reserves seats and handles idempotency to prevent oversell or double-processing.
     */
    @Transactional
    suspend fun reserveSeats(
        idempotencyKey: String,
        principalId: String,
        correlationId: String,
        request: ReservationRequest
    ): ReservationResponse {
        val fingerprint = generateFingerprint(request)
        val existingRecord = idempotencyRecordRepository.findByKeyAndPrincipal(idempotencyKey, principalId)

        if (existingRecord != null) {
            if (existingRecord.requestFingerprint != fingerprint) {
                throw ApiException(
                    status = HttpStatus.CONFLICT,
                    code = "IDEMPOTENCY_CONFLICT",
                    message = "Idempotency key reused with a different payload"
                )
            }
            if (existingRecord.responseBody != null) {
                return objectMapper.readValue(
                    existingRecord.responseBody,
                    ReservationResponse::class.java
                )
            }
        }

        val reservationId = UUID.randomUUID().toString()
        val rowsUpdated = seatRepository.holdSeats(request.routeId, request.seats, reservationId)
        val isSuccess = rowsUpdated == request.seats.size
        
        val status = if (isSuccess) ReservationStatus.HELD else ReservationStatus.FAILED
        val expiresAt = OffsetDateTime.now().plusMinutes(10)

        if (isSuccess) {
            val reservation = Reservation(
                reservationId = reservationId,
                orderId = request.orderId,
                routeId = request.routeId,
                status = status.name,
                expiresAt = expiresAt
            )
            reservationRepository.save(reservation)
        }

        val response = ReservationResponse(
            reservationId = reservationId,
            status = status,
            expiresAt = expiresAt.toLocalDateTime()
        )

        val idempotencyRecord = IdempotencyRecord(
            idempotencyKey = idempotencyKey,
            principalId = principalId,
            operation = "RESERVE",
            requestFingerprint = fingerprint,
            responseStatusCode = 201,
            responseBody = objectMapper.writeValueAsString(response)
        )
        idempotencyRecordRepository.save(idempotencyRecord)

        // Publish Kafka event based on outcome
        if (isSuccess) {
            val event = InventoryReservedEvent(
                correlationId = correlationId,
                orderId = request.orderId,
                principalId = principalId,
                payload = InventoryReservedPayload(
                    reservationId = reservationId,
                    routeId = request.routeId,
                    expiresAt = expiresAt.toLocalDateTime()
                )
            )
            eventPublisher.publishInventoryReserved(event)
        } else {
            val event = InventoryFailedEvent(
                correlationId = correlationId,
                orderId = request.orderId,
                principalId = principalId,
                payload = InventoryFailedPayload(
                    reason = "Seats unavailable or already held"
                )
            )
            eventPublisher.publishInventoryFailed(event)
        }

        return response
    }

    /**
     * Releases a reservation and frees up the associated seats.
     * This operation is naturally idempotent.
     */
    @Transactional
    suspend fun releaseReservation(reservationId: String) {
        val reservation = reservationRepository.findById(reservationId)
        
        if (reservation != null && reservation.status == ReservationStatus.HELD.name) {
            seatRepository.releaseSeats(reservationId)
            
            val updatedReservation = reservation.copy(
                status = "CANCELLED",
                isNewRecord = false
            )
            reservationRepository.save(updatedReservation)
        }
    }
}
