package it.polito.cpo.service

import com.fasterxml.jackson.databind.ObjectMapper
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

@Service
class ReservationService(
    private val seatRepository: SeatRepository,
    private val reservationRepository: ReservationRepository,
    private val idempotencyRecordRepository: IdempotencyRecordRepository,
    private val objectMapper: ObjectMapper
) {

    /**
     * Generates a SHA-256 fingerprint of the request payload to detect idempotency collisions.
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
                return objectMapper.readValue(existingRecord.responseBody, ReservationResponse::class.java)
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

        return response
    }
}
