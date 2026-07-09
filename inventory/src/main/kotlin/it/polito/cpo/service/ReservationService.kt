package it.polito.cpo.service

import tools.jackson.databind.ObjectMapper
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
import it.polito.cpo.model.OutboxEvent
import it.polito.cpo.repository.OutboxEventRepository
import org.springframework.transaction.reactive.TransactionalOperator
import org.springframework.transaction.reactive.executeAndAwait
import org.slf4j.LoggerFactory
import org.springframework.dao.DuplicateKeyException

@Service
class ReservationService(
    private val seatRepository: SeatRepository,
    private val reservationRepository: ReservationRepository,
    private val idempotencyRecordRepository: IdempotencyRecordRepository,
    private val outboxEventRepository: OutboxEventRepository,
    private val objectMapper: ObjectMapper,
    private val transactionalOperator: TransactionalOperator
) {
    private val logger = LoggerFactory.getLogger(ReservationService::class.java)

    /**
     * Generates an SHA-256 fingerprint of the request payload to detect idempotency collisions.
     */
    private fun generateFingerprint(request: ReservationRequest): String {
        // Canonize the seats order to prevent false conflicts
        val canonicalRequest = request.copy(seats = request.seats.sorted())
        val json = objectMapper.writeValueAsString(canonicalRequest)
        val digest = MessageDigest.getInstance("SHA-256").digest(json.toByteArray())
        return digest.joinToString("") { "%02x".format(it) }
    }

    /**
     * Reserves seats and handles idempotency to prevent oversell or double-processing.
     * DB writes and Outbox events are executed atomically in a programmatic transaction.
     */
    suspend fun reserveSeats(
        idempotencyKey: String,
        principalId: String,
        correlationId: String,
        request: ReservationRequest
    ): ReservationResponse {
        val fingerprint = generateFingerprint(request)
        
        // Fast-path check
        val existingRecord = idempotencyRecordRepository.findByKeyAndPrincipal(idempotencyKey, principalId)
        if (existingRecord != null) {
            return processExistingRecord(existingRecord, fingerprint, idempotencyKey)
        }

        val reservationId = UUID.randomUUID().toString()
        val expiresAt = OffsetDateTime.now().plusMinutes(10)

        try {
            val isSuccess = transactionalOperator.executeAndAwait {
                val rowsUpdated = seatRepository.holdSeats(request.routeId, request.seats, reservationId)
                val success = rowsUpdated == request.seats.size
                val status = if (success) ReservationStatus.HELD else ReservationStatus.FAILED

                if (success) {
                    val reservation = Reservation(
                        reservationId = reservationId,
                        orderId = request.orderId,
                        routeId = request.routeId,
                        status = status.name,
                        expiresAt = expiresAt
                    )
                    reservationRepository.save(reservation)
                }

                val storedResponse = ReservationResponse(
                    reservationId = reservationId,
                    status = status,
                    expiresAt = expiresAt.toLocalDateTime()
                )

                val idempotencyRecord = IdempotencyRecord(
                    idempotencyKey = idempotencyKey,
                    principalId = principalId,
                    operation = "RESERVE",
                    requestFingerprint = fingerprint,
                    responseStatusCode = if (success) 201 else 422,
                    responseBody = objectMapper.writeValueAsString(storedResponse)
                )
                idempotencyRecordRepository.save(idempotencyRecord)

                // 2. Save Outbox Event inside the transaction
                if (success) {
                    logger.info("Successfully reserved seats for order: {} by principal: {}", request.orderId, principalId)
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
                    val outboxEvent = OutboxEvent(
                        aggregateType = "Inventory",
                        aggregateId = request.orderId.toString(),
                        type = "inventory-reserved",
                        payload = objectMapper.writeValueAsString(event)
                    )
                    outboxEventRepository.save(outboxEvent)
                } else {
                    logger.info("Failed to reserve seats for order: {} by principal: {}", request.orderId, principalId)
                    val event = InventoryFailedEvent(
                        correlationId = correlationId,
                        orderId = request.orderId,
                        principalId = principalId,
                        payload = InventoryFailedPayload(
                            reason = "Seats unavailable or already held"
                        )
                    )
                    val outboxEvent = OutboxEvent(
                        aggregateType = "Inventory",
                        aggregateId = request.orderId.toString(),
                        type = "inventory-failed",
                        payload = objectMapper.writeValueAsString(event)
                    )
                    outboxEventRepository.save(outboxEvent)
                }

                success
            }

            val finalStatus = if (isSuccess) ReservationStatus.HELD else ReservationStatus.FAILED
            return ReservationResponse(
                reservationId = reservationId,
                status = finalStatus,
                expiresAt = expiresAt.toLocalDateTime()
            )
        } catch (e: DuplicateKeyException) {
            logger.info("Concurrent request detected for idempotency key: {}. Falling back to cached response.", idempotencyKey)
            val concurrentRecord = idempotencyRecordRepository.findByKeyAndPrincipal(idempotencyKey, principalId)
                ?: throw ApiException(
                    HttpStatus.INTERNAL_SERVER_ERROR,
                    "INTERNAL_ERROR",
                    "Idempotency record lost after DuplicateKeyException"
                )
            
            return processExistingRecord(concurrentRecord, fingerprint, idempotencyKey)
        }
    }

    private fun processExistingRecord(
        existingRecord: IdempotencyRecord,
        fingerprint: String,
        idempotencyKey: String
    ): ReservationResponse {
        if (existingRecord.requestFingerprint != fingerprint) {
            throw ApiException(
                status = HttpStatus.CONFLICT,
                code = "IDEMPOTENCY_CONFLICT",
                message = "Idempotency key reused with a different payload"
            )
        }
        if (existingRecord.responseBody != null) {
            logger.info("Returning cached reservation response for idempotency key: {}", idempotencyKey)
            return objectMapper.readValue(
                existingRecord.responseBody,
                ReservationResponse::class.java
            )
        }
        throw ApiException(HttpStatus.INTERNAL_SERVER_ERROR, "INTERNAL_ERROR", "Idempotency record in invalid state")
    }

    /**
     * Releases a reservation and frees up the associated seats.
     * This operation is naturally idempotent.
     */
    @Transactional
    suspend fun releaseReservation(reservationId: String) {
        val reservation = reservationRepository.findByReservationId(reservationId)
        
        if (reservation != null && reservation.status == ReservationStatus.HELD.name) {
            logger.info("Releasing reservation: {} for order: {}", reservationId, reservation.orderId)
            seatRepository.releaseSeats(reservationId)
            
            val updatedReservation = reservation.copy(
                status = "CANCELLED",
                isNewRecord = false
            )
            reservationRepository.save(updatedReservation)
        } else {
            logger.info("Ignored release request for reservation: {} (not found or already cancelled)", reservationId)
        }
    }
}
