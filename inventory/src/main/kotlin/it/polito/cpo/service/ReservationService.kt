package it.polito.cpo.service

import com.fasterxml.jackson.databind.ObjectMapper
import it.polito.cpo.contracts.inventory.ReservationRequest
import it.polito.cpo.repository.IdempotencyRecordRepository
import it.polito.cpo.repository.ReservationRepository
import it.polito.cpo.repository.SeatRepository
import org.springframework.stereotype.Service
import java.security.MessageDigest

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
}
