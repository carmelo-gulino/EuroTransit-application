package it.polito.cpo.service

import io.micrometer.core.instrument.MeterRegistry
import it.polito.cpo.contracts.inventory.ReservationStatus
import it.polito.cpo.repository.ReservationRepository
import it.polito.cpo.repository.SeatRepository
import org.slf4j.LoggerFactory
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Service
import org.springframework.transaction.reactive.TransactionalOperator
import org.springframework.transaction.reactive.executeAndAwait
import java.time.OffsetDateTime
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.runBlocking

@Service
class ReservationCleanupJob(
    private val reservationRepository: ReservationRepository,
    private val seatRepository: SeatRepository,
    private val meterRegistry: MeterRegistry,
    private val transactionalOperator: TransactionalOperator
) {
    private val logger = LoggerFactory.getLogger(ReservationCleanupJob::class.java)

    /**
     * Runs every 30 seconds to sweep expired reservations.
     * Uses runBlocking because @Scheduled does not natively support suspend functions in Spring Boot 3
     * without additional boilerplate, but runBlocking is acceptable for background jobs off the main request thread.
     */
    @Scheduled(fixedRate = 30000)
    fun sweepExpiredReservations() = runBlocking {
        val now = OffsetDateTime.now()
        val expiredReservations = reservationRepository.findByStatusAndExpiresAtBefore(ReservationStatus.HELD.name, now)

        expiredReservations.collect { reservation ->
            try {
                transactionalOperator.executeAndAwait {
                    // Double check status in case it was updated concurrently
                    val currentRes = reservationRepository.findByReservationId(reservation.reservationId)
                    if (currentRes != null && currentRes.status == ReservationStatus.HELD.name) {
                        logger.info("Reservation {} expired. Releasing seats.", reservation.reservationId)
                        
                        // Release seats
                        seatRepository.releaseSeats(reservation.reservationId)
                        
                        // Update reservation status
                        val updated = currentRes.copy(
                            status = "EXPIRED",
                            isNewRecord = false
                        )
                        reservationRepository.save(updated)
                        
                        // Record metric
                        meterRegistry.counter("inventory.holds.expired").increment()
                    }
                }
            } catch (e: Exception) {
                logger.error("Failed to expire reservation: {}", reservation.reservationId, e)
            }
        }
    }
}
