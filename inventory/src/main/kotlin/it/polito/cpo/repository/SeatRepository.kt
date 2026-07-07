package it.polito.cpo.repository

import it.polito.cpo.model.Seat
import org.springframework.data.r2dbc.repository.Modifying
import org.springframework.data.r2dbc.repository.Query
import org.springframework.data.repository.kotlin.CoroutineCrudRepository
import org.springframework.stereotype.Repository

@Repository
interface SeatRepository : CoroutineCrudRepository<Seat, String> {

    /**
     * Attempts to atomically update seats to HELD only if they are all AVAILABLE.
     * Returns the number of modified rows: if it differs from requested seats, an oversell was prevented!
     */
    @Modifying
    @Query("UPDATE seats SET status = 'HELD', reservation_id = :reservationId WHERE route_id = :routeId AND seat_id IN (:seatIds) AND status = 'AVAILABLE'")
    suspend fun holdSeats(routeId: String, seatIds: List<String>, reservationId: String): Int

    /**
     * Releases seats when a hold expires or is cancelled.
     */
    @Modifying
    @Query("UPDATE seats SET status = 'AVAILABLE', reservation_id = NULL WHERE reservation_id = :reservationId")
    suspend fun releaseSeats(reservationId: String): Int
}
