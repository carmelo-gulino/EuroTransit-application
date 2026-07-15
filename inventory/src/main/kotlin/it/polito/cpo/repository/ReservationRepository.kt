package it.polito.cpo.repository

import it.polito.cpo.model.Reservation
import org.springframework.data.repository.kotlin.CoroutineCrudRepository
import org.springframework.stereotype.Repository

@Repository
interface ReservationRepository : CoroutineCrudRepository<Reservation, String> {
    suspend fun findByReservationId(reservationId: String): Reservation?
    
    // Finds reservations matching a status (e.g. "HELD") that have expired
    fun findByStatusAndExpiresAtBefore(status: String, expiresAt: java.time.OffsetDateTime): kotlinx.coroutines.flow.Flow<Reservation>
}
