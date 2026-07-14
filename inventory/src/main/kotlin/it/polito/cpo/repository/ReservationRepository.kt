package it.polito.cpo.repository

import it.polito.cpo.model.Reservation
import org.springframework.data.repository.kotlin.CoroutineCrudRepository
import org.springframework.stereotype.Repository

@Repository
interface ReservationRepository : CoroutineCrudRepository<Reservation, String> {
    suspend fun findByReservationId(reservationId: String): Reservation?
}
