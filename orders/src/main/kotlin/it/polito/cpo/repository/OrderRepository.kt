package it.polito.cpo.repository

import it.polito.cpo.model.Order
import org.springframework.data.r2dbc.repository.Modifying
import org.springframework.data.r2dbc.repository.Query
import org.springframework.data.repository.kotlin.CoroutineCrudRepository
import java.util.UUID

interface OrderRepository : CoroutineCrudRepository<Order, UUID> {

    // --- Race-free money-path transitions --------------------------------------------------------
    // The async pipeline (Kafka consumer thread) and a customer cancellation (web request) run
    // concurrently, and the reserve/authorize calls are suspension points. Advancing the order with
    // a full-entity save would clobber a concurrent cancellation. Instead every forward transition
    // is a single atomic UPDATE guarded by `status <> 'CANCELLED'`, so a cancellation that commits
    // mid-pipeline makes the next transition affect 0 rows -> the pipeline aborts and compensates.

    @Modifying
    @Query("UPDATE orders SET status = 'RESERVING' WHERE id = :id AND status <> 'CANCELLED'")
    suspend fun markReserving(id: UUID): Long

    @Modifying
    @Query("UPDATE orders SET status = 'PAYMENT_PENDING', reservation_id = :reservationId WHERE id = :id AND status <> 'CANCELLED'")
    suspend fun markPaymentPending(id: UUID, reservationId: String): Long

    @Modifying
    @Query("UPDATE orders SET status = 'CONFIRMED', payment_method_token = NULL WHERE id = :id AND status <> 'CANCELLED'")
    suspend fun markConfirmed(id: UUID): Long

    @Modifying
    @Query("UPDATE orders SET status = 'FAILED', payment_method_token = NULL WHERE id = :id AND status <> 'CANCELLED'")
    suspend fun markFailed(id: UUID): Long

    // Cancellation is allowed only while the order is unpaid. Atomic conditional update: 1 row = the
    // order was cancelled; 0 rows = it is already CONFIRMED/terminal (not cancellable).
    @Modifying
    @Query("UPDATE orders SET status = 'CANCELLED', payment_method_token = NULL WHERE id = :id AND status IN ('ACCEPTED', 'RESERVING', 'PAYMENT_PENDING')")
    suspend fun cancelIfUnpaid(id: UUID): Long
}
