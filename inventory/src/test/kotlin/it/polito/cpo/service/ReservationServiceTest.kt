package it.polito.cpo.service

import it.polito.cpo.contracts.events.InventoryFailedEvent
import it.polito.cpo.contracts.events.InventoryReservedEvent
import it.polito.cpo.contracts.inventory.ReservationRequest
import it.polito.cpo.contracts.inventory.ReservationStatus
import it.polito.cpo.event.KafkaEventPublisher
import it.polito.cpo.model.IdempotencyRecord
import it.polito.cpo.model.Reservation
import it.polito.cpo.repository.IdempotencyRecordRepository
import it.polito.cpo.repository.ReservationRepository
import it.polito.cpo.repository.SeatRepository
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.mockito.Mockito
import org.springframework.http.HttpStatus
import it.polito.cpo.observability.ApiException
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.transaction.ReactiveTransaction
import org.springframework.transaction.reactive.TransactionCallback
import org.springframework.transaction.reactive.TransactionalOperator
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import tools.jackson.databind.ObjectMapper
import tools.jackson.databind.json.JsonMapper
import tools.jackson.module.kotlin.kotlinModule
import java.util.UUID

class ReservationServiceTest {

    private val mapper: ObjectMapper = JsonMapper.builder().addModule(kotlinModule()).build()

    private val fakeTransactionalOperator = object : TransactionalOperator {
        override fun <T : Any> execute(action: TransactionCallback<T>): Flux<T> {
            val stubTransaction = object : ReactiveTransaction {
                override fun isNewTransaction() = true
                override fun setRollbackOnly() {}
                override fun isRollbackOnly() = false
                override fun isCompleted() = false
            }
            return Flux.from(action.doInTransaction(stubTransaction))
        }
        override fun <T : Any> transactional(flux: Flux<T>): Flux<T> = flux
        override fun <T : Any> transactional(mono: Mono<T>): Mono<T> = mono
    }

    private class FakeSeatRepository(
        private val delegate: SeatRepository = Mockito.mock(SeatRepository::class.java),
        var seatsAvailable: Boolean = true
    ) : SeatRepository by delegate {
        var releaseCalledWith: String? = null
        override suspend fun holdSeats(routeId: String, seatIds: List<String>, reservationId: String): Int {
            return if (seatsAvailable) seatIds.size else 0
        }
        override suspend fun releaseSeats(reservationId: String): Int {
            releaseCalledWith = reservationId
            return 1
        }
    }

    private class FakeReservationRepository(
        private val delegate: ReservationRepository = Mockito.mock(ReservationRepository::class.java)
    ) : ReservationRepository by delegate {
        val store = HashMap<String, Reservation>()
        override suspend fun findByReservationId(reservationId: String): Reservation? {
            return store[reservationId]
        }
        override suspend fun <S : Reservation> save(entity: S): S {
            store[entity.reservationId] = entity
            return entity
        }
    }

    private class FakeIdempotencyRecordRepository(
        private val delegate: IdempotencyRecordRepository = Mockito.mock(
            IdempotencyRecordRepository::class.java)
    ) : IdempotencyRecordRepository by delegate {
        val store = HashMap<String, IdempotencyRecord>()
        override suspend fun <S : IdempotencyRecord> save(entity: S): S {
            store["${entity.idempotencyKey}-${entity.principalId}"] = entity
            return entity
        }
        override suspend fun findByKeyAndPrincipal(key: String, principalId: String): IdempotencyRecord? {
            return store["$key-$principalId"]
        }
    }

    @Suppress("UNCHECKED_CAST")
    private class FakeKafkaEventPublisher : KafkaEventPublisher(
        Mockito.mock(KafkaTemplate::class.java) as KafkaTemplate<String, Any>
    ) {
        val reservedEvents = mutableListOf<InventoryReservedEvent>()
        val failedEvents = mutableListOf<InventoryFailedEvent>()
        
        override fun publishInventoryReserved(event: InventoryReservedEvent) {
            reservedEvents.add(event)
        }
        
        override fun publishInventoryFailed(event: InventoryFailedEvent) {
            failedEvents.add(event)
        }
    }

    private fun buildService(
        seatRepo: FakeSeatRepository,
        resRepo: FakeReservationRepository,
        idemRepo: FakeIdempotencyRecordRepository,
        eventPub: FakeKafkaEventPublisher
    ) = ReservationService(
        seatRepository = seatRepo,
        reservationRepository = resRepo,
        idempotencyRecordRepository = idemRepo,
        objectMapper = mapper,
        eventPublisher = eventPub,
        transactionalOperator = fakeTransactionalOperator
    )

    @Test
    fun `reserveSeats successfully holds seats, persists state, and publishes Reserved event`() = runTest {
        val seatRepo = FakeSeatRepository(seatsAvailable = true)
        val resRepo = FakeReservationRepository()
        val idemRepo = FakeIdempotencyRecordRepository()
        val eventPub = FakeKafkaEventPublisher()
        val service = buildService(seatRepo, resRepo, idemRepo, eventPub)

        val request = ReservationRequest(UUID.randomUUID(), listOf("1A", "1B"), "route-1")
        val principalId = "user-1"
        val correlationId = "corr-1"
        val idempotencyKey = "key-success"
        
        val response = service.reserveSeats(idempotencyKey, principalId, correlationId, request)

        assertEquals(
            ReservationStatus.HELD,
            response.status,
            "The reservation status should be HELD"
        )
        
        assertTrue(
            resRepo.store.containsKey(response.reservationId),
            "The reservation must be saved in the database"
        )
        assertTrue(
            idemRepo.store.containsKey("$idempotencyKey-$principalId"),
            "The idempotency record must be saved"
        )
        
        assertEquals(
            1,
            eventPub.reservedEvents.size,
            "Exactly one Reserved event should be emitted"
        )
        assertEquals(
            0,
            eventPub.failedEvents.size,
            "No Failed event should be emitted"
        )
        
        val emittedEvent = eventPub.reservedEvents.first()
        assertEquals(correlationId, emittedEvent.correlationId)
        assertEquals(request.orderId, emittedEvent.orderId)
        assertEquals(principalId, emittedEvent.principalId)
    }

    @Test
    fun `reserveSeats fails to hold seats (oversell), does not persist reservation, and publishes Failed event`() = runTest {
        val seatRepo = FakeSeatRepository(seatsAvailable = false)
        val resRepo = FakeReservationRepository()
        val idemRepo = FakeIdempotencyRecordRepository()
        val eventPub = FakeKafkaEventPublisher()
        val service = buildService(seatRepo, resRepo, idemRepo, eventPub)

        val request = ReservationRequest(UUID.randomUUID(), listOf("1A", "1B"), "route-1")
        val principalId = "user-1"
        val correlationId = "corr-1"
        val idempotencyKey = "key-oversell"
        
        val response = service.reserveSeats(idempotencyKey, principalId, correlationId, request)

        assertEquals(
            ReservationStatus.FAILED, 
            response.status, 
            "The reservation status should be FAILED due to oversell"
        )
        
        assertTrue(
            resRepo.store.isEmpty(), 
            "The reservation must NOT be saved in the database if it failed"
        )
        assertTrue(
            idemRepo.store.containsKey("$idempotencyKey-$principalId"), 
            "The idempotency record MUST be saved to cache the failure"
        )
        
        assertEquals(0, eventPub.reservedEvents.size)
        assertEquals(1, eventPub.failedEvents.size)
        
        val emittedEvent = eventPub.failedEvents.first()
        assertEquals(correlationId, emittedEvent.correlationId)
        assertEquals(request.orderId, emittedEvent.orderId)
        assertEquals(principalId, emittedEvent.principalId)
    }

    @Test
    fun `reserveSeats with identical idempotency key returns cached response and skips DB updates`() = runTest {
        val seatRepo = FakeSeatRepository()
        val resRepo = FakeReservationRepository()
        val idemRepo = FakeIdempotencyRecordRepository()
        val eventPub = FakeKafkaEventPublisher()
        val service = buildService(seatRepo, resRepo, idemRepo, eventPub)

        val request = ReservationRequest(UUID.randomUUID(), listOf("1A", "1B"), "route-1")
        val principalId = "user-1"
        val correlationId = "corr-1"
        val idempotencyKey = "key-cached"
        
        val fingerprint = service.javaClass.getDeclaredMethod("generateFingerprint", ReservationRequest::class.java)
            .apply { isAccessible = true }
            .invoke(service, request) as String

        val cachedResponse = it.polito.cpo.contracts.inventory.ReservationResponse(
            "res-cached", 
            ReservationStatus.HELD, 
            java.time.LocalDateTime.now().plusMinutes(10)
        )
        
        idemRepo.save(IdempotencyRecord(
            idempotencyKey = idempotencyKey,
            principalId = principalId,
            operation = "RESERVE",
            requestFingerprint = fingerprint,
            responseStatusCode = 201,
            responseBody = mapper.writeValueAsString(cachedResponse)
        ))

        val response = service.reserveSeats(idempotencyKey, principalId, correlationId, request)

        assertEquals("res-cached", response.reservationId)
        assertEquals(ReservationStatus.HELD, response.status)
        
        assertTrue(resRepo.store.isEmpty(), "No new reservation should be saved")
        
        assertEquals(0, eventPub.reservedEvents.size)
        assertEquals(0, eventPub.failedEvents.size)
    }

    @Test
    fun `reserveSeats with existing idempotency key but different payload throws ApiException`() = runTest {
        val idemRepo = FakeIdempotencyRecordRepository()
        val service = buildService(
            FakeSeatRepository(),
            FakeReservationRepository(),
            idemRepo,
            FakeKafkaEventPublisher()
        )

        val request = ReservationRequest(UUID.randomUUID(), listOf("1A", "1B"), "route-1")
        val principalId = "user-1"
        val correlationId = "corr-1"
        val idempotencyKey = "key-conflict"
        
        idemRepo.save(IdempotencyRecord(
            idempotencyKey = idempotencyKey,
            principalId = principalId,
            operation = "RESERVE",
            requestFingerprint = "a-completely-different-hash",
            responseStatusCode = 201,
            responseBody = "{}"
        ))

        val exception = assertThrows<ApiException> {
            service.reserveSeats(idempotencyKey, principalId, correlationId, request)
        }

        assertEquals(HttpStatus.CONFLICT, exception.status)
        assertEquals("IDEMPOTENCY_CONFLICT", exception.code)
    }

    @Test
    fun `releaseReservation successfully releases seats if reservation is held`() = runTest {
        val seatRepo = FakeSeatRepository()
        val resRepo = FakeReservationRepository()
        val service = buildService(
            seatRepo,
            resRepo,
            FakeIdempotencyRecordRepository(),
            FakeKafkaEventPublisher()
        )

        val reservationId = "res-cancel-1"
        resRepo.save(Reservation(
            reservationId = reservationId,
            orderId = UUID.randomUUID(),
            routeId = "route-1",
            status = ReservationStatus.HELD.name,
            expiresAt = java.time.OffsetDateTime.now().plusMinutes(10)
        ))

        service.releaseReservation(reservationId)

        assertEquals(reservationId, seatRepo.releaseCalledWith)
        assertEquals("CANCELLED", resRepo.store[reservationId]?.status)
    }

    @Test
    fun `releaseReservation ignores request if reservation not found or already cancelled`() = runTest {
        val seatRepo = FakeSeatRepository()
        val resRepo = FakeReservationRepository()
        val service = buildService(
            seatRepo,
            resRepo,
            FakeIdempotencyRecordRepository(),
            FakeKafkaEventPublisher()
        )

        service.releaseReservation("non-existent")

        assertNull(seatRepo.releaseCalledWith)
    }
}
