package it.polito.cpo.service

import it.polito.cpo.client.InventoryClient
import it.polito.cpo.client.PaymentClient
import it.polito.cpo.contracts.payments.PaymentRefundResponse
import it.polito.cpo.contracts.payments.PaymentRequest
import it.polito.cpo.contracts.payments.PaymentResponse
import it.polito.cpo.contracts.payments.PaymentStatus
import it.polito.cpo.contracts.payments.RefundStatus
import it.polito.cpo.contracts.inventory.ReservationRequest
import it.polito.cpo.contracts.inventory.ReservationResponse
import it.polito.cpo.contracts.inventory.ReservationStatus
import it.polito.cpo.controller.dtos.CheckoutRequest
import it.polito.cpo.controller.dtos.CheckoutResponse
import it.polito.cpo.contracts.events.OrderPlacedEvent
import it.polito.cpo.model.IdempotentRequest
import it.polito.cpo.model.Order
import it.polito.cpo.model.OrderStatus
import it.polito.cpo.repository.IdempotentRequestRepository
import it.polito.cpo.repository.OrderRepository
import it.polito.cpo.repository.OutboxEventRepository
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import it.polito.cpo.observability.ApiException
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.mockito.Mockito.mock
import org.springframework.http.HttpStatus
import org.springframework.transaction.ReactiveTransaction
import org.springframework.transaction.ReactiveTransactionManager
import org.springframework.transaction.TransactionDefinition
import org.springframework.transaction.reactive.TransactionalOperator
import reactor.core.publisher.Mono
import tools.jackson.databind.ObjectMapper
import tools.jackson.databind.json.JsonMapper
import tools.jackson.module.kotlin.kotlinModule
import java.math.BigDecimal
import java.time.LocalDateTime
import java.util.UUID
import java.util.concurrent.CopyOnWriteArrayList
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * Unit tests for the checkout orchestration logic, exercised with lightweight in-memory fakes
 * (no database, no Kafka broker). Focus: idempotency, the money-path state machine, the automated
 * refund on the money-taken-but-not-confirmed window, and the pre-payment cancellation rule.
 *
 * The transactional boundary is faked by a [TransactionalOperator] that runs the block inline (no
 * real transaction); the outbox repository is a Mockito stand-in (its save result is unused).
 */
class CheckoutOrchestratorTest {

    private val mapper: ObjectMapper = JsonMapper.builder().addModule(kotlinModule()).build()

    // Real TransactionalOperator over a no-op reactive transaction manager: it runs the block and
    // "commits" without a database — enough for unit tests, which only need the block's effects and
    // return value, not real commit/rollback semantics.
    private val inlineTx: TransactionalOperator = TransactionalOperator.create(NoopTxManager)

    private object NoopTx : ReactiveTransaction {
        override fun isNewTransaction() = true
        override fun setRollbackOnly() {}
        override fun isRollbackOnly() = false
        override fun isCompleted() = false
    }

    private object NoopTxManager : ReactiveTransactionManager {
        override fun getReactiveTransaction(definition: TransactionDefinition?): Mono<ReactiveTransaction> =
            Mono.just(NoopTx)
        override fun commit(transaction: ReactiveTransaction): Mono<Void> = Mono.empty()
        override fun rollback(transaction: ReactiveTransaction): Mono<Void> = Mono.empty()
    }

    private class FakeOrderService : OrderService(
        mock(OrderRepository::class.java),
        mock(IdempotentRequestRepository::class.java)
    ) {
        /** Statuses persisted, in order. Thread-safe: the async pipeline appends from another thread. */
        val savedOrderStatuses = CopyOnWriteArrayList<OrderStatus>()
        val idempotencyStore = HashMap<String, IdempotentRequest>()
        val ordersById = HashMap<UUID, Order>()
        var seeded: IdempotentRequest? = null
        /** When false, markConfirmed reports 0 rows (simulates a lost confirm / concurrent cancel). */
        var confirmable = true

        override suspend fun findIdempotentRequest(key: String): IdempotentRequest? =
            seeded ?: idempotencyStore[key]

        override suspend fun getOrderById(id: UUID): Order? = ordersById[id]

        override suspend fun saveOrder(order: Order): Order {
            savedOrderStatuses.add(order.status)
            ordersById[order.getId()] = order
            return order
        }

        override suspend fun saveIdempotentRequest(
            key: String,
            responseBody: String,
            principalId: String,
            operation: String,
            requestFingerprint: String,
        ): IdempotentRequest {
            val request = IdempotentRequest(key, responseBody, LocalDateTime.now(), principalId, operation, requestFingerprint)
            idempotencyStore[key] = request
            return request
        }

        private fun advance(id: UUID, to: OrderStatus, clearToken: Boolean): Long {
            val o = ordersById[id] ?: return 0
            if (o.status == OrderStatus.CANCELLED) return 0
            o.status = to
            if (clearToken) o.paymentMethodToken = null
            savedOrderStatuses.add(to)
            return 1
        }

        override suspend fun markReserving(id: UUID): Long = advance(id, OrderStatus.RESERVING, false)

        override suspend fun markPaymentPending(id: UUID, reservationId: String): Long {
            val o = ordersById[id] ?: return 0
            if (o.status == OrderStatus.CANCELLED) return 0
            o.status = OrderStatus.PAYMENT_PENDING
            o.reservationId = reservationId
            savedOrderStatuses.add(OrderStatus.PAYMENT_PENDING)
            return 1
        }

        override suspend fun markConfirmed(id: UUID): Long =
            if (!confirmable) 0 else advance(id, OrderStatus.CONFIRMED, true)

        override suspend fun markFailed(id: UUID): Long = advance(id, OrderStatus.FAILED, true)

        override suspend fun cancelIfUnpaid(id: UUID): Long {
            val o = ordersById[id] ?: return 0
            if (o.status !in setOf(OrderStatus.ACCEPTED, OrderStatus.RESERVING, OrderStatus.PAYMENT_PENDING)) return 0
            o.status = OrderStatus.CANCELLED
            o.paymentMethodToken = null
            savedOrderStatuses.add(OrderStatus.CANCELLED)
            return 1
        }
    }

    private class StubInventoryClient(private val status: ReservationStatus = ReservationStatus.HELD) : InventoryClient {
        val releasedReservationIds = CopyOnWriteArrayList<String>()
        override suspend fun reserveSeats(request: ReservationRequest, idempotencyKey: String, userId: String): ReservationResponse =
            ReservationResponse("res-1", status, LocalDateTime.now().plusMinutes(10))

        override suspend fun releaseSeats(reservationId: String, idempotencyKey: String) {
            releasedReservationIds.add(reservationId)
        }
    }

    private class StubPaymentClient(private val status: PaymentStatus = PaymentStatus.AUTHORIZED) : PaymentClient {
        val refundedOrderIds = CopyOnWriteArrayList<UUID>()
        override suspend fun authorizePayment(request: PaymentRequest, idempotencyKey: String, correlationId: String): PaymentResponse =
            PaymentResponse(status = status, providerReference = "test-auth-id", errorCode = null)

        override suspend fun refund(orderId: UUID, idempotencyKey: String): PaymentRefundResponse {
            refundedOrderIds.add(orderId)
            return PaymentRefundResponse(RefundStatus.REFUNDED, "refund-ref", null)
        }
    }

    private fun orchestratorFor(
        service: FakeOrderService,
        inventory: StubInventoryClient = StubInventoryClient(),
        payment: StubPaymentClient = StubPaymentClient(),
    ) = CheckoutOrchestrator(
        orderService = service,
        inventoryClient = inventory,
        paymentClient = payment,
        outboxEventRepository = mock(OutboxEventRepository::class.java),
        objectMapper = mapper,
        meterRegistry = SimpleMeterRegistry(),
        txOperator = inlineTx,
    )

    private fun acceptedOrder(id: UUID, token: String? = "test-token") = Order(
        id = id,
        userId = "user-1",
        status = OrderStatus.ACCEPTED,
        routeId = "R1",
        seats = "1A,1B",
        totalAmount = BigDecimal("100.00"),
        createdAt = LocalDateTime.now(),
        paymentMethodToken = token,
    ).apply { setAsNew(false) }

    @Test
    fun `new checkout returns ACCEPTED and persists an order plus an idempotency record`() = runTest {
        val service = FakeOrderService()

        val response = orchestratorFor(service).checkout(
            request = CheckoutRequest(routeId = "R1", seats = listOf("1A", "1B"), totalAmount = BigDecimal("100.00"), paymentMethodToken = "test-token"),
            idempotencyKey = "key-123",
            userId = "user-1",
            recipientEmail = "alice@example.com"
        )

        assertEquals(OrderStatus.ACCEPTED, response.status)
        // The first persisted state happens synchronously (before the async pipeline) and must be ACCEPTED.
        assertEquals(OrderStatus.ACCEPTED, service.savedOrderStatuses.first())
        // The idempotency record is stored under the caller-provided key so retries can be replayed.
        assertTrue(service.idempotencyStore.containsKey("key-123"))
    }

    @Test
    fun `a known idempotency key replays the stored response without creating a new order`() = runTest {
        val service = FakeOrderService()
        val storedOrderId = UUID.randomUUID()
        val storedResponse = CheckoutResponse(storedOrderId, OrderStatus.ACCEPTED)
        service.seeded = IdempotentRequest("key-abc", mapper.writeValueAsString(storedResponse), LocalDateTime.now())

        val response = orchestratorFor(service).checkout(
            request = CheckoutRequest(routeId = "R1", seats = listOf("2A"), totalAmount = BigDecimal("50.00"), paymentMethodToken = "test-token"),
            idempotencyKey = "key-abc",
            userId = "user-1",
            recipientEmail = "alice@example.com"
        )

        assertEquals(storedOrderId, response.orderId)
        assertEquals(OrderStatus.ACCEPTED, response.status)
        // Replaying an existing request must NOT create or persist a new order.
        assertTrue(service.savedOrderStatuses.isEmpty())
    }

    @Test
    fun `same idempotency key with a different payload is rejected with 409`() = runTest {
        val service = FakeOrderService()
        val storedResponse = CheckoutResponse(UUID.randomUUID(), OrderStatus.ACCEPTED)
        // Seeded record whose stored fingerprint cannot match the incoming request payload.
        service.seeded = IdempotentRequest(
            "key-x", mapper.writeValueAsString(storedResponse), LocalDateTime.now(),
            "user-1", "checkout", "a-different-fingerprint",
        )

        val ex = assertThrows<ApiException> {
            orchestratorFor(service).checkout(
                request = CheckoutRequest(routeId = "R9", seats = listOf("9Z"), totalAmount = BigDecimal("999.00"), paymentMethodToken = "other-token"),
                idempotencyKey = "key-x",
                userId = "user-1",
                recipientEmail = "alice@example.com"
            )
        }

        assertEquals(HttpStatus.CONFLICT, ex.status)
        assertEquals("IDEMPOTENCY_CONFLICT", ex.code)
        assertTrue(service.savedOrderStatuses.isEmpty())
    }

    @Test
    fun `processOrderPlaced drives an accepted order to CONFIRMED`() = runTest {
        val service = FakeOrderService()
        val orderId = UUID.randomUUID()
        service.ordersById[orderId] = acceptedOrder(orderId)

        orchestratorFor(service).processOrderPlaced(
            OrderPlacedEvent(correlationId = "corr-1", orderId = orderId, principalId = "user-1")
        )

        // The pipeline walks RESERVING -> PAYMENT_PENDING -> CONFIRMED.
        assertEquals(OrderStatus.CONFIRMED, service.savedOrderStatuses.last())
    }

    @Test
    fun `processOrderPlaced skips an order already in a terminal state`() = runTest {
        val service = FakeOrderService()
        val orderId = UUID.randomUUID()
        service.ordersById[orderId] = Order(
            id = orderId,
            userId = "user-1",
            status = OrderStatus.CONFIRMED,
            routeId = "R1",
            seats = "1A",
            totalAmount = BigDecimal("50.00"),
            createdAt = LocalDateTime.now()
        ).apply { setAsNew(false) }

        orchestratorFor(service).processOrderPlaced(
            OrderPlacedEvent(correlationId = "corr-1", orderId = orderId, principalId = "user-1")
        )

        // A redelivered event for a terminal order must not re-run the pipeline.
        assertTrue(service.savedOrderStatuses.isEmpty())
    }

    @Test
    fun `a failure after authorization triggers an automated refund and compensating release`() = runTest {
        val service = FakeOrderService().apply { confirmable = false } // confirm step reports 0 rows
        val inventory = StubInventoryClient()
        val payment = StubPaymentClient()
        val orderId = UUID.randomUUID()
        service.ordersById[orderId] = acceptedOrder(orderId)

        orchestratorFor(service, inventory, payment).processOrderPlaced(
            OrderPlacedEvent(correlationId = "corr-1", orderId = orderId, principalId = "user-1")
        )

        // Payment was authorized but the order could not be confirmed -> refund + seat release.
        assertTrue(payment.refundedOrderIds.contains(orderId))
        assertTrue(inventory.releasedReservationIds.contains("res-1"))
        assertEquals(OrderStatus.FAILED, service.ordersById[orderId]!!.status)
    }

    @Test
    fun `a payment decline fails the order without a refund`() = runTest {
        val service = FakeOrderService()
        val inventory = StubInventoryClient()
        val payment = StubPaymentClient(status = PaymentStatus.DECLINED)
        val orderId = UUID.randomUUID()
        service.ordersById[orderId] = acceptedOrder(orderId)

        orchestratorFor(service, inventory, payment).processOrderPlaced(
            OrderPlacedEvent(correlationId = "corr-1", orderId = orderId, principalId = "user-1")
        )

        assertEquals(OrderStatus.FAILED, service.ordersById[orderId]!!.status)
        assertTrue(payment.refundedOrderIds.isEmpty()) // nothing was captured, nothing to refund
        assertTrue(inventory.releasedReservationIds.contains("res-1"))
    }

    @Test
    fun `cancel on an unpaid order marks it CANCELLED and releases held seats`() = runTest {
        val service = FakeOrderService()
        val inventory = StubInventoryClient()
        val orderId = UUID.randomUUID()
        service.ordersById[orderId] = acceptedOrder(orderId).apply {
            status = OrderStatus.PAYMENT_PENDING
            reservationId = "res-held"
        }

        val cancelled = orchestratorFor(service, inventory).cancel(orderId)

        assertEquals(OrderStatus.CANCELLED, cancelled.status)
        assertTrue(inventory.releasedReservationIds.contains("res-held"))
    }

    @Test
    fun `cancel on a paid order is rejected with 409`() = runTest {
        val service = FakeOrderService()
        val orderId = UUID.randomUUID()
        service.ordersById[orderId] = acceptedOrder(orderId).apply { status = OrderStatus.CONFIRMED }

        val ex = assertThrows<ApiException> {
            orchestratorFor(service).cancel(orderId)
        }

        assertEquals(HttpStatus.CONFLICT, ex.status)
        assertEquals("ORDER_NOT_CANCELLABLE", ex.code)
        // A confirmed (paid) order stays CONFIRMED.
        assertEquals(OrderStatus.CONFIRMED, service.ordersById[orderId]!!.status)
    }

    @Test
    fun `cancel on an accepted order without a hold does not attempt a release`() = runTest {
        val service = FakeOrderService()
        val inventory = StubInventoryClient()
        val orderId = UUID.randomUUID()
        service.ordersById[orderId] = acceptedOrder(orderId) // ACCEPTED, no reservationId yet

        val cancelled = orchestratorFor(service, inventory).cancel(orderId)

        assertEquals(OrderStatus.CANCELLED, cancelled.status)
        assertFalse(inventory.releasedReservationIds.any())
    }
}
