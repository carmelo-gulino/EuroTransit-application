package it.polito.cpo.service

import it.polito.cpo.client.InventoryClient
import it.polito.cpo.client.PaymentClient
import it.polito.cpo.contracts.payments.PaymentRequest
import it.polito.cpo.contracts.payments.PaymentResponse
import it.polito.cpo.contracts.payments.PaymentStatus
import it.polito.cpo.contracts.inventory.ReservationRequest
import it.polito.cpo.contracts.inventory.ReservationResponse
import it.polito.cpo.contracts.inventory.ReservationStatus
import it.polito.cpo.controller.dtos.CheckoutRequest
import it.polito.cpo.controller.dtos.CheckoutResponse
import it.polito.cpo.event.KafkaEventPublisher
import it.polito.cpo.contracts.events.NotificationRequestedEvent
import it.polito.cpo.contracts.events.OrderConfirmedEvent
import it.polito.cpo.contracts.events.OrderPlacedEvent
import it.polito.cpo.model.IdempotentRequest
import it.polito.cpo.model.Order
import it.polito.cpo.model.OrderStatus
import it.polito.cpo.repository.IdempotentRequestRepository
import it.polito.cpo.repository.OrderRepository
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test
import org.mockito.Mockito.mock
import org.springframework.kafka.core.KafkaTemplate
import tools.jackson.databind.ObjectMapper
import tools.jackson.databind.json.JsonMapper
import tools.jackson.module.kotlin.kotlinModule
import java.math.BigDecimal
import java.time.LocalDateTime
import java.util.UUID
import java.util.concurrent.CopyOnWriteArrayList
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * Unit tests for the checkout orchestration logic, exercised with lightweight in-memory fakes
 * (no database, no Kafka broker). Focus: the idempotency behaviour of the money path.
 *
 * The collaborators are faked by subclassing (OrderService / KafkaEventPublisher are `open`
 * because the kotlin-spring plugin opens Spring-annotated classes) or by implementing the small
 * client interfaces directly. Unused repository/KafkaTemplate dependencies are Mockito stand-ins.
 */
class CheckoutOrchestratorTest {

    private val mapper: ObjectMapper = JsonMapper.builder().addModule(kotlinModule()).build()

    private class FakeOrderService : OrderService(
        mock(OrderRepository::class.java),
        mock(IdempotentRequestRepository::class.java)
    ) {
        /** Statuses persisted, in order. Thread-safe: the async pipeline appends from another thread. */
        val savedOrderStatuses = CopyOnWriteArrayList<OrderStatus>()
        val idempotencyStore = HashMap<String, IdempotentRequest>()
        val ordersById = HashMap<UUID, Order>()
        var seeded: IdempotentRequest? = null

        override suspend fun findIdempotentRequest(key: String): IdempotentRequest? =
            seeded ?: idempotencyStore[key]

        override suspend fun getOrderById(id: UUID): Order? = ordersById[id]

        override suspend fun saveOrder(order: Order): Order {
            savedOrderStatuses.add(order.status)
            ordersById[order.getId()] = order
            return order
        }

        override suspend fun saveIdempotentRequest(key: String, responseBody: String): IdempotentRequest {
            val request = IdempotentRequest(key, responseBody, LocalDateTime.now())
            idempotencyStore[key] = request
            return request
        }
    }

    private class StubInventoryClient(private val status: ReservationStatus = ReservationStatus.HELD) : InventoryClient {
        override suspend fun reserveSeats(request: ReservationRequest, idempotencyKey: String, userId: String): ReservationResponse =
            ReservationResponse("res-1", status, LocalDateTime.now().plusMinutes(10))

        override suspend fun releaseSeats(reservationId: String, idempotencyKey: String) {}
    }

    private class StubPaymentClient(private val status: PaymentStatus = PaymentStatus.AUTHORIZED) : PaymentClient {
        override suspend fun authorizePayment(request: PaymentRequest, idempotencyKey: String, correlationId: String): PaymentResponse =
            PaymentResponse(
                status = status,
                providerReference = "test-auth-id",
                errorCode = null
            )
    }

    @Suppress("UNCHECKED_CAST")
    private class NoopEventPublisher :
        KafkaEventPublisher(mock(KafkaTemplate::class.java) as KafkaTemplate<String, String>, mock(ObjectMapper::class.java)) {
        override fun publishOrderPlaced(event: OrderPlacedEvent) {}
        override fun publishOrderConfirmed(event: OrderConfirmedEvent) {}
        override fun publishNotificationRequested(event: NotificationRequestedEvent) {}
    }

    private fun orchestratorFor(service: FakeOrderService) = CheckoutOrchestrator(
        orderService = service,
        inventoryClient = StubInventoryClient(),
        paymentClient = StubPaymentClient(),
        kafkaEventPublisher = NoopEventPublisher(),
        objectMapper = mapper,
        meterRegistry = SimpleMeterRegistry()
    )

    @Test
    fun `new checkout returns ACCEPTED and persists an order plus an idempotency record`() = runTest {
        val service = FakeOrderService()

        val response = orchestratorFor(service).checkout(
            request = CheckoutRequest(routeId = "R1", seats = listOf("1A", "1B"), totalAmount = BigDecimal("100.00"), paymentMethodToken = "test-token"),
            idempotencyKey = "key-123",
            userId = "user-1"
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
            userId = "user-1"
        )

        assertEquals(storedOrderId, response.orderId)
        assertEquals(OrderStatus.ACCEPTED, response.status)
        // Replaying an existing request must NOT create or persist a new order.
        assertTrue(service.savedOrderStatuses.isEmpty())
    }

    @Test
    fun `processOrderPlaced drives an accepted order to CONFIRMED`() = runTest {
        val service = FakeOrderService()
        val orderId = UUID.randomUUID()
        service.ordersById[orderId] = Order(
            id = orderId,
            userId = "user-1",
            status = OrderStatus.ACCEPTED,
            routeId = "R1",
            seats = "1A,1B",
            totalAmount = BigDecimal("100.00"),
            createdAt = LocalDateTime.now(),
            paymentMethodToken = "test-token"
        ).apply { setAsNew(false) }

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
}
