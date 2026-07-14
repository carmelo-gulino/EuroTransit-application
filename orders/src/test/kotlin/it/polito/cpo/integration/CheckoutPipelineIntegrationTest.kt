package it.polito.cpo.integration

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
import it.polito.cpo.model.Order
import it.polito.cpo.model.OrderStatus
import it.polito.cpo.repository.OrderRepository
import it.polito.cpo.repository.OutboxEventRepository
import it.polito.cpo.service.CheckoutOrchestrator
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Import
import org.springframework.context.annotation.Primary
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.test.context.EmbeddedKafka
import org.springframework.security.oauth2.jwt.ReactiveJwtDecoder
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import reactor.core.publisher.Mono
import java.math.BigDecimal
import java.time.LocalDateTime
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * End-to-end integration test of the durable checkout pipeline inside orders, with real
 * infrastructure: a Testcontainers Postgres (R2DBC at runtime + Flyway migrations at startup) and an
 * embedded Kafka broker. Only the external money-path collaborators (inventory, payments) are
 * replaced by controllable in-process doubles.
 *
 * With the transactional outbox, `checkout()` no longer publishes to Kafka directly: it writes an
 * `order-placed` row into `outbox_events` in the same transaction as the order/idempotency records
 * (Debezium relays it in production). The test stands in for Debezium via [relayOrderPlaced], reading
 * the committed outbox row and publishing it to Kafka — then the real `OrderPlacedEventListener`
 * consumes it and drives the pipeline. This proves the atomic outbox write, the produce->consume
 * serialization roundtrip, the Flyway schema, and idempotent recovery under redelivery.
 *
 * The doubles are deterministic per request/order (a `DECLINE` payment token triggers a decline; all
 * bookkeeping is keyed by order id) so tests stay isolated despite the shared singleton beans and the
 * asynchronous consumer thread.
 */
@SpringBootTest(properties = ["spring.kafka.bootstrap-servers=\${spring.embedded.kafka.brokers}"])
@EmbeddedKafka(partitions = 1, topics = ["order-placed", "order-confirmed", "notification-requested"])
@Testcontainers
@Import(CheckoutPipelineIntegrationTest.TestDoubles::class)
class CheckoutPipelineIntegrationTest {

    companion object {
        const val DECLINE_TOKEN = "DECLINE"

        @Container
        @JvmStatic
        val postgres: PostgreSQLContainer<*> = PostgreSQLContainer("postgres:16-alpine")

        @DynamicPropertySource
        @JvmStatic
        fun properties(registry: DynamicPropertyRegistry) {
            registry.add("spring.r2dbc.url") {
                "r2dbc:postgresql://${postgres.host}:${postgres.getMappedPort(5432)}/${postgres.databaseName}"
            }
            registry.add("spring.r2dbc.username") { postgres.username }
            registry.add("spring.r2dbc.password") { postgres.password }
            registry.add("spring.flyway.url") { postgres.jdbcUrl }
            registry.add("spring.flyway.user") { postgres.username }
            registry.add("spring.flyway.password") { postgres.password }
        }
    }

    @Autowired private lateinit var orchestrator: CheckoutOrchestrator
    @Autowired private lateinit var orderRepository: OrderRepository
    @Autowired private lateinit var outboxRepository: OutboxEventRepository
    @Autowired private lateinit var kafkaTemplate: KafkaTemplate<String, String>
    @Autowired private lateinit var inventory: ControllableInventoryClient
    @Autowired private lateinit var payment: ControllablePaymentClient

    @Test
    fun `happy path drives the order through the outbox and Kafka to CONFIRMED`() = runBlocking {
        val response = orchestrator.checkout(request(), "it-happy-${UUID.randomUUID()}", "user-1", "user-1@example.com")
        assertEquals(OrderStatus.ACCEPTED, response.status)

        relayOrderPlaced(response.orderId)

        val order = awaitTerminal(response.orderId)
        assertEquals(OrderStatus.CONFIRMED, order.status)
        assertTrue(payment.authorizedOrderIds.contains(response.orderId))
        assertFalse(inventory.releasedReservationIds.contains("res-${response.orderId}")) // no compensation
    }

    @Test
    fun `checkout writes exactly one order-placed outbox row atomically with the order`() = runBlocking {
        val response = orchestrator.checkout(request(), "it-outbox-${UUID.randomUUID()}", "user-1", "user-1@example.com")

        val rows = outboxRepository.findAll().toList()
            .filter { it.aggregateId == response.orderId.toString() && it.type == "order-placed" }
        assertEquals(1, rows.size)
        // The order row is present too — the two were committed together.
        assertTrue(orderRepository.findById(response.orderId) != null)
    }

    @Test
    fun `a payment decline fails the order and compensates the seat hold`() = runBlocking {
        val response = orchestrator.checkout(request(token = DECLINE_TOKEN), "it-decline-${UUID.randomUUID()}", "user-1", "user-1@example.com")
        relayOrderPlaced(response.orderId)
        val order = awaitTerminal(response.orderId)

        assertEquals(OrderStatus.FAILED, order.status)
        assertTrue(inventory.releasedReservationIds.contains("res-${response.orderId}")) // held seats released
        assertFalse(payment.refundedOrderIds.contains(response.orderId)) // nothing captured -> no refund
    }

    @Test
    fun `a redelivered order-placed is idempotent and does not reprocess a terminal order`() = runBlocking {
        val response = orchestrator.checkout(request(), "it-redeliver-${UUID.randomUUID()}", "user-1", "user-1@example.com")
        relayOrderPlaced(response.orderId)
        val order = awaitTerminal(response.orderId)
        assertEquals(OrderStatus.CONFIRMED, order.status)
        assertEquals(1, payment.authorizedOrderIds.count { it == response.orderId })

        // Simulate Kafka at-least-once redelivery of the same order-placed event.
        relayOrderPlaced(response.orderId)
        delay(3_000) // give the consumer time to (not) reprocess

        val reloaded = orderRepository.findById(response.orderId)!!
        assertEquals(OrderStatus.CONFIRMED, reloaded.status)
        assertEquals(1, payment.authorizedOrderIds.count { it == response.orderId }) // no re-authorization
    }

    @Test
    fun `cancelling before the pipeline runs leaves the order CANCELLED and the pipeline skips it`() = runBlocking {
        val response = orchestrator.checkout(request(), "it-cancel-${UUID.randomUUID()}", "user-1", "user-1@example.com")

        // Cancel while still ACCEPTED (before relaying order-placed).
        val cancelled = orchestrator.cancel(response.orderId)
        assertEquals(OrderStatus.CANCELLED, cancelled.status)

        // The durable trigger still fires; the pipeline must skip a terminal (cancelled) order.
        relayOrderPlaced(response.orderId)
        delay(3_000)

        val reloaded = orderRepository.findById(response.orderId)!!
        assertEquals(OrderStatus.CANCELLED, reloaded.status)
        assertFalse(payment.authorizedOrderIds.contains(response.orderId)) // never charged
    }

    @Test
    fun `a duplicate idempotency key replays the same order without creating a new one`() = runBlocking {
        val key = "it-idem-${UUID.randomUUID()}"
        val first = orchestrator.checkout(request(), key, "user-1", "user-1@example.com")
        val second = orchestrator.checkout(request(), key, "user-1", "user-1@example.com")

        assertEquals(first.orderId, second.orderId)
    }

    private fun request(token: String = "tok-123") =
        CheckoutRequest(routeId = "R1", seats = listOf("1A", "1B"), totalAmount = BigDecimal("100.00"), paymentMethodToken = token)

    // Stands in for Debezium: reads the committed order-placed outbox row(s) for this order and
    // publishes the JSON payload to Kafka, keyed by aggregate id (the order id).
    private suspend fun relayOrderPlaced(orderId: UUID) {
        outboxRepository.findAll().toList()
            .filter { it.aggregateId == orderId.toString() && it.type == "order-placed" }
            .forEach { kafkaTemplate.send("order-placed", it.aggregateId, it.payload.asString()) }
    }

    private suspend fun awaitTerminal(id: UUID): Order = withTimeout(20_000) {
        var order = orderRepository.findById(id)
        while (order == null || order.status !in TERMINAL) {
            delay(200)
            order = orderRepository.findById(id)
        }
        order
    }

    private val TERMINAL = setOf(OrderStatus.CONFIRMED, OrderStatus.FAILED, OrderStatus.CANCELLED)

    /**
     * Controllable in-process doubles for the external money-path collaborators. Behaviour is derived
     * from the request (a [DECLINE_TOKEN] payment token declines), and all bookkeeping is keyed by
     * order id, so concurrent/async processing across tests never cross-contaminates.
     */
    class ControllableInventoryClient : InventoryClient {
        val reservedOrderIds = CopyOnWriteArrayList<UUID>()
        val releasedReservationIds = ConcurrentHashMap.newKeySet<String>()

        override suspend fun reserveSeats(request: ReservationRequest, idempotencyKey: String, userId: String): ReservationResponse {
            reservedOrderIds.add(request.orderId)
            return ReservationResponse("res-${request.orderId}", ReservationStatus.HELD, LocalDateTime.now().plusMinutes(10))
        }

        override suspend fun releaseSeats(reservationId: String, idempotencyKey: String) {
            releasedReservationIds.add(reservationId)
        }
    }

    class ControllablePaymentClient : PaymentClient {
        val authorizedOrderIds = CopyOnWriteArrayList<UUID>()
        val refundedOrderIds = CopyOnWriteArrayList<UUID>()

        override suspend fun authorizePayment(request: PaymentRequest, idempotencyKey: String, correlationId: String): PaymentResponse {
            val declined = request.paymentMethodToken == DECLINE_TOKEN
            if (!declined) authorizedOrderIds.add(request.orderId)
            return PaymentResponse(
                status = if (declined) PaymentStatus.DECLINED else PaymentStatus.AUTHORIZED,
                providerReference = "test-ref",
                errorCode = if (declined) "DECLINED" else null
            )
        }

        override suspend fun refund(orderId: UUID, idempotencyKey: String): PaymentRefundResponse {
            refundedOrderIds.add(orderId)
            return PaymentRefundResponse(RefundStatus.REFUNDED, "refund-ref", null)
        }
    }

    @TestConfiguration
    class TestDoubles {
        @Bean @Primary fun inventoryDouble() = ControllableInventoryClient()

        @Bean @Primary fun paymentDouble() = ControllablePaymentClient()

        // No real Keycloak in the test: back off the network-fetching decoder. It is never invoked
        // because these tests drive the orchestrator directly rather than over HTTP.
        @Bean @Primary fun testJwtDecoder(): ReactiveJwtDecoder = ReactiveJwtDecoder { Mono.empty() }
    }
}
