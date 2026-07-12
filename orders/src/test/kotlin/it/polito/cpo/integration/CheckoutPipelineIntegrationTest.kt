package it.polito.cpo.integration

import it.polito.cpo.client.InventoryClient
import it.polito.cpo.client.PaymentClient
import it.polito.cpo.contracts.events.OrderPlacedEvent
import it.polito.cpo.contracts.inventory.ReservationRequest
import it.polito.cpo.contracts.inventory.ReservationResponse
import it.polito.cpo.contracts.inventory.ReservationStatus
import it.polito.cpo.contracts.payments.PaymentRequest
import it.polito.cpo.contracts.payments.PaymentResponse
import it.polito.cpo.contracts.payments.PaymentStatus
import it.polito.cpo.controller.dtos.CheckoutRequest
import it.polito.cpo.event.KafkaEventPublisher
import it.polito.cpo.model.Order
import it.polito.cpo.model.OrderStatus
import it.polito.cpo.repository.OrderRepository
import it.polito.cpo.service.CheckoutOrchestrator
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Import
import org.springframework.context.annotation.Primary
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
 * infrastructure: a Testcontainers Postgres (R2DBC at runtime + Flyway migrations V1/V2 at startup)
 * and an embedded Kafka broker. Only the external money-path collaborators (inventory, payments)
 * are replaced by controllable in-process doubles.
 *
 * It exercises the real path: `checkout()` persists ACCEPTED and publishes `order-placed`, the real
 * `OrderPlacedEventListener` consumes it, and the pipeline drives the order to a terminal state.
 * This is what unit tests cannot prove: the produce->consume serialization roundtrip, the Flyway
 * schema, and idempotent recovery under Kafka at-least-once redelivery.
 *
 * The doubles are deterministic per request/order (a `DECLINE` payment token triggers a decline; all
 * bookkeeping is keyed by order id) so tests stay isolated despite the shared singleton beans and the
 * asynchronous consumer thread — no shared mutable "next outcome" flag to race on.
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
    @Autowired private lateinit var eventPublisher: KafkaEventPublisher
    @Autowired private lateinit var inventory: ControllableInventoryClient
    @Autowired private lateinit var payment: ControllablePaymentClient

    @Test
    fun `happy path drives the order through Kafka to CONFIRMED`() = runBlocking {
        val response = orchestrator.checkout(request(), "it-happy-${UUID.randomUUID()}", "user-1")
        assertEquals(OrderStatus.ACCEPTED, response.status)

        val order = awaitTerminal(response.orderId)
        assertEquals(OrderStatus.CONFIRMED, order.status)
        assertTrue(payment.authorizedOrderIds.contains(response.orderId))
        assertFalse(inventory.releasedReservationIds.contains("res-${response.orderId}")) // no compensation
    }

    @Test
    fun `a payment decline fails the order and compensates the seat hold`() = runBlocking {
        val response = orchestrator.checkout(request(token = DECLINE_TOKEN), "it-decline-${UUID.randomUUID()}", "user-1")
        val order = awaitTerminal(response.orderId)

        assertEquals(OrderStatus.FAILED, order.status)
        assertTrue(inventory.releasedReservationIds.contains("res-${response.orderId}")) // held seats released
    }

    @Test
    fun `a redelivered order-placed is idempotent and does not reprocess a terminal order`() = runBlocking {
        val response = orchestrator.checkout(request(), "it-redeliver-${UUID.randomUUID()}", "user-1")
        val order = awaitTerminal(response.orderId)
        assertEquals(OrderStatus.CONFIRMED, order.status)
        assertEquals(1, payment.authorizedOrderIds.count { it == response.orderId })

        // Simulate Kafka at-least-once redelivery of the same order-placed event.
        eventPublisher.publishOrderPlaced(
            OrderPlacedEvent(correlationId = "corr-redeliver", orderId = response.orderId, principalId = "user-1")
        )
        delay(3_000) // give the consumer time to (not) reprocess

        val reloaded = orderRepository.findById(response.orderId)!!
        assertEquals(OrderStatus.CONFIRMED, reloaded.status)
        assertEquals(1, payment.authorizedOrderIds.count { it == response.orderId }) // no re-authorization
    }

    @Test
    fun `a duplicate idempotency key replays the same order without creating a new one`() = runBlocking {
        val key = "it-idem-${UUID.randomUUID()}"
        val first = orchestrator.checkout(request(), key, "user-1")
        val second = orchestrator.checkout(request(), key, "user-1")

        assertEquals(first.orderId, second.orderId)
    }

    private fun request(token: String = "tok-123") =
        CheckoutRequest(routeId = "R1", seats = listOf("1A", "1B"), totalAmount = BigDecimal("100.00"), paymentMethodToken = token)

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

        override suspend fun authorizePayment(request: PaymentRequest, idempotencyKey: String, correlationId: String): PaymentResponse {
            val declined = request.paymentMethodToken == DECLINE_TOKEN
            if (!declined) authorizedOrderIds.add(request.orderId)
            return PaymentResponse(
                status = if (declined) PaymentStatus.DECLINED else PaymentStatus.AUTHORIZED,
                providerReference = "test-ref",
                errorCode = if (declined) "DECLINED" else null
            )
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
