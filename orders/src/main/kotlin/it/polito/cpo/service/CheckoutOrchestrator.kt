package it.polito.cpo.service

import tools.jackson.databind.ObjectMapper
import it.polito.cpo.client.InventoryClient
import it.polito.cpo.client.PaymentClient
import it.polito.cpo.contracts.payments.PaymentRequest
import it.polito.cpo.contracts.payments.PaymentStatus
import it.polito.cpo.contracts.inventory.ReservationRequest
import it.polito.cpo.contracts.inventory.ReservationStatus
import it.polito.cpo.controller.dtos.CheckoutRequest
import it.polito.cpo.controller.dtos.CheckoutResponse
import it.polito.cpo.event.KafkaEventPublisher
import it.polito.cpo.contracts.events.NotificationRequestedEvent
import it.polito.cpo.contracts.events.NotificationRequestedPayload
import it.polito.cpo.contracts.events.OrderConfirmedEvent
import it.polito.cpo.contracts.events.OrderPlacedEvent
import it.polito.cpo.model.Order
import it.polito.cpo.model.OrderStatus
import it.polito.cpo.observability.CorrelationId
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.reactor.ReactorContext
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.time.LocalDateTime
import java.util.UUID

/**
 * Checkout money-path coordinator.
 *
 * The synchronous entry ([checkout]) accepts the request quickly (202), persists the order in
 * ACCEPTED, records the caller idempotency record, and publishes `order-placed`. The reserve -> pay
 * -> confirm pipeline is NOT run inline: it is driven by the `order-placed` Kafka consumer
 * ([processOrderPlaced]) so it survives restarts. Kafka redelivery is at-least-once, so processing
 * is made idempotent by the order state (terminal orders are skipped) and by deriving the downstream
 * idempotency keys from the immutable order id.
 */
@Service
class CheckoutOrchestrator(
    private val orderService: OrderService,
    private val inventoryClient: InventoryClient,
    private val paymentClient: PaymentClient,
    private val kafkaEventPublisher: KafkaEventPublisher,
    private val objectMapper: ObjectMapper,
    private val meterRegistry: MeterRegistry
) {
    private val logger = LoggerFactory.getLogger(CheckoutOrchestrator::class.java)

    private companion object {
        val TERMINAL_STATES = setOf(OrderStatus.CONFIRMED, OrderStatus.FAILED, OrderStatus.CANCELLED)
    }

    suspend fun checkout(request: CheckoutRequest, idempotencyKey: String, userId: String): CheckoutResponse {
        // Reuse the correlation id resolved by the inbound WebFilter (falls back to a fresh one
        // when the call is not driven through the filter, e.g. unit tests) so the trace stays
        // consistent across the sync accept, the emitted event, and the async pipeline.
        val correlationId = currentCoroutineContext()[ReactorContext]?.context
            ?.getOrEmpty<String>(CorrelationId.CONTEXT_KEY)?.orElse(null)
            ?: CorrelationId.generate()

        // 1. Check idempotency
        val existing = orderService.findIdempotentRequest(idempotencyKey)
        if (existing != null) {
            logger.info("Found existing request for idempotency key: {}", idempotencyKey)
            return objectMapper.readValue(existing.responseBody, CheckoutResponse::class.java)
        }

        meterRegistry.counter("orders.checkout.started").increment()

        // 2. Create order in ACCEPTED status (keep the payment token so the async pipeline can resume)
        val orderId = UUID.randomUUID()
        val order = Order(
            id = orderId,
            userId = userId,
            status = OrderStatus.ACCEPTED,
            routeId = request.routeId,
            seats = request.seats.joinToString(","),
            totalAmount = request.totalAmount,
            createdAt = LocalDateTime.now(),
            paymentMethodToken = request.paymentMethodToken
        ).apply { setAsNew(true) }

        orderService.saveOrder(order)

        // 3. Save idempotency response
        val response = CheckoutResponse(orderId, OrderStatus.ACCEPTED)
        val responseBody = objectMapper.writeValueAsString(response)
        orderService.saveIdempotentRequest(idempotencyKey, responseBody)

        // 4. Emit order-placed: the durable trigger for the pipeline (consumed by processOrderPlaced).
        kafkaEventPublisher.publishOrderPlaced(
            OrderPlacedEvent(
                correlationId = correlationId,
                orderId = orderId,
                principalId = userId
            )
        )

        return response
    }

    /**
     * Runs the checkout pipeline for one order. Invoked by the `order-placed` consumer.
     * Idempotent under Kafka at-least-once delivery: terminal orders are skipped, and a
     * non-terminal order is re-driven from the start relying on the downstream idempotency keys.
     */
    suspend fun processOrderPlaced(event: OrderPlacedEvent) {
        val order = orderService.getOrderById(event.orderId)
        if (order == null) {
            logger.warn("Received order-placed for unknown order: {}", event.orderId)
            return
        }
        if (order.status in TERMINAL_STATES) {
            logger.info("Skipping order-placed for order {} already in terminal state {}", event.orderId, order.status)
            return
        }
        runPipeline(order, event.correlationId)
    }

    private suspend fun runPipeline(order: Order, correlationId: String) {
        val orderId = order.id
        logger.info("Starting checkout pipeline for order: {}", orderId)

        val sample = Timer.start(meterRegistry)
        var outcome = "failed"

        // Step 1: Set state to RESERVING
        order.status = OrderStatus.RESERVING
        order.setAsNew(false)
        orderService.saveOrder(order)

        var reservationId: String? = null
        try {
            // Step 2: Call Inventory hold (idempotency key derived from the order id).
            val reservationResponse = inventoryClient.reserveSeats(
                ReservationRequest(
                    orderId = orderId,
                    seats = order.seats.split(","),
                    routeId = order.routeId
                ),
                idempotencyKey = "inv-$orderId"
            )

            if (reservationResponse.status != ReservationStatus.HELD) {
                throw IllegalStateException("Seat reservation rejected: ${reservationResponse.status}")
            }
            reservationId = reservationResponse.reservationId

            // Step 3: Update state to PAYMENT_PENDING
            order.status = OrderStatus.PAYMENT_PENDING
            orderService.saveOrder(order)

            // Step 4: Call Payments authorization
            val paymentToken = order.paymentMethodToken
                ?: throw IllegalStateException("Missing payment method token for order $orderId")
            val paymentResponse = paymentClient.authorizePayment(
                PaymentRequest(
                    orderId = orderId,
                    principalId = order.userId,
                    amount = order.totalAmount,
                    currency = "EUR",
                    paymentMethodToken = paymentToken
                ),
                idempotencyKey = "pay-$orderId"
            )

            if (paymentResponse.status != PaymentStatus.AUTHORIZED) {
                throw IllegalStateException("Payment authorization declined: ${paymentResponse.status}")
            }

            // Step 5: Success! Set state to CONFIRMED and drop the payment token.
            order.status = OrderStatus.CONFIRMED
            order.paymentMethodToken = null
            orderService.saveOrder(order)

            // Step 6: Publish confirmed & notification requested events
            kafkaEventPublisher.publishOrderConfirmed(
                OrderConfirmedEvent(
                    correlationId = correlationId,
                    orderId = orderId,
                    principalId = order.userId
                )
            )

            kafkaEventPublisher.publishNotificationRequested(
                NotificationRequestedEvent(
                    correlationId = correlationId,
                    orderId = orderId,
                    principalId = order.userId,
                    payload = NotificationRequestedPayload(
                        recipientEmail = "customer@example.com",
                        message = "Your order $orderId has been successfully confirmed!"
                    )
                )
            )

            outcome = "confirmed"
            meterRegistry.counter("orders.checkout.confirmed").increment()
            logger.info("Successfully confirmed order: {}", orderId)

        } catch (e: Exception) {
            meterRegistry.counter("orders.checkout.failed").increment()
            logger.warn("Checkout pipeline failed for order: {}, rolling back. Reason: {}", orderId, e.message)

            // Compensation: Release Inventory if held
            if (reservationId != null) {
                try {
                    logger.info("Executing compensation: releasing seats reservation: {}", reservationId)
                    inventoryClient.releaseSeats(reservationId, "rel-$orderId")
                } catch (compEx: Exception) {
                    logger.error("Failed to release seats for reservation: {} during rollback", reservationId, compEx)
                }
            }

            // Mark order as FAILED and drop the payment token
            order.status = OrderStatus.FAILED
            order.paymentMethodToken = null
            orderService.saveOrder(order)

            // Notify user of failure
            kafkaEventPublisher.publishNotificationRequested(
                NotificationRequestedEvent(
                    correlationId = correlationId,
                    orderId = orderId,
                    principalId = order.userId,
                    payload = NotificationRequestedPayload(
                        recipientEmail = "customer@example.com",
                        message = "Your order $orderId failed. Reason: ${e.message}"
                    )
                )
            )
        } finally {
            sample.stop(meterRegistry.timer("orders.checkout.duration", "outcome", outcome))
        }
    }
}
