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
import it.polito.cpo.observability.ApiException
import it.polito.cpo.observability.CorrelationId
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.reactor.ReactorContext
import org.slf4j.LoggerFactory
import org.springframework.http.HttpStatus
import org.springframework.stereotype.Service
import java.security.MessageDigest
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

    suspend fun checkout(
        request: CheckoutRequest,
        idempotencyKey: String,
        userId: String,
        recipientEmail: String?,
    ): CheckoutResponse {
        // Reuse the correlation id resolved by the inbound WebFilter (falls back to a fresh one
        // when the call is not driven through the filter, e.g. unit tests) so the trace stays
        // consistent across the sync accept, the emitted event, and the async pipeline.
        val correlationId = currentCoroutineContext()[ReactorContext]?.context
            ?.getOrEmpty<String>(CorrelationId.CONTEXT_KEY)?.orElse(null)
            ?: CorrelationId.generate()

        val fingerprint = fingerprint(userId, request)

        // 1. Check idempotency. Same key + same payload -> replay the stored result; same key + a
        //    different logical payload -> 409 Conflict (api-design.md §Shared Request Headers).
        val existing = orderService.findIdempotentRequest(idempotencyKey)
        if (existing != null) {
            if (existing.requestFingerprint != null && existing.requestFingerprint != fingerprint) {
                meterRegistry.counter("orders.checkout.idempotency_conflict").increment()
                logger.warn("Idempotency conflict for key {}: fingerprint mismatch", idempotencyKey)
                throw ApiException(
                    HttpStatus.CONFLICT,
                    "IDEMPOTENCY_CONFLICT",
                    "Idempotency key was reused with a different request",
                )
            }
            meterRegistry.counter("orders.checkout.idempotent_replay").increment()
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
            paymentMethodToken = request.paymentMethodToken,
            recipientEmail = recipientEmail,
        ).apply { setAsNew(true) }

        orderService.saveOrder(order)

        // 3. Save idempotency response (scoped with principal, operation and request fingerprint).
        val response = CheckoutResponse(orderId, OrderStatus.ACCEPTED)
        val responseBody = objectMapper.writeValueAsString(response)
        orderService.saveIdempotentRequest(idempotencyKey, responseBody, userId, "checkout", fingerprint)

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

    // Canonical, order-independent fingerprint of the checkout payload (SHA-256 hex). Used to tell a
    // genuine retry (same payload -> replay) from a key reuse with a different payload (-> 409).
    private fun fingerprint(userId: String, request: CheckoutRequest): String {
        val canonical = buildString {
            append(userId).append('|')
            append(request.routeId).append('|')
            append(request.seats.sorted().joinToString(",")).append('|')
            append(request.totalAmount.stripTrailingZeros().toPlainString()).append('|')
            append(request.paymentMethodToken)
        }
        return MessageDigest.getInstance("SHA-256")
            .digest(canonical.toByteArray(Charsets.UTF_8))
            .joinToString("") { "%02x".format(it) }
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
        var paymentAuthorized = false
        try {
            // Step 2: Call Inventory hold (idempotency key derived from the order id).
            meterRegistry.counter("orders.inventory.hold.attempt").increment()
            val reservationResponse = inventoryClient.reserveSeats(
                ReservationRequest(
                    orderId = orderId,
                    seats = order.seats.split(","),
                    routeId = order.routeId
                ),
                idempotencyKey = "inv-$orderId",
                userId = order.userId
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
                idempotencyKey = "pay-$orderId",
                correlationId = correlationId
            )

            if (paymentResponse.status != PaymentStatus.AUTHORIZED) {
                throw IllegalStateException("Payment authorization declined: ${paymentResponse.status}")
            }
            paymentAuthorized = true

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
                        recipientEmail = recipientEmail(order),
                        message = "Your order $orderId has been successfully confirmed!"
                    )
                )
            )

            outcome = "confirmed"
            meterRegistry.counter("orders.checkout.completed", "result", "confirmed").increment()
            logger.info("Successfully confirmed order: {}", orderId)

        } catch (e: Exception) {
            val reason = classifyFailure(e, paymentAuthorized)
            outcome = reason
            meterRegistry.counter("orders.checkout.completed", "result", "failed").increment()
            meterRegistry.counter("orders.checkout.failed", "reason", reason).increment()

            if (paymentAuthorized) {
                // Money-taken-but-not-confirmed window: authorization succeeded but a later step failed.
                // We still release the seat and fail the order; an automated payment refund is NOT wired
                // yet (documented follow-up, see plan P3.3) — surface it loudly for reconciliation.
                logger.error(
                    "Order {} was payment-authorized but not confirmed ({}). Manual refund/reconciliation may be required.",
                    orderId, e.message,
                )
            } else {
                logger.warn("Checkout pipeline failed for order: {}, rolling back. Reason: {}", orderId, e.message)
            }

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
                        recipientEmail = recipientEmail(order),
                        message = "Your order $orderId failed. Reason: ${e.message}"
                    )
                )
            )
        } finally {
            sample.stop(meterRegistry.timer("orders.checkout.duration", "outcome", outcome))
        }
    }

    // Delivery address for the notification, captured at checkout from the JWT `email` claim. If the
    // token had no email, fall back to a placeholder and warn — the checkout must not fail for this.
    private fun recipientEmail(order: Order): String {
        val email = order.recipientEmail
        if (email.isNullOrBlank()) {
            logger.warn("Order {} has no recipient email (missing JWT email claim); notification will use a placeholder", order.id)
            return "unknown@eurotransit.invalid"
        }
        return email
    }

    // Low-cardinality failure reason for metrics/logs.
    private fun classifyFailure(e: Exception, paymentAuthorized: Boolean): String = when {
        paymentAuthorized -> "authorized_not_confirmed"
        e is IllegalStateException && e.message?.contains("reservation rejected", ignoreCase = true) == true -> "reservation_rejected"
        e is IllegalStateException && e.message?.contains("declined", ignoreCase = true) == true -> "payment_declined"
        e is IllegalStateException && e.message?.contains("payment method token", ignoreCase = true) == true -> "payment_token_missing"
        else -> "dependency_error"
    }
}
