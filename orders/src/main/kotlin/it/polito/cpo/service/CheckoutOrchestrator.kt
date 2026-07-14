package it.polito.cpo.service

import io.r2dbc.postgresql.codec.Json
import tools.jackson.databind.ObjectMapper
import it.polito.cpo.client.InventoryClient
import it.polito.cpo.client.PaymentClient
import it.polito.cpo.contracts.payments.PaymentRequest
import it.polito.cpo.contracts.payments.PaymentStatus
import it.polito.cpo.contracts.payments.RefundStatus
import it.polito.cpo.contracts.inventory.ReservationRequest
import it.polito.cpo.contracts.inventory.ReservationStatus
import it.polito.cpo.controller.dtos.CheckoutRequest
import it.polito.cpo.controller.dtos.CheckoutResponse
import it.polito.cpo.contracts.events.NotificationRequestedEvent
import it.polito.cpo.contracts.events.NotificationRequestedPayload
import it.polito.cpo.contracts.events.OrderConfirmedEvent
import it.polito.cpo.contracts.events.OrderPlacedEvent
import it.polito.cpo.model.Order
import it.polito.cpo.model.OrderStatus
import it.polito.cpo.model.OutboxEvent
import it.polito.cpo.observability.ApiException
import it.polito.cpo.observability.CorrelationId
import it.polito.cpo.repository.OutboxEventRepository
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.reactor.ReactorContext
import org.slf4j.LoggerFactory
import org.springframework.http.HttpStatus
import org.springframework.stereotype.Service
import org.springframework.transaction.reactive.TransactionalOperator
import org.springframework.transaction.reactive.executeAndAwait
import java.security.MessageDigest
import java.time.LocalDateTime
import java.util.UUID

/**
 * Checkout money-path coordinator.
 *
 * The synchronous entry ([checkout]) accepts the request quickly (202), and — in a single DB
 * transaction — persists the order in ACCEPTED, records the caller idempotency record, and writes the
 * `order-placed` outbox row. Debezium relays that outbox row to Kafka, so the DB write and the event
 * are atomic (no dual-write). The reserve -> pay -> confirm pipeline is NOT run inline: it is driven
 * by the `order-placed` Kafka consumer ([processOrderPlaced]) so it survives restarts. Kafka
 * redelivery is at-least-once, so processing is made idempotent by the order state (terminal orders
 * are skipped) and by deriving the downstream idempotency keys from the immutable order id.
 *
 * Forward state transitions in the pipeline use atomic conditional updates guarded by
 * `status <> CANCELLED`, so a concurrent customer cancellation (allowed only while unpaid) can never
 * be clobbered: a cancellation that commits mid-pipeline makes the next transition affect 0 rows,
 * which aborts the pipeline into compensation (release seats + refund if already authorized).
 */
@Service
class CheckoutOrchestrator(
    private val orderService: OrderService,
    private val inventoryClient: InventoryClient,
    private val paymentClient: PaymentClient,
    private val outboxEventRepository: OutboxEventRepository,
    private val objectMapper: ObjectMapper,
    private val meterRegistry: MeterRegistry,
    private val txOperator: TransactionalOperator,
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

        // 3. Persist order + idempotency + the order-placed outbox row atomically (fixes the
        //    order/idempotency and order/Kafka dual-write). Debezium relays the outbox row.
        val response = CheckoutResponse(orderId, OrderStatus.ACCEPTED)
        val responseBody = objectMapper.writeValueAsString(response)
        txOperator.executeAndAwait {
            orderService.saveOrder(order)
            orderService.saveIdempotentRequest(idempotencyKey, responseBody, userId, "checkout", fingerprint)
            outboxEventRepository.save(
                outbox(
                    "order-placed", orderId,
                    OrderPlacedEvent(correlationId = correlationId, orderId = orderId, principalId = userId),
                )
            )
        }

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

        // Step 1: RESERVING. If the order was cancelled before the pipeline picked it up, abort.
        if (orderService.markReserving(orderId) == 0L) {
            logger.info("Order {} cancelled before pipeline start; skipping", orderId)
            sample.stop(meterRegistry.timer("orders.checkout.duration", "outcome", "cancelled"))
            return
        }

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

            // Step 3: PAYMENT_PENDING, persisting the reservation id. Abort if cancelled meanwhile.
            if (orderService.markPaymentPending(orderId, reservationId) == 0L) {
                throw IllegalStateException("Order cancelled before payment")
            }

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

            // Step 5: CONFIRMED + events, atomically. If the order was cancelled after the payment
            // was authorized, markConfirmed affects 0 rows -> abort so the catch block refunds.
            val confirmed = txOperator.executeAndAwait {
                if (orderService.markConfirmed(orderId) == 0L) {
                    false
                } else {
                    outboxEventRepository.save(
                        outbox(
                            "order-confirmed", orderId,
                            OrderConfirmedEvent(correlationId = correlationId, orderId = orderId, principalId = order.userId),
                        )
                    )
                    outboxEventRepository.save(
                        outbox(
                            "notification-requested", orderId,
                            NotificationRequestedEvent(
                                correlationId = correlationId,
                                orderId = orderId,
                                principalId = order.userId,
                                payload = NotificationRequestedPayload(
                                    recipientEmail = recipientEmail(order),
                                    message = "Your order $orderId has been successfully confirmed!"
                                )
                            ),
                        )
                    )
                    true
                }
            }
            if (confirmed != true) {
                throw IllegalStateException("Order cancelled before confirmation")
            }

            outcome = "confirmed"
            meterRegistry.counter("orders.checkout.completed", "result", "confirmed").increment()
            logger.info("Successfully confirmed order: {}", orderId)

        } catch (e: Exception) {
            val reason = classifyFailure(e, paymentAuthorized)
            outcome = reason
            meterRegistry.counter("orders.checkout.completed", "result", "failed").increment()
            meterRegistry.counter("orders.checkout.failed", "reason", reason).increment()

            // Fix #2: money-taken-but-not-confirmed window (incl. a cancellation that landed after
            // authorization). Issue an automated refund; it is idempotent on the order id, so a Kafka
            // re-drive is safe. A refund that does not complete is surfaced loudly for reconciliation.
            if (paymentAuthorized) {
                try {
                    val refund = paymentClient.refund(orderId, "refund-$orderId")
                    if (refund.status == RefundStatus.REFUNDED) {
                        logger.info("Refund completed for order {} (ref {})", orderId, refund.refundReference)
                        meterRegistry.counter("orders.refund", "result", "refunded").increment()
                    } else {
                        logger.error(
                            "Refund NOT completed for order {}: {} — manual reconciliation required",
                            orderId, refund.status,
                        )
                        meterRegistry.counter("orders.refund", "result", "failed").increment()
                    }
                } catch (rex: Exception) {
                    logger.error("Refund call failed for order {} — manual reconciliation required", orderId, rex)
                    meterRegistry.counter("orders.refund", "result", "error").increment()
                }
            } else {
                logger.warn("Checkout pipeline failed for order: {}, rolling back. Reason: {}", orderId, e.message)
            }

            // Compensation: Release Inventory if held (idempotent on inventory; may also be released
            // by a concurrent cancellation).
            if (reservationId != null) {
                try {
                    logger.info("Executing compensation: releasing seats reservation: {}", reservationId)
                    inventoryClient.releaseSeats(reservationId, "rel-$orderId")
                } catch (compEx: Exception) {
                    logger.error("Failed to release seats for reservation: {} during rollback", reservationId, compEx)
                }
            }

            // Mark FAILED + notify, unless the order was cancelled meanwhile (do not clobber CANCELLED).
            val failed = txOperator.executeAndAwait {
                if (orderService.markFailed(orderId) == 0L) {
                    false
                } else {
                    outboxEventRepository.save(
                        outbox(
                            "notification-requested", orderId,
                            NotificationRequestedEvent(
                                correlationId = correlationId,
                                orderId = orderId,
                                principalId = order.userId,
                                payload = NotificationRequestedPayload(
                                    recipientEmail = recipientEmail(order),
                                    message = "Your order $orderId failed. Reason: ${e.message}"
                                )
                            ),
                        )
                    )
                    true
                }
            }
            if (failed != true) {
                logger.info("Order {} was cancelled during the pipeline; left CANCELLED after compensation", orderId)
            }
        } finally {
            sample.stop(meterRegistry.timer("orders.checkout.duration", "outcome", outcome))
        }
    }

    /**
     * Cancels an order — allowed only while it is unpaid (ACCEPTED/RESERVING/PAYMENT_PENDING). Once
     * CONFIRMED the order is paid and cannot be cancelled. Ownership is enforced by the controller.
     * Returns the cancelled order. Throws [ApiException] 409 if the order is not cancellable.
     */
    suspend fun cancel(orderId: UUID): Order {
        // Atomic conditional cancel: race-free against the async pipeline.
        if (orderService.cancelIfUnpaid(orderId) == 0L) {
            val current = orderService.getOrderById(orderId)
                ?: throw ApiException(HttpStatus.NOT_FOUND, "ORDER_NOT_FOUND", "Order not found")
            throw ApiException(
                HttpStatus.CONFLICT,
                "ORDER_NOT_CANCELLABLE",
                "Order in state ${current.status} cannot be cancelled (already paid or terminal)",
            )
        }

        val order = orderService.getOrderById(orderId)
            ?: throw ApiException(HttpStatus.NOT_FOUND, "ORDER_NOT_FOUND", "Order not found")

        // Release any held seats (best-effort; release is idempotent, so overlapping with the
        // pipeline's own compensation is safe).
        order.reservationId?.let { rid ->
            try {
                inventoryClient.releaseSeats(rid, "cancel-$orderId")
            } catch (e: Exception) {
                logger.error("Failed to release seats for cancelled order {} (reservation {})", orderId, rid, e)
            }
        }

        meterRegistry.counter("orders.cancel", "result", "cancelled").increment()
        logger.info("Order {} cancelled", orderId)
        return order
    }

    // Builds an outbox row carrying the serialized event as its JSON payload. Debezium's EventRouter
    // routes by the `type` column to the matching topic and keys by `aggregate_id` (the order id).
    private fun outbox(type: String, orderId: UUID, event: Any): OutboxEvent = OutboxEvent(
        aggregateType = "Order",
        aggregateId = orderId.toString(),
        type = type,
        payload = Json.of(objectMapper.writeValueAsString(event)),
    )

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
        e is IllegalStateException && e.message?.contains("cancelled", ignoreCase = true) == true -> "cancelled"
        e is IllegalStateException && e.message?.contains("reservation rejected", ignoreCase = true) == true -> "reservation_rejected"
        e is IllegalStateException && e.message?.contains("declined", ignoreCase = true) == true -> "payment_declined"
        e is IllegalStateException && e.message?.contains("payment method token", ignoreCase = true) == true -> "payment_token_missing"
        else -> "dependency_error"
    }
}
