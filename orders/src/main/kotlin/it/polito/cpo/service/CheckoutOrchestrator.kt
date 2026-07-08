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
import kotlinx.coroutines.*
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.time.LocalDateTime
import java.util.UUID

@Service
class CheckoutOrchestrator(
    private val orderService: OrderService,
    private val inventoryClient: InventoryClient,
    private val paymentClient: PaymentClient,
    private val kafkaEventPublisher: KafkaEventPublisher,
    private val objectMapper: ObjectMapper
) {
    private val logger = LoggerFactory.getLogger(CheckoutOrchestrator::class.java)
    private val orchestratorScope = CoroutineScope(SupervisorJob() + Dispatchers.Default)

    suspend fun checkout(request: CheckoutRequest, idempotencyKey: String, userId: String): CheckoutResponse {
        // 1. Check idempotency
        val existing = orderService.findIdempotentRequest(idempotencyKey)
        if (existing != null) {
            logger.info("Found existing request for idempotency key: {}", idempotencyKey)
            return objectMapper.readValue(existing.responseBody, CheckoutResponse::class.java)
        }

        // 2. Create order in ACCEPTED status
        val orderId = UUID.randomUUID()
        val order = Order(
            id = orderId,
            userId = userId,
            status = OrderStatus.ACCEPTED,
            routeId = request.routeId,
            seats = request.seats.joinToString(","),
            totalAmount = request.totalAmount,
            createdAt = LocalDateTime.now()
        ).apply { setAsNew(true) }

        orderService.saveOrder(order)

        // 3. Save idempotency response
        val response = CheckoutResponse(orderId, OrderStatus.ACCEPTED)
        val responseBody = objectMapper.writeValueAsString(response)
        orderService.saveIdempotentRequest(idempotencyKey, responseBody)

        // 4. Emit order-placed event
        val correlationId = UUID.randomUUID().toString()
        kafkaEventPublisher.publishOrderPlaced(
            OrderPlacedEvent(
                correlationId = correlationId,
                orderId = orderId,
                principalId = userId
            )
        )

        // 5. Run async pipeline in background
        orchestratorScope.launch {
            try {
                runPipeline(order, request, correlationId, idempotencyKey)
            } catch (e: Exception) {
                logger.error("Error executing checkout pipeline for order: {}", orderId, e)
            }
        }

        return response
    }

    private suspend fun runPipeline(
        order: Order,
        request: CheckoutRequest,
        correlationId: String,
        idempotencyKey: String
    ) {
        val orderId = order.id
        logger.info("Starting background checkout pipeline for order: {}", orderId)

        // Step 1: Set state to RESERVING
        order.status = OrderStatus.RESERVING
        order.setAsNew(false)
        orderService.saveOrder(order)

        var reservationId: String? = null
        try {
            // Step 2: Call Inventory hold
            val reservationResponse = inventoryClient.reserveSeats(
                ReservationRequest(
                    orderId = orderId,
                    seats = request.seats,
                    routeId = request.routeId
                ),
                idempotencyKey = "inv-$idempotencyKey"
            )

            if (reservationResponse.status != ReservationStatus.HELD) {
                throw IllegalStateException("Seat reservation rejected: ${reservationResponse.status}")
            }
            reservationId = reservationResponse.reservationId

            // Step 3: Update state to PAYMENT_PENDING
            order.status = OrderStatus.PAYMENT_PENDING
            orderService.saveOrder(order)

            // Step 4: Call Payments authorization
            val paymentResponse = paymentClient.authorizePayment(
                PaymentRequest(
                    orderId = orderId,
                    principalId = order.userId,
                    amount = order.totalAmount,
                    currency = "EUR",
                    paymentMethodToken = request.paymentMethodToken
                ),
                idempotencyKey = "pay-$idempotencyKey"
            )

            if (paymentResponse.status != it.polito.cpo.contracts.payments.PaymentStatus.AUTHORIZED) {
                throw IllegalStateException("Payment authorization declined: ${paymentResponse.status}")
            }

            // Step 5: Success! Set state to CONFIRMED
            order.status = OrderStatus.CONFIRMED
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

            logger.info("Successfully confirmed order: {}", orderId)

        } catch (e: Exception) {
            logger.warn("Checkout pipeline failed for order: {}, rolling back. Reason: {}", orderId, e.message)

            // Compensation: Release Inventory if held
            if (reservationId != null) {
                try {
                    logger.info("Executing compensation: releasing seats reservation: {}", reservationId)
                    inventoryClient.releaseSeats(reservationId, "rel-$idempotencyKey")
                } catch (compEx: Exception) {
                    logger.error("Failed to release seats for reservation: {} during rollback", reservationId, compEx)
                }
            }

            // Mark order as FAILED
            order.status = OrderStatus.FAILED
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
        }
    }
}
