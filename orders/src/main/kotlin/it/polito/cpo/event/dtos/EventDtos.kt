package it.polito.cpo.event.dtos

import java.time.LocalDateTime
import java.util.UUID

data class OrderPlacedEvent(
    val eventId: UUID = UUID.randomUUID(),
    val eventType: String = "order-placed",
    val occurredAt: LocalDateTime = LocalDateTime.now(),
    val correlationId: String,
    val orderId: UUID,
    val principalId: String,
    val payloadVersion: Int = 1
)

data class OrderConfirmedEvent(
    val eventId: UUID = UUID.randomUUID(),
    val eventType: String = "order-confirmed",
    val occurredAt: LocalDateTime = LocalDateTime.now(),
    val correlationId: String,
    val orderId: UUID,
    val principalId: String,
    val payloadVersion: Int = 1
)

data class NotificationRequestedEvent(
    val eventId: UUID = UUID.randomUUID(),
    val eventType: String = "notification-requested",
    val occurredAt: LocalDateTime = LocalDateTime.now(),
    val correlationId: String,
    val orderId: UUID,
    val principalId: String,
    val payloadVersion: Int = 1,
    val recipientEmail: String,
    val message: String
)
