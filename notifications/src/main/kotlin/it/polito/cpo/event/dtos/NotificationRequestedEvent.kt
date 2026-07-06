package it.polito.cpo.event.dtos

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import java.time.LocalDateTime
import java.util.UUID

/**
 * Consumer-side view of the `notification-requested` event.
 *
 * Mirrors the ACTUAL wire shape emitted by Orders
 * (`orders/.../event/dtos/EventDtos.kt`): flat fields, `payloadVersion` (not the frozen
 * doc's `schemaVersion`), `occurredAt` as a local date-time. `@JsonIgnoreProperties`
 * keeps this forward-compatible if the producer adds fields. This is intentionally a
 * module-local copy — introducing a shared contracts module would mean editing Orders,
 * which this slice does not own.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
data class NotificationRequestedEvent(
    val eventId: UUID,
    val eventType: String,
    val occurredAt: LocalDateTime,
    val correlationId: String,
    val orderId: UUID,
    val principalId: String,
    val payloadVersion: Int = 1,
    val recipientEmail: String,
    val message: String,
)
