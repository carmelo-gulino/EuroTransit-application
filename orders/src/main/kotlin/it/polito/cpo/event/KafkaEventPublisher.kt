package it.polito.cpo.event

import it.polito.cpo.contracts.events.NotificationRequestedEvent
import it.polito.cpo.contracts.events.OrderConfirmedEvent
import it.polito.cpo.contracts.events.OrderPlacedEvent
import org.slf4j.LoggerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Component
import tools.jackson.databind.ObjectMapper

/**
 * Publishes money-path events as JSON strings, serialized with the app's Jackson 3 ObjectMapper
 * (which handles java.time). Consumers parse the String back into the shared contract DTO — keeping
 * producer and consumer symmetric and avoiding the Kafka JsonSerializer's Jackson 2 java.time gap.
 */
@Component
class KafkaEventPublisher(
    private val kafkaTemplate: KafkaTemplate<String, String>,
    private val objectMapper: ObjectMapper,
) {
    private val logger = LoggerFactory.getLogger(KafkaEventPublisher::class.java)

    fun publishOrderPlaced(event: OrderPlacedEvent) {
        logger.info("Publishing order-placed event for order: {} with correlation ID: {}", event.orderId, event.correlationId)
        kafkaTemplate.send("order-placed", event.orderId.toString(), objectMapper.writeValueAsString(event))
    }

    fun publishOrderConfirmed(event: OrderConfirmedEvent) {
        logger.info("Publishing order-confirmed event for order: {} with correlation ID: {}", event.orderId, event.correlationId)
        kafkaTemplate.send("order-confirmed", event.orderId.toString(), objectMapper.writeValueAsString(event))
    }

    fun publishNotificationRequested(event: NotificationRequestedEvent) {
        logger.info("Publishing notification-requested event for order: {} with correlation ID: {}", event.orderId, event.correlationId)
        kafkaTemplate.send("notification-requested", event.orderId.toString(), objectMapper.writeValueAsString(event))
    }
}
