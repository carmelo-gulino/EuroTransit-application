package it.polito.cpo.event

import it.polito.cpo.contracts.events.NotificationRequestedEvent
import it.polito.cpo.contracts.events.OrderConfirmedEvent
import it.polito.cpo.contracts.events.OrderPlacedEvent
import org.slf4j.LoggerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Component

@Component
class KafkaEventPublisher(
    private val kafkaTemplate: KafkaTemplate<String, Any>
) {
    private val logger = LoggerFactory.getLogger(KafkaEventPublisher::class.java)

    fun publishOrderPlaced(event: OrderPlacedEvent) {
        logger.info("Publishing order-placed event for order: {} with correlation ID: {}", event.orderId, event.correlationId)
        kafkaTemplate.send("order-placed", event.orderId.toString(), event)
    }

    fun publishOrderConfirmed(event: OrderConfirmedEvent) {
        logger.info("Publishing order-confirmed event for order: {} with correlation ID: {}", event.orderId, event.correlationId)
        kafkaTemplate.send("order-confirmed", event.orderId.toString(), event)
    }

    fun publishNotificationRequested(event: NotificationRequestedEvent) {
        logger.info("Publishing notification-requested event for order: {} with correlation ID: {}", event.orderId, event.correlationId)
        kafkaTemplate.send("notification-requested", event.orderId.toString(), event)
    }
}
