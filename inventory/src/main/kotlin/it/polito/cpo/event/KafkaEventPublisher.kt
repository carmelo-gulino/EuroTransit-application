package it.polito.cpo.event

import it.polito.cpo.contracts.events.InventoryFailedEvent
import it.polito.cpo.contracts.events.InventoryReservedEvent
import org.slf4j.LoggerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Component

@Component
class KafkaEventPublisher(
    private val kafkaTemplate: KafkaTemplate<String, Any>
) {
    private val logger = LoggerFactory.getLogger(KafkaEventPublisher::class.java)

    fun publishInventoryReserved(event: InventoryReservedEvent) {
        logger.info("Publishing inventory-reserved event for order: {} with correlation ID: {}", event.orderId, event.correlationId)
        kafkaTemplate.send("inventory-reserved", event.orderId.toString(), event)
    }

    fun publishInventoryFailed(event: InventoryFailedEvent) {
        logger.info("Publishing inventory-failed event for order: {} with correlation ID: {}", event.orderId, event.correlationId)
        kafkaTemplate.send("inventory-failed", event.orderId.toString(), event)
    }
}
