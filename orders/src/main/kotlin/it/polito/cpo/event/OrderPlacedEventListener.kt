package it.polito.cpo.event

import io.micrometer.core.instrument.MeterRegistry
import it.polito.cpo.contracts.events.OrderPlacedEvent
import it.polito.cpo.observability.CorrelationId
import it.polito.cpo.service.CheckoutOrchestrator
import kotlinx.coroutines.reactor.ReactorContext
import kotlinx.coroutines.runBlocking
import org.slf4j.LoggerFactory
import org.slf4j.MDC
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component
import reactor.util.context.Context
import tools.jackson.databind.ObjectMapper

/**
 * Consumes `order-placed` and drives the checkout pipeline. This makes the pipeline durable:
 * a crash mid-checkout is recovered because the offset is committed only after processing, so
 * Kafka redelivers and [CheckoutOrchestrator.processOrderPlaced] re-drives the (idempotent) work.
 *
 * - Parses the raw String payload with the shared contract DTO (`money-path-contracts`), matching
 *   the house style used by the notifications consumer (decoupled from the producer `__TypeId__`).
 * - Seeds the correlation id into both the logging MDC and the Reactor context, so the pipeline's
 *   outbound calls to inventory/payments carry `X-Correlation-Id` and logs stay grep-able by it.
 * - Only unparseable (poison) payloads are swallowed; a processing failure propagates so the
 *   container can retry (recovery). A dead-letter topic is a documented follow-up.
 * - Emits a low-cardinality counter `orders.events.consumed{topic,result}`.
 */
@Component
class OrderPlacedEventListener(
    private val objectMapper: ObjectMapper,
    private val orchestrator: CheckoutOrchestrator,
    private val meterRegistry: MeterRegistry,
) {
    private val log = LoggerFactory.getLogger(OrderPlacedEventListener::class.java)
    private val topic = "order-placed"

    @KafkaListener(topics = ["order-placed"], groupId = "\${spring.kafka.consumer.group-id}")
    fun onMessage(payload: String) {
        val event = try {
            objectMapper.readValue(payload, OrderPlacedEvent::class.java)
        } catch (ex: Exception) {
            log.warn("Discarding unparseable order-placed payload", ex)
            count("error")
            return
        }

        MDC.putCloseable(CorrelationId.CONTEXT_KEY, event.correlationId).use {
            runBlocking(ReactorContext(Context.of(CorrelationId.CONTEXT_KEY, event.correlationId))) {
                orchestrator.processOrderPlaced(event)
            }
            count("processed")
        }
    }

    private fun count(result: String) {
        meterRegistry.counter("orders.events.consumed", "topic", topic, "result", result).increment()
    }
}
