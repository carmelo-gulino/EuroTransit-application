package it.polito.cpo.config

import org.apache.kafka.common.serialization.StringSerializer
import org.springframework.boot.kafka.autoconfigure.KafkaProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.core.DefaultKafkaProducerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.core.ProducerFactory
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer
import org.springframework.kafka.listener.DefaultErrorHandler
import org.springframework.util.backoff.FixedBackOff

@Configuration
class KafkaConfiguration {

    // String producer/template. Orders no longer produces money-path events directly — they are
    // written to the transactional outbox and relayed to Kafka by Debezium. This String template is
    // retained for the consumer-side dead-letter recoverer below (and mirrors the String + manual
    // parse convention the consumers use, avoiding the Kafka JsonSerializer's Jackson 2 java.time gap).
    @Bean
    fun producerFactory(properties: KafkaProperties): ProducerFactory<String, String> {
        val configProps = properties.buildProducerProperties()
        return DefaultKafkaProducerFactory(configProps, StringSerializer(), StringSerializer())
    }

    @Bean
    fun kafkaTemplate(producerFactory: ProducerFactory<String, String>): KafkaTemplate<String, String> {
        val template = KafkaTemplate(producerFactory)
        // Emit producer observation spans so `traceparent` is injected into record headers and the
        // async Kafka stages join the checkout trace end-to-end (slo-observability.md §Tracing).
        template.setObservationEnabled(true)
        return template
    }

    // Poison/persistently-failing records: after a bounded set of retries, publish to `<topic>.DLT`
    // instead of redelivering forever (ADR-011 follow-up). Spring Boot wires this into the default
    // listener container factory.
    @Bean
    fun kafkaErrorHandler(kafkaTemplate: KafkaTemplate<String, String>): DefaultErrorHandler {
        val recoverer = DeadLetterPublishingRecoverer(kafkaTemplate)
        return DefaultErrorHandler(recoverer, FixedBackOff(1_000L, 3L))
    }
}
