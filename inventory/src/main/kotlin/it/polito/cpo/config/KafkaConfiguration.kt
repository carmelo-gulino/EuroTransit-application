package it.polito.cpo.config

import org.apache.kafka.common.serialization.StringSerializer
import org.springframework.boot.kafka.autoconfigure.KafkaProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.core.DefaultKafkaProducerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.core.ProducerFactory
import org.springframework.kafka.support.serializer.JsonSerializer

@Configuration
class KafkaConfiguration {

    @Bean
    fun producerFactory(properties: KafkaProperties): ProducerFactory<String, Any> {
        val configProps = properties.buildProducerProperties()
        return DefaultKafkaProducerFactory(
            configProps,
            StringSerializer(),
            JsonSerializer<Any>()
        )
    }

    @Bean
    fun kafkaTemplate(producerFactory: ProducerFactory<String, Any>): KafkaTemplate<String, Any> {
        return KafkaTemplate(producerFactory)
    }
}
