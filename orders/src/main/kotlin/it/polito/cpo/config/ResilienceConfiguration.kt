package it.polito.cpo.config

import io.github.resilience4j.bulkhead.BulkheadConfig
import io.github.resilience4j.bulkhead.BulkheadRegistry
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry
import io.github.resilience4j.retry.RetryConfig
import io.github.resilience4j.retry.RetryRegistry
import io.github.resilience4j.core.IntervalFunction
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.web.reactive.function.client.WebClientRequestException
import org.springframework.web.reactive.function.client.WebClientResponseException
import java.time.Duration
import java.util.concurrent.TimeoutException

/**
 * Explicit Resilience4j instances for the money-path clients (inventory, payments), applied to the
 * WebClient reactive chains. Deliberately NOT `ofDefaults()`: the defaults (minimumNumberOfCalls=100,
 * 60s open state, retry-on-everything) never trip at checkout volumes and would retry business
 * declines / 4xx. Values are a starting point, tuned per dependency and meant to be adjusted after
 * load testing. Note: a payment *decline* is an HTTP 200 with status=DECLINED (not an exception),
 * so it never counts as a circuit-breaker failure; only transport faults / timeouts / 5xx do.
 */
@Configuration
class ResilienceConfiguration {

    @Bean
    fun circuitBreakerRegistry(): CircuitBreakerRegistry {
        val registry = CircuitBreakerRegistry.ofDefaults()
        registry.circuitBreaker("inventory", circuitBreakerConfig(slowCall = Duration.ofSeconds(2)))
        registry.circuitBreaker("payments", circuitBreakerConfig(slowCall = Duration.ofSeconds(3)))
        return registry
    }

    @Bean
    fun retryRegistry(): RetryRegistry {
        val registry = RetryRegistry.ofDefaults()
        registry.retry("inventory", retryConfig())
        registry.retry("payments", retryConfig())
        return registry
    }

    @Bean
    fun bulkheadRegistry(): BulkheadRegistry {
        val registry = BulkheadRegistry.ofDefaults()
        registry.bulkhead("inventory", bulkheadConfig())
        registry.bulkhead("payments", bulkheadConfig())
        return registry
    }

    private fun circuitBreakerConfig(slowCall: Duration): CircuitBreakerConfig =
        CircuitBreakerConfig.custom()
            // Time-based window so the breaker reacts at low checkout volumes, not after 100 calls.
            .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.TIME_BASED)
            .slidingWindowSize(20) // seconds
            .minimumNumberOfCalls(5)
            .failureRateThreshold(50f)
            .slowCallDurationThreshold(slowCall)
            .slowCallRateThreshold(50f)
            .waitDurationInOpenState(Duration.ofSeconds(10))
            .permittedNumberOfCallsInHalfOpenState(3)
            // 4xx means "our request was wrong", not "the dependency is down": don't trip the breaker.
            .ignoreExceptions(
                WebClientResponseException.BadRequest::class.java,
                WebClientResponseException.Conflict::class.java,
                WebClientResponseException.NotFound::class.java,
            )
            .build()

    private fun retryConfig(): RetryConfig =
        RetryConfig.custom<Any>()
            .maxAttempts(3) // 1 initial + 2 retries
            .intervalFunction(IntervalFunction.ofExponentialRandomBackoff(Duration.ofMillis(200), 2.0, 0.5))
            // Retry only transient faults; never declines/4xx, and never CallNotPermittedException.
            .retryExceptions(
                TimeoutException::class.java,
                WebClientRequestException::class.java, // connection reset / DNS / refused
                WebClientResponseException.InternalServerError::class.java,
                WebClientResponseException.BadGateway::class.java,
                WebClientResponseException.ServiceUnavailable::class.java,
                WebClientResponseException.GatewayTimeout::class.java,
            )
            .build()

    private fun bulkheadConfig(): BulkheadConfig =
        BulkheadConfig.custom()
            .maxConcurrentCalls(20)
            .maxWaitDuration(Duration.ZERO) // fail fast when saturated rather than queueing
            .build()
}
