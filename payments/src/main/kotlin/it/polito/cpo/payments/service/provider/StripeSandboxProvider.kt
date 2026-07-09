package it.polito.cpo.payments.service.provider

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.http.HttpHeaders
import org.springframework.http.MediaType
import org.springframework.stereotype.Component
import org.springframework.util.LinkedMultiValueMap
import org.springframework.web.reactive.function.BodyInserters
import org.springframework.web.reactive.function.client.WebClient
import org.springframework.web.reactive.function.client.WebClientResponseException
import org.springframework.web.reactive.function.client.awaitBody
import org.springframework.http.client.reactive.ReactorClientHttpConnector
import reactor.netty.http.client.HttpClient
import java.time.Duration
import java.math.BigDecimal

@Component
class StripeSandboxProvider(
    @Value("\${payments.provider.sandbox-url}") private val sandboxUrl: String,
    @Value("\${payments.provider.secret-key}") private val secretKey: String
) : IPaymentProvider {

    private val log = LoggerFactory.getLogger(javaClass)
    
    // Configuro il timeout a livello di connessione HTTP (10 secondi)
    private val httpClient = HttpClient.create()
        .responseTimeout(Duration.ofSeconds(10))

    // Costruiamo il client HTTP agganciandogli la chiave segreta e il timeout
    private val webClient = WebClient.builder()
        .clientConnector(ReactorClientHttpConnector(httpClient))
        .baseUrl(sandboxUrl)
        .defaultHeader(HttpHeaders.AUTHORIZATION, "Bearer $secretKey")
        .build()

    override suspend fun authorize(
        amount: BigDecimal,
        currency: String,
        paymentMethodToken: String,
        idempotencyKey: String
    ): ProviderAuthorizationResult {
        log.info("Sending charge request to Stripe Sandbox for amount $amount $currency")
        
        val formData = LinkedMultiValueMap<String, String>()
        // Stripe expects amount in cents
        formData.add("amount", amount.multiply(BigDecimal(100)).toLong().toString())
        formData.add("currency", currency.lowercase())
        formData.add("source", paymentMethodToken)
        // capture defaults to true: authorize and charge in one call

        return try {
            val response = webClient.post()
                .uri("/charges")
                .header("Idempotency-Key", "stripe_$idempotencyKey")
                .contentType(MediaType.APPLICATION_FORM_URLENCODED)
                .body(BodyInserters.fromFormData(formData))
                .retrieve()
                .awaitBody<StripeChargeResponse>()

            ProviderAuthorizationResult(
                success = true,
                providerReference = response.id
            )
        } catch (e: WebClientResponseException) {
            // Never log the raw provider response body: it may carry payment/card data (api-design.md
            // forbids raw payment data / card details in logs). Log only the whitelisted error code.
            val errorBody = try { e.getResponseBodyAs(StripeErrorResponse::class.java) } catch (_: Exception) { null }
            log.warn("Stripe API returned error: status={} code={}", e.statusCode, errorBody?.error?.code ?: "unknown")
            if (e.statusCode.value() == 402) {
                // HTTP 402 Payment Required is used by Stripe for card errors (e.g. insufficient funds)
                ProviderAuthorizationResult(
                    success = false,
                    providerReference = null,
                    errorCode = errorBody?.error?.code ?: "card_declined"
                )
            } else {
                // 5xx errors or other network issues should bubble up to trigger DEPENDENCY_FAILED
                throw e
            }
        } catch (e: Exception) {
            log.error("Network error communicating with Stripe", e)
            throw e
        }
    }

    override suspend fun refund(
        providerReference: String,
        amount: BigDecimal?,
        idempotencyKey: String
    ): ProviderRefundResult {
        log.info("Sending refund request to Stripe Sandbox for charge $providerReference")
        
        val formData = LinkedMultiValueMap<String, String>()
        formData.add("charge", providerReference)
        if (amount != null) {
            formData.add("amount", amount.multiply(BigDecimal(100)).toLong().toString())
        }

        return try {
            val response = webClient.post()
                .uri("/refunds")
                .header("Idempotency-Key", "stripe_refund_$idempotencyKey")
                .contentType(MediaType.APPLICATION_FORM_URLENCODED)
                .body(BodyInserters.fromFormData(formData))
                .retrieve()
                .awaitBody<StripeRefundResponse>()

            ProviderRefundResult(
                success = true,
                refundReference = response.id
            )
        } catch (e: WebClientResponseException) {
            // Never log the raw provider response body: it may carry payment/card data. Log only the whitelisted error code.
            val errorBody = try { e.getResponseBodyAs(StripeErrorResponse::class.java) } catch (_: Exception) { null }
            log.warn("Stripe API returned error on refund: status={} code={}", e.statusCode, errorBody?.error?.code ?: "unknown")
            ProviderRefundResult(
                success = false,
                refundReference = null,
                errorCode = errorBody?.error?.code ?: "refund_failed"
            )
        } catch (e: Exception) {
            log.error("Network error communicating with Stripe for refund", e)
            throw e
        }
    }
}

@JsonIgnoreProperties(ignoreUnknown = true)
data class StripeRefundResponse(
    val id: String?
)

@JsonIgnoreProperties(ignoreUnknown = true)
data class StripeChargeResponse(
    val id: String?
)

@JsonIgnoreProperties(ignoreUnknown = true)
data class StripeErrorResponse(
    val error: StripeErrorDetails?
)

@JsonIgnoreProperties(ignoreUnknown = true)
data class StripeErrorDetails(
    val code: String?,
    val message: String?
)
