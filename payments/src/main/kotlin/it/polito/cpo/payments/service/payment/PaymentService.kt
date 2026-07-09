package it.polito.cpo.payments.service.payment

import tools.jackson.databind.ObjectMapper
import it.polito.cpo.payments.service.provider.IPaymentProvider
import it.polito.cpo.contracts.payments.PaymentResponse
import it.polito.cpo.contracts.payments.PaymentStatus
import it.polito.cpo.payments.model.IdempotencyRecord
import it.polito.cpo.payments.model.PaymentAuthorization
import it.polito.cpo.payments.repository.IdempotencyRepository
import it.polito.cpo.payments.repository.PaymentAuthorizationRepository
import kotlinx.coroutines.reactor.awaitSingleOrNull
import org.slf4j.LoggerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Service
import org.springframework.transaction.ReactiveTransactionManager
import org.springframework.transaction.reactive.TransactionalOperator
import org.springframework.transaction.reactive.executeAndAwait
import java.math.BigDecimal
import java.time.LocalDateTime
import java.util.UUID

@Service
class PaymentService(
    private val providerAdapter: IPaymentProvider,
    private val idempotencyRepository: IdempotencyRepository,
    private val paymentAuthorizationRepository: PaymentAuthorizationRepository,
    private val paymentRefundRepository: it.polito.cpo.payments.repository.PaymentRefundRepository,
    private val kafkaTemplate: KafkaTemplate<String, Any>,
    private val objectMapper: ObjectMapper,
    transactionManager: ReactiveTransactionManager
) : IPaymentService {
    private val log = LoggerFactory.getLogger(javaClass)
    private val txOperator = TransactionalOperator.create(transactionManager)

    companion object {
        private val ISO_CURRENCY_PATTERN = Regex("^[A-Z]{3}$")
    }

    override suspend fun authorize(
        orderId: String,
        principalId: String,
        amount: BigDecimal,
        currency: String,
        paymentMethodToken: String,
        idempotencyKey: String
    ): PaymentResponse {

        // Validate before touching the DB or the provider: a malformed amount/currency is never a valid charge.
        if (amount <= BigDecimal.ZERO) {
            return PaymentResponse(PaymentStatus.DECLINED, null, "INVALID_AMOUNT")
        }
        if (!ISO_CURRENCY_PATTERN.matches(currency)) {
            return PaymentResponse(PaymentStatus.DECLINED, null, "INVALID_CURRENCY")
        }

        val fingerprint = "$orderId-$amount-$currency-$paymentMethodToken"

        // 1. Check idempotency record
        val existingRecord = idempotencyRepository.findById(idempotencyKey)
        if (existingRecord != null) {
            if (existingRecord.requestFingerprint != fingerprint || existingRecord.principalId != principalId) {
                log.warn("Idempotency conflict for key $idempotencyKey")
                return PaymentResponse(PaymentStatus.CONFLICT, null, "IDEMPOTENCY_CONFLICT")
            }
            return objectMapper.readValue(existingRecord.responseBody, PaymentResponse::class.java)
        }

        // 2. Call provider (outside any DB transaction)
        val result = try {
            providerAdapter.authorize(amount, currency, paymentMethodToken, idempotencyKey)
        } catch (e: Exception) {
            log.error("Payment provider failed", e)
            return PaymentResponse(PaymentStatus.DEPENDENCY_FAILED, null, "PROVIDER_UNAVAILABLE")
        }

        val authStatus = if (result.success) PaymentStatus.AUTHORIZED else PaymentStatus.DECLINED

        val authorization = PaymentAuthorization(
            orderId = orderId,
            principalId = principalId,
            amount = amount,
            currency = currency,
            status = authStatus.name,
            providerReference = result.providerReference,
            idempotencyKey = idempotencyKey
        ).apply { setAsNew(true) }

        val response = PaymentResponse(authStatus, result.providerReference, result.errorCode)
        val idempotencyRecord = IdempotencyRecord(
            idempotencyKey = idempotencyKey,
            operation = "authorize",
            principalId = principalId,
            requestFingerprint = fingerprint,
            responseCode = if (result.success) 200 else 422,
            responseBody = objectMapper.writeValueAsString(response),
            createdAt = LocalDateTime.now()
        ).apply { setAsNew(true) }

        // 3+4. Persist authorization and idempotency record atomically, in a short transaction
        txOperator.executeAndAwait {
            paymentAuthorizationRepository.save(authorization)
            idempotencyRepository.save(idempotencyRecord)
        }

        // 5. Emit event
        val eventType = if (result.success) "payment-authorized" else "payment-declined"
        publishEvent(eventType, orderId, principalId, authorization)

        return response
    }

    override suspend fun refund(
        orderId: String,
        amount: BigDecimal?,
        idempotencyKey: String
    ): it.polito.cpo.payments.dto.PaymentRefundResponse {

        // null amount means full refund; an explicit amount must be strictly positive.
        if (amount != null && amount <= BigDecimal.ZERO) {
            return it.polito.cpo.payments.dto.PaymentRefundResponse(IPaymentService.RefundStatus.FAILED, null, "INVALID_AMOUNT")
        }

        val fingerprint = "refund-$orderId-$amount"

        // 1. Check idempotency record
        val existingRecord = idempotencyRepository.findById(idempotencyKey)
        if (existingRecord != null) {
            if (existingRecord.requestFingerprint != fingerprint) {
                log.warn("Idempotency conflict for key $idempotencyKey")
                return it.polito.cpo.payments.dto.PaymentRefundResponse(IPaymentService.RefundStatus.CONFLICT, null, "IDEMPOTENCY_CONFLICT")
            }
            return objectMapper.readValue(existingRecord.responseBody, it.polito.cpo.payments.dto.PaymentRefundResponse::class.java)
        }

        // 2. Fetch original authorization
        val authorization = paymentAuthorizationRepository.findByOrderId(orderId)
            ?: return it.polito.cpo.payments.dto.PaymentRefundResponse(IPaymentService.RefundStatus.NOT_FOUND, null, "AUTHORIZATION_NOT_FOUND")

        if (authorization.status != PaymentAuthorization.STATUS_AUTHORIZED || authorization.providerReference == null) {
            return it.polito.cpo.payments.dto.PaymentRefundResponse(IPaymentService.RefundStatus.FAILED, null, "INVALID_AUTHORIZATION_STATUS")
        }

        // 3. Call provider (outside any DB transaction)
        val result = try {
            providerAdapter.refund(authorization.providerReference, amount, idempotencyKey)
        } catch (e: Exception) {
            log.error("Payment provider failed on refund", e)
            return it.polito.cpo.payments.dto.PaymentRefundResponse(IPaymentService.RefundStatus.DEPENDENCY_FAILED, null, "PROVIDER_UNAVAILABLE")
        }

        val refundStatus = if (result.success) IPaymentService.RefundStatus.REFUNDED else IPaymentService.RefundStatus.FAILED

        val paymentRefund = it.polito.cpo.payments.model.PaymentRefund(
            authorizationId = authorization.getId(), // We must use getId() since id is private
            amount = amount,
            currency = authorization.currency,
            status = refundStatus.name,
            providerReference = result.refundReference,
            idempotencyKey = idempotencyKey
        ).apply { setAsNew(true) }

        val response = it.polito.cpo.payments.dto.PaymentRefundResponse(refundStatus, result.refundReference, result.errorCode)
        val idempotencyRecord = IdempotencyRecord(
            idempotencyKey = idempotencyKey,
            operation = "refund",
            principalId = authorization.principalId,
            requestFingerprint = fingerprint,
            responseCode = if (result.success) 200 else 422,
            responseBody = objectMapper.writeValueAsString(response),
            createdAt = LocalDateTime.now()
        ).apply { setAsNew(true) }

        // 4+5. Persist refund record and idempotency record atomically, in a short transaction
        txOperator.executeAndAwait {
            paymentRefundRepository.save(paymentRefund)
            idempotencyRepository.save(idempotencyRecord)
        }

        // 6. Emit event
        val eventType = if (result.success) "payment-refunded" else "payment-refund-failed"
        publishEvent(eventType, orderId, authorization.principalId, paymentRefund)

        return response
    }

    private fun publishEvent(eventType: String, orderId: String, principalId: String, payload: Any) {
        val event = mapOf(
            "eventId" to java.util.UUID.randomUUID().toString(),
            "eventType" to eventType,
            "schemaVersion" to 1,
            "occurredAt" to LocalDateTime.now().toString(),
            "correlationId" to (org.slf4j.MDC.get("correlationId") ?: ""),
            "orderId" to orderId,
            "principalId" to principalId,
            "payload" to payload
        )
        kafkaTemplate.send(eventType, orderId, event)
    }
}
