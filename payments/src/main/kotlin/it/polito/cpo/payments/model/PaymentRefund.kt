package it.polito.cpo.payments.model

import org.springframework.data.annotation.Id
import org.springframework.data.relational.core.mapping.Table
import java.math.BigDecimal
import java.time.LocalDateTime
import java.util.UUID

@Table("payment_refunds")
data class PaymentRefund(
    @Id
    val id: UUID = UUID.randomUUID(),
    val authorizationId: UUID,
    val amount: BigDecimal?,
    val currency: String,
    val status: String,
    val providerReference: String?,
    val idempotencyKey: String,
    val createdAt: LocalDateTime = LocalDateTime.now(),
    val updatedAt: LocalDateTime = LocalDateTime.now()
) {
    companion object {
        const val STATUS_REFUNDED = "REFUNDED"
        const val STATUS_FAILED = "FAILED"
    }
}
