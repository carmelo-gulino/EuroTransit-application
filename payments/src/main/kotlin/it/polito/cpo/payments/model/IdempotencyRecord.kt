package it.polito.cpo.payments.model

import org.springframework.data.annotation.Id
import org.springframework.data.relational.core.mapping.Table
import java.time.LocalDateTime

@Table("idempotency_records")
data class IdempotencyRecord(
    @Id
    val idempotencyKey: String,
    val operation: String,
    val principalId: String,
    val requestFingerprint: String,
    val responseCode: Int,
    val responseBody: String?,
    val createdAt: LocalDateTime = LocalDateTime.now()
)
