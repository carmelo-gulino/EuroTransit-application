package it.polito.cpo.model

import org.springframework.data.annotation.Id
import org.springframework.data.annotation.Transient
import org.springframework.data.domain.Persistable
import org.springframework.data.relational.core.mapping.Table
import java.time.OffsetDateTime

@Table("idempotency_records")
data class IdempotencyRecord(
    @Id
    @get:JvmName("getIdempotencyKeyValue")
    val idempotencyKey: String,
    
    val principalId: String,
    val operation: String,
    val requestFingerprint: String,
    val responseStatusCode: Int,
    val responseBody: String?,
    val createdAt: OffsetDateTime? = null,
    
    @Transient
    var isNewRecord: Boolean = true
) : Persistable<String> {
    override fun getId(): String = idempotencyKey
    override fun isNew(): Boolean = isNewRecord
}
