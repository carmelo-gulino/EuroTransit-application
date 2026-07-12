package it.polito.cpo.model

import org.springframework.data.annotation.Id
import org.springframework.data.annotation.Transient
import org.springframework.data.domain.Persistable
import org.springframework.data.relational.core.mapping.Table
import java.math.BigDecimal
import java.time.LocalDateTime
import java.util.UUID

@Table("orders")
class Order(
    @Id private val id: UUID,
    val userId: String,
    var status: OrderStatus,
    val routeId: String,
    val seats: String, // comma-separated seats
    val totalAmount: BigDecimal,
    val createdAt: LocalDateTime,
    // Sandbox payment reference kept only until the order reaches a terminal state, so the
    // asynchronous pipeline can reconstruct the payment request. Cleared on CONFIRMED/FAILED.
    var paymentMethodToken: String? = null
) : Persistable<UUID> {

    @Transient
    private var isNew: Boolean = true

    override fun getId(): UUID = id

    override fun isNew(): Boolean = isNew

    fun setAsNew(isNew: Boolean): Order {
        this.isNew = isNew
        return this
    }
}
