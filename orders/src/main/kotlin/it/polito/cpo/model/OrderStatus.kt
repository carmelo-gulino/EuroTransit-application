package it.polito.cpo.model

enum class OrderStatus {
    ACCEPTED,
    RESERVING,
    PAYMENT_PENDING,
    CONFIRMED,
    FAILED,
    // Reserved for a future customer/operations cancellation flow (api-design.md mentions release
    // "when the order is cancelled"). No cancellation endpoint exists yet; kept for forward contract.
    CANCELLED
}
