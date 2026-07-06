package it.polito.cpo.model

enum class OrderStatus {
    ACCEPTED,
    RESERVING,
    PAYMENT_PENDING,
    CONFIRMED,
    FAILED,
    CANCELLED
}
