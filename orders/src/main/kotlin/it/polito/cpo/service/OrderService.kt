package it.polito.cpo.service

import it.polito.cpo.model.IdempotentRequest
import it.polito.cpo.model.Order
import it.polito.cpo.repository.IdempotentRequestRepository
import it.polito.cpo.repository.OrderRepository
import org.springframework.stereotype.Service
import java.time.LocalDateTime
import java.util.UUID

@Service
class OrderService(
    private val orderRepository: OrderRepository,
    private val idempotencyRepository: IdempotentRequestRepository
) {

    suspend fun getOrderById(id: UUID): Order? {
        return orderRepository.findById(id)
    }

    suspend fun saveOrder(order: Order): Order {
        return orderRepository.save(order)
    }

    suspend fun findIdempotentRequest(key: String): IdempotentRequest? {
        return idempotencyRepository.findById(key)
    }

    suspend fun saveIdempotentRequest(
        key: String,
        responseBody: String,
        principalId: String,
        operation: String,
        requestFingerprint: String,
    ): IdempotentRequest {
        val request = IdempotentRequest(
            key = key,
            responseBody = responseBody,
            createdAt = LocalDateTime.now(),
            principalId = principalId,
            operation = operation,
            requestFingerprint = requestFingerprint,
        )
        return idempotencyRepository.save(request)
    }

    suspend fun purgeExpiredIdempotency(cutoff: LocalDateTime): Long =
        idempotencyRepository.deleteExpired(cutoff)
}
