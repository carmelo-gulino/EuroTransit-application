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

    suspend fun saveIdempotentRequest(key: String, responseBody: String): IdempotentRequest {
        val request = IdempotentRequest(key, responseBody, LocalDateTime.now())
        return idempotencyRepository.save(request)
    }
}
