package it.polito.cpo.controller

import it.polito.cpo.controller.dtos.CheckoutRequest
import it.polito.cpo.controller.dtos.CheckoutResponse
import it.polito.cpo.controller.dtos.OrderView
import it.polito.cpo.service.CheckoutOrchestrator
import it.polito.cpo.service.OrderService
import org.springframework.http.HttpStatus
import org.springframework.security.core.annotation.AuthenticationPrincipal
import org.springframework.security.oauth2.jwt.Jwt
import org.springframework.web.bind.annotation.*
import org.springframework.web.server.ResponseStatusException
import java.util.UUID

@RestController
@RequestMapping("/api/orders")
class OrderController(
    private val orderService: OrderService,
    private val checkoutOrchestrator: CheckoutOrchestrator
) {

    @PostMapping
    @ResponseStatus(HttpStatus.ACCEPTED)
    suspend fun checkout(
        @RequestBody request: CheckoutRequest,
        @RequestHeader("Idempotency-Key") idempotencyKey: String,
        @AuthenticationPrincipal jwt: Jwt
    ): CheckoutResponse {
        val userId = jwt.subject ?: throw ResponseStatusException(HttpStatus.UNAUTHORIZED, "Missing subject claim")
        return checkoutOrchestrator.checkout(request, idempotencyKey, userId)
    }

    @GetMapping("/{orderId}")
    suspend fun getOrder(
        @PathVariable orderId: UUID,
        @AuthenticationPrincipal jwt: Jwt
    ): OrderView {
        val userId = jwt.subject ?: throw ResponseStatusException(HttpStatus.UNAUTHORIZED, "Missing subject claim")
        val order = orderService.getOrderById(orderId)
            ?: throw ResponseStatusException(HttpStatus.NOT_FOUND, "Order not found")

        // Enforce ownership
        if (order.userId != userId) {
            val realmAccess = jwt.getClaim<Map<String, Any>>("realm_access")
            val roles = realmAccess?.get("roles") as? List<*>
            val isAdmin = roles?.contains("admin") == true || roles?.contains("operator") == true
            if (!isAdmin) {
                throw ResponseStatusException(HttpStatus.FORBIDDEN, "Access denied to order")
            }
        }

        return OrderView(
            orderId = order.id,
            userId = order.userId,
            status = order.status,
            routeId = order.routeId,
            seats = order.seats.split(","),
            totalAmount = order.totalAmount,
            createdAt = order.createdAt
        )
    }
}
