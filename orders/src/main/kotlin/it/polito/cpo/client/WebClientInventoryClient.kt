package it.polito.cpo.client

import it.polito.cpo.client.dtos.ReservationRequest
import it.polito.cpo.client.dtos.ReservationResponse
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Profile
import org.springframework.http.MediaType
import org.springframework.stereotype.Component
import org.springframework.web.reactive.function.client.WebClient
import org.springframework.web.reactive.function.client.awaitBody

@Component
@Profile("!dev")
class WebClientInventoryClient(
    @Value("\${eurotransit.services.inventory.url}") private val inventoryUrl: String
) : InventoryClient {

    private val webClient = WebClient.builder().baseUrl(inventoryUrl).build()

    override suspend fun reserveSeats(request: ReservationRequest, idempotencyKey: String): ReservationResponse {
        return webClient.post()
            .uri("/api/inventory/reservations")
            .contentType(MediaType.APPLICATION_JSON)
            .header("Idempotency-Key", idempotencyKey)
            .bodyValue(request)
            .retrieve()
            .awaitBody()
    }

    override suspend fun releaseSeats(reservationId: String, idempotencyKey: String) {
        webClient.delete()
            .uri("/api/inventory/reservations/{reservationId}", reservationId)
            .header("Idempotency-Key", idempotencyKey)
            .retrieve()
            .awaitBody<Unit>()
    }
}
