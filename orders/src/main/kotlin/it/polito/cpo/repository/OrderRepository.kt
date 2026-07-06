package it.polito.cpo.repository

import it.polito.cpo.model.Order
import org.springframework.data.repository.kotlin.CoroutineCrudRepository
import java.util.UUID

interface OrderRepository : CoroutineCrudRepository<Order, UUID>
