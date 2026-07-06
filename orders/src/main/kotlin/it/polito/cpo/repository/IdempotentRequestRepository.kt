package it.polito.cpo.repository

import it.polito.cpo.model.IdempotentRequest
import org.springframework.data.repository.kotlin.CoroutineCrudRepository

interface IdempotentRequestRepository : CoroutineCrudRepository<IdempotentRequest, String>
