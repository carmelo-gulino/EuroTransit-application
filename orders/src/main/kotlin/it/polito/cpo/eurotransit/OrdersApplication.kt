package it.polito.cpo.eurotransit

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class EuroTransitApplication

fun main(args: Array<String>) {
    runApplication<EuroTransitApplication>(*args)
}
