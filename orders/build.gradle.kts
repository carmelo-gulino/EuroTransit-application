plugins {
    id("services-conventions")
}

dependencies {
    implementation("io.github.resilience4j:resilience4j-spring-boot3:2.2.0")
    implementation("org.springframework.boot:spring-boot-starter-data-r2dbc")
    implementation("org.springframework.boot:spring-boot-starter-security")
    implementation("org.springframework.boot:spring-boot-starter-oauth2-resource-server")

    // Schema migrations (Flyway runs at startup, before WebFlux; JDBC driver used only for this).
    // spring-boot-flyway carries FlywayAutoConfiguration (a separate module in Spring Boot 4).
    implementation("org.springframework.boot:spring-boot-flyway")
    implementation("org.flywaydb:flyway-database-postgresql")
    runtimeOnly("org.postgresql:postgresql")

    runtimeOnly("org.postgresql:r2dbc-postgresql")
}
