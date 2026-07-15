FROM eclipse-temurin:21-jdk-alpine AS builder

WORKDIR /workspace

COPY gradle gradle
COPY gradlew .
COPY build.gradle.kts settings.gradle.kts ./
COPY buildSrc buildSrc
COPY catalog/build.gradle.kts catalog/build.gradle.kts
# Shared library modules build files: they are included in settings.gradle.kts and
# depended on by services, so their directories must exist for Gradle to configure the build.
COPY observability/build.gradle.kts observability/build.gradle.kts
COPY security-support/build.gradle.kts security-support/build.gradle.kts
COPY money-path-contracts/build.gradle.kts money-path-contracts/build.gradle.kts
COPY inventory/build.gradle.kts inventory/build.gradle.kts
COPY notifications/build.gradle.kts notifications/build.gradle.kts
COPY orders/build.gradle.kts orders/build.gradle.kts
COPY payments/build.gradle.kts payments/build.gradle.kts

# Download dependencies (this layer will be cached unless a build.gradle.kts file changes)
RUN ./gradlew --no-daemon dependencies || true

# Copy shared library sources
COPY observability/src observability/src
COPY security-support/src security-support/src
COPY money-path-contracts/src money-path-contracts/src

ARG SERVICE
# Copy the target service source
COPY ${SERVICE}/src ${SERVICE}/src

RUN ./gradlew --no-daemon ":${SERVICE}:bootJar"

RUN cp "${SERVICE}/build/libs/${SERVICE}.jar" application.jar
RUN java -Djarmode=tools -jar application.jar extract --layers --destination extracted

FROM eclipse-temurin:21-jre-alpine

WORKDIR /app

RUN addgroup -S appgroup && adduser -S appuser -G appgroup

COPY --from=builder /workspace/extracted/dependencies/ ./
COPY --from=builder /workspace/extracted/spring-boot-loader/ ./
COPY --from=builder /workspace/extracted/snapshot-dependencies/ ./
COPY --from=builder /workspace/extracted/application/ ./

USER appuser

ENV SERVER_PORT=8080

EXPOSE 8080

ENTRYPOINT ["java", "-jar", "application.jar"]
