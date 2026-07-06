FROM eclipse-temurin:21-jdk-alpine AS builder

ARG SERVICE

WORKDIR /workspace

COPY gradle gradle
COPY gradlew .
COPY build.gradle.kts settings.gradle.kts ./
COPY buildSrc buildSrc
COPY catalog/build.gradle.kts catalog/build.gradle.kts
COPY observability observability
COPY inventory/build.gradle.kts inventory/build.gradle.kts
COPY notifications/build.gradle.kts notifications/build.gradle.kts
COPY orders/build.gradle.kts orders/build.gradle.kts
COPY payments/build.gradle.kts payments/build.gradle.kts
COPY ${SERVICE} ${SERVICE}

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
