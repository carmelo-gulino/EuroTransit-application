FROM eclipse-temurin:21-jre

ARG SERVICE

WORKDIR /app

COPY ${SERVICE}/build/libs/${SERVICE}-0.0.1-SNAPSHOT.jar /app/app.jar

ENV SERVER_PORT=8080

EXPOSE 8080

ENTRYPOINT ["java", "-jar", "/app/app.jar"]
