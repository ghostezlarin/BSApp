FROM openjdk:21-jdk-slim

# Установка необходимых утилит (wget для healthcheck + инструменты для диагностики)
RUN apt-get update && \
    apt-get install -y wget netcat-openbsd && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY build/libs/*.jar app.jar
ENTRYPOINT ["java", "-jar", "app.jar"]