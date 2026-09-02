# Relay

A small Kafka lab I built to understand event processing, retries, ordering, consumer groups, and handling duplicate or failed messages.

## Stack
- Java 21
- Spring Boot 3.4.3
- Apache Kafka
- Spring for Apache Kafka
- Maven
- Docker Compose

## Local Kafka Setup

Start local Kafka broker:
```bash
docker compose up -d
```

Stop local Kafka broker:
```bash
docker compose down
```

## Build and Test Commands

Run tests:
```bash
./mvnw test
```

Package application:
```bash
./mvnw package
```

Run application:
```bash
./mvnw spring-boot:run
```
