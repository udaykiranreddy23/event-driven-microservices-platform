# Event-Driven Microservices Platform

Production-style reference system using FastAPI, PostgreSQL, Redis and Kafka-compatible messaging.

## Architecture

```text
Client -> Order API -> PostgreSQL
                    -> Kafka/Redpanda
                       |- Inventory Service
                       |- Payment Service
                       |- Notification Service
                       `- Analytics Service
```

## Features
- Event-driven service communication
- Idempotency keys
- PostgreSQL persistence
- Consumer-side event deduplication
- Retry and dead-letter topic
- Redis-backed idempotency
- Docker Compose
- Pytest
- GitHub Actions CI

## Run

```bash
docker compose up --build
```

API: http://localhost:8000/docs

```bash
curl -X POST http://localhost:8000/orders   -H "Content-Type: application/json"   -H "Idempotency-Key: demo-001"   -d '{"customer_id":"cust-100","items":[{"sku":"keyboard","quantity":1}]}'
```

## Topics
`orders.created`, `payments.completed`, `inventory.reserved`, `notifications.send`, `events.dlq`

This is a portfolio/reference implementation; payment processing is simulated.
