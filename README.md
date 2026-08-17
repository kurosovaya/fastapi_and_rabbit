# Webhook Delivery Service

Portfolio project: an async webhook delivery service with at-least-once guarantees, a retry ladder, a DLQ, and a load-testing setup for measuring system capacity.

Full spec (on Russian): [TZ-webhook-delivery-service.md](TZ-webhook-delivery-service.md).

Stack: Python 3.13, FastAPI, RabbitMQ (aio-pika), MongoDB, Prometheus, Grafana, Docker Compose.

## Quick start

```bash
docker compose up --build
```
