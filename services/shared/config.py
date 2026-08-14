from enum import Enum


class Config(Enum, str):

    RABBIT_URL = "amqp://guest:guest@rabbitmq:5672/"
    EXCHANGE_NAME = "webhooks.direct"
    QUEUE_NAME = "q.deliveries"
    DELIVER_ROUTING_KEY = "deliver"

    DLE_NAME = "webhooks.dle"
    DLQ_NAME = "q.dlq"
    DLQ_ROUTING_KEY = "deliver.dlq"

    MONGO_URI = "mongodb://mongo:27017/?replicaSet=rs0"
