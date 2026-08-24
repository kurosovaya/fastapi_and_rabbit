from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    RABBIT_URL: str = "amqp://guest:guest@rabbitmq:5672/"
    EXCHANGE_NAME: str = "webhooks.direct"
    QUEUE_NAME: str = "q.deliveries"
    DELIVER_ROUTING_KEY: str = "deliver"

    DLE_NAME: str = "webhooks.dle"
    DLQ_NAME: str = "q.dlq"
    DLQ_ROUTING_KEY: str = "deliver.dlq"

    MONGO_URI: str = "mongodb://mongo:27017/?replicaSet=rs0"

    STORAGE_BACKEND: str = "mongo"
    OUTBOX_POLL_INTERVAL: float = 0.2


Config = Settings()
