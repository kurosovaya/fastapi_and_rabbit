from contextlib import asynccontextmanager
from fastapi import FastAPI
from pymongo import AsyncMongoClient
from typing import Any
from shared.config import Config


@asynccontextmanager
async def mongo_lifespan(app: FastAPI):

    client: AsyncMongoClient[dict[str, Any]] = AsyncMongoClient(Config.MONGO_URI)
    app.state.mongodb_client = client
    events_collection = client["webhooks"]["events"]
    await events_collection.create_index("idempotency_key", unique=True)
    clients_collection = client["webhooks"]["clients"]
    await clients_collection.create_index("client_name", unique=True)
    # app.state.mongodb_database =
    yield
    await client.close()
