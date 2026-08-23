from shared.storage.base import Storage
from shared.config import Config
from shared.storage.mongo import MongoStorage
from pymongo import AsyncMongoClient


def make_storage() -> Storage:
    match Config.STORAGE_BACKEND:
        case "mongo":
            return MongoStorage(AsyncMongoClient(Config.MONGO_URI))
        case "postgres":
            raise NotImplementedError("Postgres DB is not implemented for now")
            # return PostgresStorage(await asyncpg.create_pool(Config.POSTGRES_DSN))
        case other:
            raise ValueError(f"unknown Config.STORAGE_BACKEND: {other}")
