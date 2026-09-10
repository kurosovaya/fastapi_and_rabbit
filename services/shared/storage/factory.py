from pymongo import AsyncMongoClient
from shared.config import Config
from shared.storage.base import Storage
from shared.storage.mongo import MongoStorage
from shared.storage.postgres import PostgresStorage
from psycopg import AsyncConnection
from psycopg_pool import AsyncConnectionPool
from psycopg.rows import DictRow, dict_row


def make_storage() -> Storage:
    match Config.STORAGE_BACKEND:
        case "mongo":
            return MongoStorage(AsyncMongoClient(Config.MONGO_URI))
        case "postgres":
            pool: AsyncConnectionPool[AsyncConnection[DictRow]] = AsyncConnectionPool(
                Config.POSTGRES_URI, open=False, kwargs={"row_factory": dict_row}
            )
            return PostgresStorage(pool)
        case other:
            raise ValueError(f"unknown Config.STORAGE_BACKEND: {other}")
