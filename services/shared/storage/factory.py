from psycopg import AsyncConnection
from psycopg.rows import DictRow, dict_row
from psycopg_pool import AsyncConnectionPool
from pymongo import AsyncMongoClient
from shared.config import Config
from shared.storage.base import Storage
from shared.storage.mongo import MongoStorage
from shared.storage.postgres import PostgresStorage


def make_storage() -> Storage:
    match Config.STORAGE_BACKEND:
        case "mongo":
            return MongoStorage(AsyncMongoClient(Config.MONGO_URI, tz_aware=True))
        case "postgres":
            pool: AsyncConnectionPool[AsyncConnection[DictRow]] = AsyncConnectionPool(
                Config.POSTGRES_URI, open=False, kwargs={"row_factory": dict_row}
            )
            return PostgresStorage(pool)
        case other:
            raise ValueError(f"unknown Config.STORAGE_BACKEND: {other}")
