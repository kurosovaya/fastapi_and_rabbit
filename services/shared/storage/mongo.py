import datetime as dt
from datetime import datetime
from typing import Any, Self

from pymongo import AsyncMongoClient, ReturnDocument
from pymongo.asynchronous.collection import AsyncCollection
from shared.models import DlvStatus, Events, Subscriptions
from shared.storage.base import Storage


class MongoStorage(Storage):
    def __init__(self, mongodb_client: AsyncMongoClient[dict[str, Any]]) -> None:
        self.mongodb_client: AsyncMongoClient[dict[str, Any]] = mongodb_client
        self.events_collection: AsyncCollection = self.mongodb_client["webhooks"][
            "events"
        ]
        self.subscriptions_collection: AsyncCollection = self.mongodb_client[
            "webhooks"
        ]["subscriptions"]
        self.clients_collection: AsyncCollection = self.mongodb_client["webhooks"][
            "clients"
        ]
        self.deliveries_collection: AsyncCollection = self.mongodb_client["webhooks"][
            "deliveries"
        ]
        self.sink_settings_collection: AsyncCollection = self.mongodb_client[
            "webhooks"
        ]["sink_settings"]

    async def __aenter__(self) -> Self:
        await self.events_collection.create_index("idempotency_key", unique=True)
        await self.clients_collection.create_index("client_name", unique=True)
        await self.subscriptions_collection.create_index(
            [("client_id", 1), ("url", 1)], unique=True
        )
        await self.subscriptions_collection.create_index(
            [("event_types", 1), ("active", 1)], unique=False
        )
        await self.deliveries_collection.create_index(
            [("event_id", 1), ("subscription_id", 1)], unique=True
        )
        await self.events_collection.create_index([("published", 1), ("_id", 1)])
        return self

    async def __aexit__(self, *exc: object):
        await self.close()

    async def ensure_client(self, client_name: str) -> str:
        doc = await self.clients_collection.find_one_and_update(
            {"client_name": client_name},
            {"$setOnInsert": {"client_name": client_name}},
            projection=["_id"],
            upsert=True,
            return_document=ReturnDocument.AFTER,
        )
        if doc is None:
            raise RuntimeError("upsert did not return a document")
        return str(doc["_id"])

    async def create_subscription(
        self, sub_id: str, client_id: str, subscriptions: Subscriptions
    ) -> str:
        doc = await self.subscriptions_collection.find_one_and_update(
            {"client_id": client_id, "url": str(subscriptions.url)},
            {
                "$setOnInsert": {
                    "_id": sub_id,
                    "client_id": client_id,
                    "url": str(subscriptions.url),
                    "created_at": datetime.now(dt.UTC),
                },
                "$set": {
                    "active": subscriptions.active,
                    "secret": subscriptions.secret,
                    "event_types": subscriptions.event_types,
                },
            },
            projection={"_id"},
            upsert=True,
            return_document=ReturnDocument.AFTER,
        )
        if doc is None:
            raise RuntimeError("upsert did not return a document")
        return str(doc["_id"])

    async def get_subscriptions(self, event_types: str | list):
        return await self.subscriptions_collection.find(
            {"event_types": event_types, "active": True}
        ).to_list()

    async def create_event(self, event_id: str, idempotency_key: str, events: Events):
        await self.events_collection.insert_one(
            {
                "_id": event_id,
                "idempotency_key": idempotency_key,
                "event_type": events.event_type,
                "payload": events.payload,
                "published": False,
                "accepted_at": datetime.now(dt.UTC),
            }
        )

    async def get_event_id(self, idempotency_key: str) -> str:
        event_id = await self.events_collection.find_one(
            {"idempotency_key": idempotency_key}
        )
        return str((event_id or {})["_id"])

    async def get_event(self, event_id: str):
        return await self.events_collection.find_one({"_id": event_id})

    async def get_unpublished_events(self, start_id: str | None = None):
        params: dict = {"published": False}
        if start_id:
            params["_id"] = {"$gt": start_id}
        batch = (
            await self.events_collection.find(params)
            .sort({"_id": 1})
            .limit(500)
            .to_list(500)
        )
        return batch

    async def mark_event_as_published(self, id: str):
        await self.events_collection.update_one(
            {"_id": id}, {"$set": {"published": True}}
        )

    async def find_delivery(self, event_id: str, subscription_id: str):
        return await self.deliveries_collection.find_one(
            {"event_id": event_id, "subscription_id": subscription_id}
        )

    async def create_delivery(
        self, dlv_id: str, event_id: str, subscription_id: str, accepted_at: datetime
    ):
        await self.deliveries_collection.insert_one(
            {
                "_id": dlv_id,
                "event_id": event_id,
                "subscription_id": subscription_id,
                "status": "pending",
                "attempt": 0,
                "attempt_epoch": 0,
                "locked_by": None,
                "locked_until": None,
                "next_attempt_at": None,
                "last_error": None,
                "accepted_at": accepted_at,
                "delivered_at": None,
            }
        )

    async def update_delivery_status(
        self,
        id: str,
        status: DlvStatus,
        delivered_at: datetime | None = None,
        attempt: int | None = None,
        last_error: str | None = None,
        next_attempt_at: datetime | None = None,
    ):
        set_params: dict[str, Any] = {"status": status}
        if delivered_at:
            set_params["delivered_at"] = delivered_at
        if attempt:
            set_params["attempt"] = attempt
        if last_error:
            set_params["last_error"] = last_error
        if next_attempt_at:
            set_params["next_attempt_at"] = next_attempt_at

        await self.deliveries_collection.update_one(
            {"_id": id},
            {"$set": {**set_params}},
        )

    async def get_sink_settings(self) -> list[dict]:
        docs = await self.sink_settings_collection.find().to_list()
        return [
            {
                "client_id": str(doc["_id"]),
                "accept_rate": doc["accept_rate"],
                "delay_ms": doc["delay_ms"],
            }
            for doc in docs
        ]

    async def set_sink_settings(self, client_id: str, accept_rate: int, delay_ms: int):
        await self.sink_settings_collection.update_one(
            {"_id": client_id},
            {"$set": {"accept_rate": accept_rate, "delay_ms": delay_ms}},
            upsert=True,
        )

    async def close(self):
        await self.mongodb_client.close()
