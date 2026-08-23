import datetime as dt
from datetime import datetime
from typing import Any

from pymongo import AsyncMongoClient, ReturnDocument
from pymongo.asynchronous.collection import AsyncCollection

from shared.storage.base import Storage
from shared.models import *
from typing_extensions import Self


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

    async def __aenter__(self) -> Self:
        await self.events_collection.create_index("idempotency_key", unique=True)
        await self.clients_collection.create_index("client_name", unique=True)
        await self.deliveries_collection.create_index(
            [("event_id", 1), ("subscription_id", 1)], unique=True
        )
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
        return str(doc["_id"])

    async def create_subscription(
        self, sub_id: str, client_id: str, subscriptions: Subscriptions
    ):
        await self.subscriptions_collection.insert_one(
            {
                "_id": sub_id,
                "client_id": client_id,
                "url": str(subscriptions.url),
                "event_types": subscriptions.event_types,
                "secret": subscriptions.secret,
                "active": subscriptions.active,
                "created_at": datetime.now(dt.UTC),
            }
        )

    async def get_subscriptions(self, event_types: str | list):
        return await self.subscriptions_collection.find(
            {"event_types": event_types}
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
            .sort([("accepted_at", 1), ("_id", 1)])
            .limit(500)
            .to_list(500)
        )
        return batch

    async def mark_event_as_published(self, id: str):
        await self.events_collection.update_one(
            {"_id": id}, {"$set": {"published": True}}
        )

    async def create_delivery(self, dlv_id: str, event_id: str, subscription_id: str):
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
                "accepted_at": datetime.now(dt.UTC),
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

    async def close(self):
        await self.mongodb_client.close()
