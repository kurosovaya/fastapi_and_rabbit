from fastapi import FastAPI, Body, Request, Depends, Header, status
from pydantic import BaseModel, HttpUrl, Field
from typing import Annotated
from aio_pika import Message, DeliveryMode
from contextlib import AsyncExitStack, asynccontextmanager
from rabbit import lifespan as rabbit_lifespan
from mongo_lifespan import mongo_lifespan
from aio_pika.abc import AbstractExchange
from pymongo.asynchronous.collection import AsyncCollection 
from pymongo.errors import DuplicateKeyError
import json
import uuid
from datetime import datetime
import datetime as dt
from fastapi.responses import JSONResponse
from shared.models import EventType
from shared.config import Config
from prometheus_fastapi_instrumentator import Instrumentator


@asynccontextmanager
async def lifespan(app: FastAPI):

    async with AsyncExitStack() as stack:
        await stack.enter_async_context(rabbit_lifespan(app))
        await stack.enter_async_context(mongo_lifespan(app))
        yield


app = FastAPI(lifespan=lifespan)
Instrumentator().instrument(app).expose(app)

class Subscriptions(BaseModel):
    url: HttpUrl
    event_types: list[EventType]
    secret: str
    active: bool
    client_name: str

class Events(BaseModel):
    event_type: EventType
    payload: dict[str, int]

class EventDB(Events):
    id: str = Field(alias="_id")
    idempotency_key: str


def get_exchange(request: Request) -> AbstractExchange:
    return request.app.state.rabbit_exchange

def get_events_collection(request: Request) -> AsyncCollection:
    return request.app.state.mongodb_client["webhooks"]["events"]

def get_subscriptions_collection(request: Request) -> AsyncCollection:
    return request.app.state.mongodb_client["webhooks"]["subscriptions"]

def get_clients_collection(request: Request) -> AsyncCollection:
    return request.app.state.mongodb_client["webhooks"]["clients"]

def get_deliveries_collection(request: Request) -> AsyncCollection:
    return request.app.state.mongodb_client["webhooks"]["deliveries"]


@app.post("/subscriptions", status_code=201)
async def subscriptions(subscriptions: Subscriptions,
                        subscriptions_collection: AsyncCollection = Depends(get_subscriptions_collection),
                        clients_collection: AsyncCollection = Depends(get_clients_collection)):

    await clients_collection.update_one(
        {"client_name": subscriptions.client_name},
        {"$setOnInsert": {"client_name": subscriptions.client_name}},
        upsert=True,
    )

    client_id = str(await clients_collection.find_one({"client_name": subscriptions.client_name},
                                                  projection=["_id"]))

    sub_id = f"sub_{uuid.uuid4().hex}"
    await subscriptions_collection.insert_one(
        {
            "_id": sub_id,
            "client_id": client_id["_id"],
            "url": str(subscriptions.url),
            "event_types": subscriptions.event_types,
            "secret": subscriptions.secret,
            "active": subscriptions.active,
            "created_at": datetime.now(dt.UTC),
        }
    )

    return sub_id
    

@app.post("/events", status_code=202)
async def events(
    events: Events,
    idempotency_key: Annotated[str, Header(alias="Idempotency-Key")],
    exchange: AbstractExchange = Depends(get_exchange),
    events_collections: AsyncCollection = Depends(get_events_collection),
    subscriptions_collection: AsyncCollection = Depends(get_subscriptions_collection),
    deliveries_collection: AsyncCollection = Depends(get_deliveries_collection)
) -> str:

    try:
        event_id = f"evt_{uuid.uuid4().hex}"
        await events_collections.insert_one(
            {
                "_id": event_id,
                "idempotency_key": idempotency_key,
                "event_type": events.event_type,
                "payload": events.payload,
            }
        )

        async with subscriptions_collection.find({"event_types": events.event_type}) as cursor:
            async for doc in cursor:

                dlv_id = f"dlv_{uuid.uuid4().hex}"
                await deliveries_collection.insert_one({
                    "_id": dlv_id,
                    "event_id": event_id,
                    "subscription_id": doc["_id"],
                    "status": "pending",
                    "attempt": 0,
                    "attempt_epoch": 0,
                    "locked_by": None,
                    "locked_until": None,
                    "next_attempt_at": None,
                    "last_error": None,
                    "accepted_at": datetime.now(dt.UTC),
                    "delivered_at": None
                })

                message = Message(
                    body=json.dumps(
                        {
                            "dlv_id": dlv_id,
                            "event_id": event_id,
                            "event_type": events.event_type,
                            "url": doc["url"],
                            "payload": events.payload,
                        }
                    ).encode(),
                    delivery_mode=DeliveryMode.PERSISTENT,
                    headers={"X-Event-Id": event_id, "X-Dlv-Id": dlv_id,
                             "X-Client-Id": str(doc["client_id"]), "X-Url": doc["url"]},
                    content_type="application/json",
                )
                await exchange.publish(message, routing_key=Config.DELIVER_ROUTING_KEY)
        return event_id
    except DuplicateKeyError:
        event: EventDB = EventDB.model_validate(await events_collections.find_one({"idempotency_key": idempotency_key}))
        return event.id
        

@app.get("/events/{event_id}")
async def events_get(event_id: str, events_collections: AsyncCollection = Depends(get_events_collection)):

    found_event = await events_collections.find_one({"_id": event_id})
    if found_event:
        return found_event
    else:
        return JSONResponse(f"Not found event with ID {event_id}",
                            status_code=status.HTTP_404_NOT_FOUND)


@app.get("/health")
async def health():
    return "I\'AM ALIVE" 
