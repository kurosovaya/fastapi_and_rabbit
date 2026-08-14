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


@asynccontextmanager
async def lifespan(app: FastAPI):

    async with AsyncExitStack() as stack:
        await stack.enter_async_context(rabbit_lifespan(app))
        await stack.enter_async_context(mongo_lifespan(app))
        yield


app = FastAPI(lifespan=lifespan)

class Subscriptions(BaseModel):
    url: HttpUrl
    event_types: list[EventType]
    secret: str

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


@app.post("/subscriptions", status_code=201)
async def subscriptions(subscriptions: Subscriptions,
                        subscriptions_collection: AsyncCollection = Depends(get_subscriptions_collection)):

    sub_id = f"sub_{uuid.uuid4().hex}"
    await subscriptions_collection.insert_one({
        "_id": sub_id,
        "url": str(subscriptions.url),
        "event_types": subscriptions.event_types,
        "secret": subscriptions.secret,
        "active": True,
        "created_at": datetime.now(dt.UTC),
    })

    return sub_id
    

@app.post("/events", status_code=202)
async def events(
    events: Events,
    idempotency_key: Annotated[str, Header(alias="Idempotency-Key")],
    exchange: AbstractExchange = Depends(get_exchange),
    events_collections: AsyncCollection = Depends(get_events_collection)
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

        message = Message(
            body=json.dumps(
                {
                    "event_id": event_id,
                    "event_type": events.event_type,
                    "payload": events.payload,
                }
            ).encode(),
            delivery_mode=DeliveryMode.PERSISTENT,
            headers={"X-Event-Id": event_id},
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
