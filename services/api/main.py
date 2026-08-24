from contextlib import AsyncExitStack, asynccontextmanager
from typing import Annotated

from fastapi import Depends, FastAPI, Header, Request, status
from fastapi.responses import JSONResponse
from prometheus_fastapi_instrumentator import Instrumentator
from pymongo.errors import DuplicateKeyError
from rabbit import lifespan as rabbit_lifespan
from shared.ids import new_id
from shared.models import *
from shared.storage.base import Storage
from storage_lifespan import lifespan as storage_lifespan


@asynccontextmanager
async def lifespan(app: FastAPI):

    async with AsyncExitStack() as stack:
        await stack.enter_async_context(rabbit_lifespan(app))
        await stack.enter_async_context(storage_lifespan(app))
        yield


app = FastAPI(lifespan=lifespan)
Instrumentator().instrument(app).expose(app)


def get_storage(request: Request) -> Storage:
    return request.app.state.storage


@app.post("/subscriptions", status_code=201)
async def subscriptions(
    subscriptions: Subscriptions, storage: Storage = Depends(get_storage)
):

    client_id = await storage.ensure_client(subscriptions.client_name)

    sub_id = new_id("sub")
    await storage.create_subscription(sub_id, client_id, subscriptions)
    return sub_id


@app.post("/events", status_code=202)
async def events(
    events: Events,
    idempotency_key: Annotated[str, Header(alias="Idempotency-Key")],
    storage: Storage = Depends(get_storage),
) -> str:

    try:
        event_id = new_id("evt")
        await storage.create_event(event_id, idempotency_key, events)

        return event_id
    except DuplicateKeyError:
        return await storage.get_event_id(idempotency_key)


@app.get("/events/{event_id}")
async def events_get(event_id: str, storage: Storage = Depends(get_storage)):

    found_event = await storage.get_event(event_id)
    if found_event:
        return found_event
    else:
        return JSONResponse(
            f"Not found event with ID {event_id}", status_code=status.HTTP_404_NOT_FOUND
        )


@app.get("/health")
async def health():
    return "I'AM ALIVE"
