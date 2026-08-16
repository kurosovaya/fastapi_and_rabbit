from fastapi import FastAPI, status, Depends, status
from fastapi.responses import JSONResponse
from pymongo import AsyncMongoClient
from collections import defaultdict
from pydantic import BaseModel, Field
from shared.models import EventType
from shared.config import Config
from typing import Any
from pymongo.asynchronous.collection import AsyncCollection
from random import randint
from prometheus_fastapi_instrumentator import Instrumentator


app = FastAPI()
Instrumentator().instrument(app).expose(app)
mongodb_client: AsyncMongoClient[dict[str, Any]] = AsyncMongoClient(Config.MONGO_URI)


def get_events_collection() -> AsyncCollection:
    return mongodb_client["webhooks"]["events"]


def get_subscriptions_collection() -> AsyncCollection:
    return mongodb_client["webhooks"]["subscriptions"]


def get_clients_sink_settings() -> AsyncCollection:
    return mongodb_client["sink"]["clients"]["settings"]


class ReceivedHook(BaseModel):
    event_id: str
    event_type: EventType
    payload: dict[str, int]


class HooksConfig(BaseModel):
    success_percent: int = Field(default_factory=lambda: randint(80, 100))


received_hooks = defaultdict(list)


@app.post("/hook/{client_id}")
async def hook(
    client_id: str,
    hook: ReceivedHook,
    clients_sink_settings: AsyncCollection = Depends(get_clients_sink_settings),
):  
    client = await clients_sink_settings.find_one({"client_id": client_id}) or {}
    clients_stngs = HooksConfig.model_validate(client)
    num = randint(1, 100)
    if num <= clients_stngs.success_percent:
        received_hooks[client_id].append(hook.model_dump())
    else:
        JSONResponse("Error!", status.HTTP_500_INTERNAL_SERVER_ERROR)


@app.put("/config/{client_id}")
async def config(
    client_id: str,
    hooks_config: HooksConfig,
    clients_sink_settings: AsyncCollection = Depends(get_clients_sink_settings),
):
    await clients_sink_settings.update_one(
        {"client_id": client_id},
        {"$setOnInsert": {"client_id": client_id, **hooks_config.model_dump()}},
        upsert=True,
    )

    return await clients_sink_settings.find_one({"client_id": client_id})


@app.post("/hook_404/{client_id}", status_code=status.HTTP_404_NOT_FOUND)
async def hook_404(client_id: str, hook: ReceivedHook):
    return "Poshel na hui"


@app.get("/received_hook")
async def received_hook() -> defaultdict[str, list]:
    return received_hooks


@app.get("/")
@app.get("/health")
async def health():
    return "SINK IS ALIIIIIIIIIIIIVE"
