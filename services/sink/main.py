from fastapi import FastAPI, status
from pymongo import AsyncMongoClient
from collections import defaultdict
from pydantic import BaseModel
from shared.models import EventType
from shared.config import Config
from typing import Any
from pymongo.asynchronous.collection import AsyncCollection 


app = FastAPI()
mongodb_client: AsyncMongoClient[dict[str, Any]] = AsyncMongoClient(Config.MONGO_URI)


def get_events_collection() -> AsyncCollection:
    return mongodb_client["webhooks"]["events"]

def get_subscriptions_collection() -> AsyncCollection:
    return mongodb_client["webhooks"]["subscriptions"]


class ReceivedHook(BaseModel):
    event_id: str
    event_type: EventType
    payload: dict[str, int]

class HooksConfig(BaseModel):
    pass
    

@app.post("/hook/{client_id}")
async def hook(client_id: str, hook: ReceivedHook):
    # received_hooks[client_id].append(hook.model_dump())
    pass

@app.put("/config/{client_id}")
async def config(client_id: str, hooks_config: HooksConfig):
    pass

@app.post("/hook_404/{client_id}", status_code=status.HTTP_404_NOT_FOUND)
async def hook_404(client_id: str, hook: ReceivedHook):
    return "Poshel na hui"

@app.get("/received_hook")
async def received_hook() -> defaultdict[str, list]:
    # return received_hooks
    pass

@app.get("/health")
async def health():
    return "SINK IS ALIIIIIIIIIIIIVE"
