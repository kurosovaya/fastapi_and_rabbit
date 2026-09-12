import asyncio
from collections import defaultdict
from contextlib import AsyncExitStack, asynccontextmanager
from random import randint

from fastapi import Depends, FastAPI, Request, status
from fastapi.responses import JSONResponse
from prometheus_fastapi_instrumentator import Instrumentator
from pydantic import BaseModel, Field
from shared.models import EventType
from shared.storage.base import Storage
from shared.storage.factory import make_storage


class HooksConfig(BaseModel):
    accept_rate: int = Field(default=100, ge=0, le=100)
    delay_ms: int = Field(default=20, ge=0)


DEFAULT_CONFIG = HooksConfig()

settings: dict[str, HooksConfig] = {}


async def load_settings(storage: Storage):
    settings.clear()
    for profile in await storage.get_sink_settings():
        settings[profile["client_id"]] = HooksConfig(
            accept_rate=profile["accept_rate"],
            delay_ms=profile["delay_ms"],
        )


@asynccontextmanager
async def storage_lifespan(app: FastAPI):

    async with make_storage() as storage:
        app.state.storage = storage
        await load_settings(storage)
        yield


@asynccontextmanager
async def lifespan(app: FastAPI):

    async with AsyncExitStack() as stack:
        await stack.enter_async_context(storage_lifespan(app))
        yield


def get_storage(request: Request) -> Storage:
    return request.app.state.storage


app = FastAPI(lifespan=lifespan)
Instrumentator().instrument(app).expose(app)


class ReceivedHook(BaseModel):
    event_id: str
    event_type: EventType
    payload: dict[str, int]


received_hooks: defaultdict[str, list] = defaultdict(list)


@app.post("/hook/{client}")
async def hook(client: str, hook: ReceivedHook):

    clients_stngs = settings.get(client, DEFAULT_CONFIG)

    await asyncio.sleep(clients_stngs.delay_ms / 1000)

    num = randint(1, 100)
    if num <= clients_stngs.accept_rate:
        received_hooks[client].append(hook.model_dump())
    else:
        return JSONResponse("Error!", status.HTTP_500_INTERNAL_SERVER_ERROR)


@app.put("/config/{client_id}")
async def config(
    client_id: str,
    hooks_config: HooksConfig,
    storage: Storage = Depends(get_storage),
) -> HooksConfig:

    await storage.set_sink_settings(
        client_id, hooks_config.accept_rate, hooks_config.delay_ms
    )
    settings[client_id] = hooks_config

    return hooks_config


@app.post("/hook_404/{client_id}", status_code=status.HTTP_404_NOT_FOUND)
async def hook_404(client_id: str, hook: ReceivedHook):
    return "Error"


@app.get("/received_hook")
async def received_hook() -> defaultdict[str, list]:
    return received_hooks


@app.get("/")
@app.get("/health")
async def health():
    return "SINK IS ALIIIIIIIIIIIIVE"
