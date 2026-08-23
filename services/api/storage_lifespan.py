from fastapi import FastAPI
from contextlib import asynccontextmanager
from shared.storage.factory import make_storage



@asynccontextmanager
async def lifespan(app: FastAPI):

    async with make_storage() as storage:
        app.state.storage = storage
        yield
