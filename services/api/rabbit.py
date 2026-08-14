import aio_pika
from aio_pika import ExchangeType
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request
import os
from shared.config import Config


@asynccontextmanager
async def lifespan(app: FastAPI):

    connect = await aio_pika.connect_robust(Config.RABBIT_URL)
    channel = await connect.channel(publisher_confirms=True, on_return_raises=True)

    exchange = await channel.declare_exchange(
        Config.EXCHANGE_NAME, ExchangeType.DIRECT, durable=True
    )
    queue = await channel.declare_queue(
        Config.QUEUE_NAME, durable=True, arguments={"x-queue-type": "quorum"}
    )

    await queue.bind(exchange, Config.DELIVER_ROUTING_KEY)

    app.state.rabbit_connection = connect
    app.state.rabbit_exchange = exchange

    yield

    await connect.close()
