import os
import aio_pika
from aio_pika import ExchangeType, IncomingMessage, Message, DeliveryMode
from aio_pika.abc import AbstractIncomingMessage
from pymongo import AsyncMongoClient
import asyncio
import httpx
from shared.models import RabbitCustomFields
from shared.config import Config


mongo_client = AsyncMongoClient(Config.MONGO_URI)
timeouts = [1, 5, 25, 125]

# import debugpy
# debugpy.listen(("0.0.0.0", 5678))
# print("Waiting for debugger to attach...")
# debugpy.wait_for_client()




async def worker():
    print("Worker started")

    rabbit_connect = await aio_pika.connect_robust(Config.RABBIT_URL)
    channel = await rabbit_connect.channel()
    await channel.set_qos(1)
    exchange = await channel.get_exchange(Config.EXCHANGE_NAME)
    queue = await channel.get_queue(Config.QUEUE_NAME)
    dead_exchange = await channel.get_exchange(Config.DLE_NAME)

    async def callback(message: AbstractIncomingMessage):
        async with httpx.AsyncClient() as client:
            try:
                response = await client.post(
                    "http://sink:9001/hook/1",
                    content=message.body,
                    headers={"Content-Type": "application/json"},
                )
                if response.status_code == 200:
                    print(f"Sent: {message.body.decode()}")
                    await message.ack()
                else:
                    raise httpx.HTTPError("Не прислал нихуя")
            except httpx.HTTPError as exc:
                # args = {"x-message-ttl": 1000 * 100}
                try:
                    attempt = message.headers.get(RabbitCustomFields.ATTEMPT, 0)
                    attempt = int(attempt) + 1
                except (AttributeError, ValueError) as exp:
                    print(exp)
                    attempt = 5

                message_new = Message(
                    body=message.body,
                    headers={
                        **message.headers,
                        RabbitCustomFields.ATTEMPT: attempt,
                        RabbitCustomFields.URL: f"{exc.request.url}",
                        RabbitCustomFields.ERR: str(exc),
                    },
                    delivery_mode=DeliveryMode.PERSISTENT,
                )

                if attempt > 5:
                    await dead_exchange.publish(message_new, "webhooks.dle")

                await exchange.publish(message_new, f"retry.{timeouts[attempt - 1]}s")
                await message.ack()

    await queue.consume(callback)
    await asyncio.Future()


asyncio.run(worker())
