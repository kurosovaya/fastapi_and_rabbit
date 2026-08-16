import os
import aio_pika
from aio_pika import ExchangeType, IncomingMessage, Message, DeliveryMode
from aio_pika.abc import AbstractIncomingMessage
from pymongo import AsyncMongoClient
from pymongo.asynchronous.collection import AsyncCollection
import asyncio
import httpx
from shared.models import RabbitCustomFields, DlvStatus
from shared.config import Config
from datetime import datetime
import datetime as dt
from datetime import timedelta


mongo_client = AsyncMongoClient(Config.MONGO_URI)
timeouts = [1, 5, 25, 125]

# import debugpy
# debugpy.listen(("0.0.0.0", 5678))
# print("Waiting for debugger to attach...")
# debugpy.wait_for_client()


def get_deliveries_collection() -> AsyncCollection:
    return mongo_client["webhooks"]["deliveries"]


async def worker():
    deliveries_collection = get_deliveries_collection()
    print("Worker started")

    rabbit_connect = await aio_pika.connect_robust(Config.RABBIT_URL)
    channel = await rabbit_connect.channel()
    await channel.set_qos(1)
    exchange = await channel.get_exchange(Config.EXCHANGE_NAME)
    queue = await channel.get_queue(Config.QUEUE_NAME)
    dead_exchange = await channel.get_exchange(Config.DLE_NAME)

    async def callback(message: AbstractIncomingMessage):
        await deliveries_collection.find_one_and_update(
            {"_id": message.headers.get("X-Dlv-Id")},
            {"$set": {"status": DlvStatus.IN_PROGRESS}},
        )
        async with httpx.AsyncClient() as client:
            try:
                response = await client.post(
                    f"http://sink:9001/hook/{message.headers.get("X-Client-Id")}",
                    content=message.body,
                    headers={"Content-Type": "application/json"},
                )
                print(f"Sent: {message.body.decode()}")
                await deliveries_collection.find_one_and_update(
                    {"_id": message.headers.get("X-Dlv-Id")},
                    {
                        "$set": {
                            "status": DlvStatus.DELIVERED,
                            "delivered_at": datetime.now(dt.UTC),
                        }
                    },
                )
                response.raise_for_status()                
                await message.ack()
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

                if attempt > 4:
                    await dead_exchange.publish(message_new, Config.DLQ_ROUTING_KEY)
                    await deliveries_collection.find_one_and_update(
                        {"_id": message.headers.get("X-Dlv-Id")},
                        {
                            "$set": {
                                "status": DlvStatus.FAILED,
                                "attempt": attempt,
                                "last_error": str(exc),
                                "next_attempt_at": None,
                            }
                        },
                    )
                else:
                    await exchange.publish(
                        message_new, f"retry.{timeouts[attempt - 1]}s"
                    )
                    await deliveries_collection.find_one_and_update(
                        {"_id": message.headers.get("X-Dlv-Id")},
                        {
                            "$set": {
                                "status": DlvStatus.IN_PROGRESS,
                                "attempt": attempt,
                                "last_error": str(exc),
                                "next_attempt_at": datetime.now(dt.UTC)
                                + timedelta(seconds=timeouts[attempt - 1]),
                            }
                        },
                    )
                await message.ack()

    await queue.consume(callback)
    await asyncio.Future()


asyncio.run(worker())
