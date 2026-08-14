import aio_pika
import asyncio
from shared.config import Config


async def declare_queues():

    connect = await aio_pika.connect_robust(Config.RABBIT_URL)
    channel = await connect.channel()
    exchange = await channel.declare_exchange(Config.EXCHANGE_NAME, durable=True)
    dead_exchange = await channel.declare_exchange("webhooks.dle", durable=True)
    deliver_queue = await channel.declare_queue(
        Config.QUEUE_NAME, durable=True, arguments={"x-queue-type": "quorum"}
    )
    dead_queue = await channel.declare_queue(
        "q.dlq", durable=True, arguments={"x-queue-type": "quorum"}
    )

    await deliver_queue.bind(exchange, Config.DELIVER_ROUTING_KEY)
    await dead_queue.bind(dead_exchange, "dlq")

    timeouts = [1, 5, 25, 125]
    for t in timeouts:
        queue = await channel.declare_queue(
            f"q.retry.{t}s",
            durable=True,
            arguments={
                "x-queue-type": "quorum",
                "x-message-ttl": 1000 * t,
                "x-dead-letter-exchange": Config.EXCHANGE_NAME,
                "x-dead-letter-routing-key": Config.DELIVER_ROUTING_KEY,
            },
        )
        await queue.bind(exchange, f"retry.{t}s")


asyncio.run(declare_queues())
