import asyncio
import datetime as dt
from datetime import datetime, timedelta

import aio_pika
import httpx
from aio_pika import DeliveryMode, Message
from aio_pika.abc import AbstractIncomingMessage
from metrics import (
    DELIVERY_ATTEMPTS,
    DELIVERY_DURATION,
    E2E_LATENCY,
    REDELIVERIES,
    WORKER_INFLIGHT,
)
from prometheus_client import start_http_server
from shared.config import Config
from shared.models import DlvStatus, RabbitCustomFields
from shared.storage.factory import make_storage

timeouts = [1, 5, 25, 125]

# import debugpy
# debugpy.listen(("0.0.0.0", 5678))
# print("Waiting for debugger to attach...")
# debugpy.wait_for_client()

client = httpx.AsyncClient()


def observe_e2e(message: AbstractIncomingMessage, delivered_at: datetime):
    accepted_at = message.headers.get("X-Accepted-At")
    if accepted_at is not None:
        E2E_LATENCY.observe(
            (delivered_at - datetime.fromisoformat(str(accepted_at))).total_seconds()
        )


async def worker():
    print("Worker started")
    start_http_server(9004)

    async with make_storage() as storage:
        rabbit_connect = await aio_pika.connect_robust(Config.RABBIT_URL)
        channel = await rabbit_connect.channel()
        await channel.set_qos(1)
        exchange = await channel.get_exchange(Config.EXCHANGE_NAME)
        queue = await channel.get_queue(Config.QUEUE_NAME)
        dead_exchange = await channel.get_exchange(Config.DLE_NAME)

        async def callback(message: AbstractIncomingMessage):
            with WORKER_INFLIGHT.track_inprogress():
                if message.redelivered:
                    REDELIVERIES.inc()

                dlv_id = str(message.headers.get("X-Dlv-Id"))
                client_id = str(message.headers.get("X-Client-Id"))
                await storage.update_delivery_status(dlv_id, DlvStatus.IN_PROGRESS)
                try:
                    with DELIVERY_DURATION.time():
                        response = await client.post(
                            f"http://sink:9001/hook/{client_id}",
                            content=message.body,
                            headers={"Content-Type": "application/json"},
                        )
                    print(f"Sent: {message.body.decode()}")
                    response.raise_for_status()
                    delivered_at = datetime.now(dt.UTC)
                    await storage.update_delivery_status(
                        dlv_id, DlvStatus.DELIVERED, delivered_at
                    )
                    observe_e2e(message, delivered_at)
                    DELIVERY_ATTEMPTS.labels(result="delivered").inc()
                    await message.ack()
                except httpx.HTTPError as exc:
                    # args = {"x-message-ttl": 1000 * 100}
                    try:
                        attempt = message.headers.get(RabbitCustomFields.ATTEMPT, 0)
                        attempt = int(str(attempt)) + 1
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
                        await storage.update_delivery_status(
                            dlv_id,
                            status=DlvStatus.FAILED,
                            attempt=attempt,
                            last_error=str(exc),
                            next_attempt_at=None,
                        )
                        DELIVERY_ATTEMPTS.labels(result="failed").inc()
                    else:
                        await exchange.publish(
                            message_new, f"retry.{timeouts[attempt - 1]}s"
                        )
                        await storage.update_delivery_status(
                            dlv_id,
                            status=DlvStatus.IN_PROGRESS,
                            attempt=attempt,
                            last_error=str(exc),
                            next_attempt_at=datetime.now(dt.UTC)
                            + timedelta(seconds=timeouts[attempt - 1]),
                        )
                        DELIVERY_ATTEMPTS.labels(result="retry").inc()
                    await message.ack()

        await queue.consume(callback)
        await asyncio.Future()


asyncio.run(worker())
