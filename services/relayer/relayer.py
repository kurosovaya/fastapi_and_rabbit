import asyncio
import json
from collections.abc import AsyncGenerator
from contextlib import aclosing

import aio_pika
from aio_pika import DeliveryMode, Message
from shared.config import Config
from shared.ids import new_id
from shared.storage.factory import make_storage


async def relayer():
    print("Relayer started")

    async with make_storage() as storage:
        rabbit_connect = await aio_pika.connect_robust(Config.RABBIT_URL)
        channel = await rabbit_connect.channel()
        await channel.set_qos(1)
        exchange = await channel.get_exchange(Config.EXCHANGE_NAME)

        async def get_unpublished() -> AsyncGenerator[list]:

            last_id = None

            while True:
                batch = await storage.get_unpublished_events(start_id=last_id)

                if not batch:
                    return
                yield batch
                last_id = batch[-1]["_id"]

        while True:
            async with aclosing(get_unpublished()) as batches:
                async for batch in batches:
                    for event in batch:
                        for subscription in await storage.get_subscriptions(
                            event["event_type"]
                        ):
                            dlv_id = new_id("dlv")
                            message = Message(
                                body=json.dumps(
                                    {
                                        "dlv_id": dlv_id,
                                        "event_id": event["_id"],
                                        "event_type": event["event_type"],
                                        "url": subscription["url"],
                                        "payload": event["payload"],
                                    }
                                ).encode(),
                                delivery_mode=DeliveryMode.PERSISTENT,
                                headers={
                                    "X-Event-Id": event["_id"],
                                    "X-Dlv-Id": dlv_id,
                                    "X-Client-Id": str(subscription["client_id"]),
                                    # "X-Client-Name": subscription[""],
                                    "X-Url": subscription["url"],
                                    "X-Accepted-At": event["accepted_at"].isoformat(),
                                },
                                content_type="application/json",
                            )

                            if not await storage.find_delivery(
                                event["_id"], subscription["_id"]
                            ):
                                await exchange.publish(
                                    message, routing_key=Config.DELIVER_ROUTING_KEY
                                )
                                await storage.create_delivery(
                                    dlv_id,
                                    event["_id"],
                                    subscription["_id"],
                                    event["accepted_at"],
                                )
                        await storage.mark_event_as_published(event["_id"])
            await asyncio.sleep(Config.OUTBOX_POLL_INTERVAL)


asyncio.run(relayer())
