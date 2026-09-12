import datetime as dt
from datetime import datetime
from typing import Self

from psycopg import AsyncConnection
from psycopg.rows import DictRow
from psycopg.types.json import Jsonb
from psycopg_pool import AsyncConnectionPool
from shared.models import DlvStatus, Events, Subscriptions
from shared.storage.base import Storage


class PostgresStorage(Storage):
    def __init__(
        self, postgres_client: AsyncConnectionPool[AsyncConnection[DictRow]]
    ) -> None:
        self.postgres_client: AsyncConnectionPool[AsyncConnection[DictRow]] = (
            postgres_client
        )

    async def __aenter__(self) -> Self:
        await self.postgres_client.open()
        await self.postgres_client.wait()
        return self

    async def __aexit__(self, *exc: object):
        await self.close()

    async def ensure_client(self, client_name: str) -> str:
        async with self.postgres_client.connection() as conn:
            row = await (
                await conn.execute(
                    """WITH ins AS (insert into clients (_id, client_name)
                    values (gen_random_uuid(), %(client_name)s)
                    on conflict do nothing
                    returning _id)
                    select _id from ins
                    union all
                    select _id from clients where client_name = %(client_name)s
                    limit 1""",
                    {"client_name": client_name},
                )
            ).fetchone()
        if row is None:
            raise RuntimeError("upsert did not return a document")
        return str(row["_id"])

    async def create_subscription(
        self, sub_id: str, client_id: str, subscriptions: Subscriptions
    ) -> str:

        async with self.postgres_client.connection() as conn:
            row = await (
                await conn.execute(
                    """INSERT INTO subscriptions
            (_id, client_id, url, event_types, secret, active, created_at)
            VALUES(%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (client_id, url) DO UPDATE SET
                active      = EXCLUDED.active,
                secret      = EXCLUDED.secret,
                event_types = EXCLUDED.event_types
            RETURNING _id
            """,
                    (
                        sub_id,
                        client_id,
                        str(subscriptions.url),
                        subscriptions.event_types,
                        subscriptions.secret,
                        subscriptions.active,
                        datetime.now(dt.UTC),
                    ),
                )
            ).fetchone()
        if row is None:
            raise RuntimeError("upsert did not return a document")
        return str(row["_id"])

    async def get_subscriptions(self, event_types: str | list):
        wanted = [event_types] if isinstance(event_types, str) else list(event_types)
        async with self.postgres_client.connection() as conn:
            return await (
                await conn.execute(
                    """SELECT _id, client_id, url, event_types,
            secret, active, created_at
            FROM subscriptions
            WHERE event_types && %s AND active""",
                    (wanted,),
                )
            ).fetchall()

    async def create_event(self, event_id: str, idempotency_key: str, events: Events):
        async with self.postgres_client.connection() as conn:
            await conn.execute(
                """INSERT INTO events
            (_id, idempotency_key, event_type, payload, published, accepted_at)
            VALUES(%s, %s, %s, %s, %s, %s);""",
                (
                    event_id,
                    idempotency_key,
                    events.event_type,
                    Jsonb(events.payload),
                    False,
                    datetime.now(dt.UTC),
                ),
            )

    async def get_event_id(self, idempotency_key: str) -> str:
        async with self.postgres_client.connection() as conn:
            row = await (
                await conn.execute(
                    """select from events
            where idempotency_key = %(idempotency_key)s""",
                    {"idempotency_key": idempotency_key},
                )
            ).fetchone()
        return str((row or {})["_id"])

    async def get_event(self, event_id: str):
        async with self.postgres_client.connection() as conn:
            row = await (
                await conn.execute(
                    """select * from events
            where _id = %(event_id)s""",
                    {"event_id": event_id},
                )
            ).fetchone()
        return row

    async def get_unpublished_events(self, start_id: str | None = None):

        async with self.postgres_client.connection() as conn:
            batch = await (
                await conn.execute(
                    """select * from events
            where published = false AND (%(start_id)s::text IS NULL OR _id > %(start_id)s)
            order by _id
            limit %(limit)s""",
                    {"start_id": start_id, "limit": 500},
                )
            ).fetchall()
        return batch

    async def mark_event_as_published(self, id: str):
        async with self.postgres_client.connection() as conn:
            await conn.execute(
                """update events
            set published = true
            where _id = %s""",
                (id,),
            )

    async def find_delivery(self, event_id: str, subscription_id: str):
        async with self.postgres_client.connection() as conn:
            return await (
                await conn.execute(
                    """select * from deliveries
            where event_id = %s AND subscription_id = %s""",
                    (event_id, subscription_id),
                )
            ).fetchone()

    async def create_delivery(
        self, dlv_id: str, event_id: str, subscription_id: str, accepted_at: datetime
    ):
        async with self.postgres_client.connection() as conn:
            await conn.execute(
                """INSERT INTO deliveries
            (_id, event_id, subscription_id, status, attempt,
            attempt_epoch, locked_by, locked_until, next_attempt_at,
            last_error, accepted_at, delivered_at)
            VALUES(%s, %s, %s, 'pending'::text, 0, 0, %s, %s, %s, %s, %s, %s);""",
                (
                    dlv_id,
                    event_id,
                    subscription_id,
                    None,
                    None,
                    None,
                    None,
                    accepted_at,
                    None,
                ),
            )

    async def update_delivery_status(
        self,
        id: str,
        status: DlvStatus,
        delivered_at: datetime | None = None,
        attempt: int | None = None,
        last_error: str | None = None,
        next_attempt_at: datetime | None = None,
    ):
        async with self.postgres_client.connection() as conn:
            await conn.execute(
                """UPDATE deliveries
            SET status=%(status)s,
            attempt=COALESCE(%(attempt)s, attempt),
            next_attempt_at=COALESCE(%(next_attempt_at)s, next_attempt_at),
            last_error=COALESCE(%(last_error)s, last_error),
            delivered_at=COALESCE(%(delivered_at)s, delivered_at)
            WHERE _id=%(id)s;
            """,
                {
                    "attempt": attempt,
                    "delivered_at": delivered_at,
                    "last_error": last_error,
                    "next_attempt_at": next_attempt_at,
                    "id": id,
                    "status": status,
                },
            )

    async def get_sink_settings(self) -> list[dict]:
        async with self.postgres_client.connection() as conn:
            rows = await (
                await conn.execute("""select client_id, accept_rate, delay_ms
            from sink_settings""")
            ).fetchall()
        return [dict(row) for row in rows]

    async def set_sink_settings(self, client_id: str, accept_rate: int, delay_ms: int):
        async with self.postgres_client.connection() as conn:
            await conn.execute(
                """INSERT INTO sink_settings
            (client_id, accept_rate, delay_ms)
            VALUES(%s, %s, %s)
            ON CONFLICT (client_id) DO UPDATE SET
                accept_rate = EXCLUDED.accept_rate,
                delay_ms    = EXCLUDED.delay_ms""",
                (client_id, accept_rate, delay_ms),
            )

    async def close(self):
        await self.postgres_client.close()
