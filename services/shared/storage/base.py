from abc import abstractmethod
from datetime import datetime
from typing import Protocol, Self

from shared.models import *


class Storage(Protocol):
    @abstractmethod
    async def ensure_client(self, client_name: str) -> str:
        raise NotImplementedError()

    @abstractmethod
    async def create_subscription(self, sub_id, client_id, subscriptions: Subscriptions):
        raise NotImplementedError()

    @abstractmethod
    async def get_subscriptions(self, event_types: str | list):
        raise NotImplementedError()

    @abstractmethod
    async def create_event(self, event_id: str, idempotency_key: str, events: Events):
        raise NotImplementedError()

    @abstractmethod
    async def get_event_id(self, idempotency_key: str) -> str:
        raise NotImplementedError()

    @abstractmethod
    async def get_event(self, event_id: str):
        raise NotImplementedError()

    @abstractmethod
    async def get_unpublished_events(self, start_id: str | None = None):
        raise NotImplementedError()


    @abstractmethod
    async def mark_event_as_published(self, id: str):
        raise NotImplementedError()

    @abstractmethod
    async def create_delivery(self, dlv_id: str, event_id: str, subscription_id: str):
        raise NotImplementedError()

    @abstractmethod
    async def update_delivery_status(
        self,
        id: str,
        status: DlvStatus,
        delivered_at: datetime | None = None,
        attempt: int | None = None,
        last_error: str | None = None,
        next_attempt_at: datetime | None = None,
    ):
        raise NotImplementedError()

    @abstractmethod
    def close(self):
        raise NotImplementedError()

    async def __aenter__(self) -> Self:
        raise NotImplementedError()

    async def __aexit__(self, *exc: object):
        raise NotImplementedError()
