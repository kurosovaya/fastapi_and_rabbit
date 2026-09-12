from enum import StrEnum

from pydantic import BaseModel, Field, HttpUrl


class EventType(StrEnum):
    ORDER_CREATED = "order.created"
    PAYMENT_SUCCEEDED = "payment.succeeded"
    USER_REGISTERED = "user.registered"


class DlvStatus(StrEnum):
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    DELIVERED = "delivered"
    FAILED = "failed"


class RabbitCustomFields(StrEnum):
    URL = "X-URL"
    ATTEMPT = "X-Attempt"
    ERR = "X-ERR"


class Subscriptions(BaseModel):
    url: HttpUrl
    event_types: list[EventType]
    secret: str
    active: bool
    client_name: str


class Events(BaseModel):
    event_type: EventType
    payload: dict[str, int]


class EventDB(Events):
    id: str = Field(alias="_id")
    idempotency_key: str
