from enum import StrEnum


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
