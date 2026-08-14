from enum import Enum


class EventType(str, Enum):
    ORDER_CREATED = "order.created"
    PAYMENT_SUCCEEDED = "payment.succeeded"
    USER_REGISTERED = "user.registered"

class RabbitCustomFields(str, Enum):
    URL = "X-URL"
    ATTEMPT = "X-Attempt"
    ERR = "X-ERR"
