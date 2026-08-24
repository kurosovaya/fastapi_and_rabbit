import time
import uuid


def new_id(prefix: str):

    return f"{prefix}_{time.time_ns():019d}_{uuid.uuid4().hex[:8]}"
