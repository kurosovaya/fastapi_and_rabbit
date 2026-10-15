from prometheus_client import Counter

EVENTS_ACCEPTED = Counter(
    "events_accepted_total",
    "Events accepted by the API",
)

EVENTS_IDEMPOTENT_HITS = Counter(
    "events_idempotent_hits_total",
    "Requests resolved from an already-seen Idempotency-Key",
)
