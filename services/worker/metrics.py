from prometheus_client import Counter, Gauge, Histogram

E2E_LATENCY = Histogram(
    "e2e_latency_seconds",
    "Time from acceptance of the event by the API to successful delivery",
    buckets=(0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60, 120, 300, 600),
)

DELIVERY_DURATION = Histogram(
    "delivery_duration_seconds",
    "Duration of a single outgoing HTTP request to a subscriber",
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10),
)

DELIVERY_ATTEMPTS = Counter(
    "delivery_attempts_total",
    "Delivery attempts by outcome",
    ["result"],
)

WORKER_INFLIGHT = Gauge(
    "worker_inflight",
    "Deliveries this worker is processing right now",
)

REDELIVERIES = Counter(
    "redeliveries_total",
    "Messages the broker redelivered after a consumer went away without acking",
)

for result in ("delivered", "retry", "failed"):
    DELIVERY_ATTEMPTS.labels(result=result)
