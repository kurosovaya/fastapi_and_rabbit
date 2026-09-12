import re
import uuid
from collections.abc import Generator

import httpx
import pytest

base_url = "http://localhost:9000"


def event_body(order_id: int = 1) -> dict:
    return {"event_type": "order.created", "payload": {"order_id": order_id}}


def subscription_body(client_name: str) -> dict:
    return {
        "url": "http://sink:9001/hook/" + client_name,
        "event_types": ["order.created"],
        "secret": "whsec_test",
        "active": True,
        "client_name": client_name,
    }


def counter_value(client: httpx.Client, name: str) -> float:
    match = re.search(rf"^{name} (\S+)$", client.get("/metrics").text, re.MULTILINE)
    return float(match.group(1)) if match else 0.0


class TestApi:
    @pytest.fixture(scope="class")
    @classmethod
    def client(cls) -> Generator[httpx.Client]:
        with httpx.Client(base_url=base_url, timeout=10) as client:
            try:
                client.get("/health")
            except httpx.ConnectError:
                pytest.skip(f"api is not running at {base_url}")
            yield client

    @pytest.fixture
    @classmethod
    def idempotency_key(cls) -> str:
        return f"test-{uuid.uuid4()}"

    @pytest.fixture
    @classmethod
    def client_name(cls) -> str:
        return f"test-{uuid.uuid4().hex[:8]}"

    def test_health_reports_alive(self, client: httpx.Client):
        response = client.get("/health")
        assert response.status_code == 200
        assert response.json() == "I'AM ALIVE"

    def test_subscription_is_created_and_returns_its_id(
        self, client: httpx.Client, client_name: str
    ):
        response = client.post("/subscriptions", json=subscription_body(client_name))
        assert response.status_code == 201
        assert response.json().startswith("sub_")

    def test_subscription_response_never_leaks_the_secret(
        self, client: httpx.Client, client_name: str
    ):
        response = client.post("/subscriptions", json=subscription_body(client_name))
        assert "whsec_test" not in response.text

    def test_resubscribing_the_same_client_and_url_reuses_the_id(
        self, client: httpx.Client, client_name: str
    ):
        body = subscription_body(client_name)
        first = client.post("/subscriptions", json=body)
        second = client.post("/subscriptions", json=body)
        assert second.json() == first.json()

    @pytest.mark.parametrize(
        "override",
        [
            {"url": "not-a-url"},
            {"event_types": ["nope.not.real"]},
            {"secret": None},
            {"client_name": None},
        ],
    )
    def test_subscription_rejects_invalid_bodies(
        self, client: httpx.Client, client_name: str, override: dict
    ):
        body = subscription_body(client_name) | override
        assert client.post("/subscriptions", json=body).status_code == 422

    def test_event_is_accepted_and_returns_its_id(
        self, client: httpx.Client, idempotency_key: str
    ):
        response = client.post(
            "/events", json=event_body(), headers={"Idempotency-Key": idempotency_key}
        )
        assert response.status_code == 202
        assert response.json().startswith("evt_")

    def test_event_requires_an_idempotency_key(self, client: httpx.Client):
        assert client.post("/events", json=event_body()).status_code == 422

    @pytest.mark.parametrize(
        "body",
        [
            {"event_type": "nope.not.real", "payload": {"order_id": 1}},
            {"event_type": "order.created", "payload": {"order_id": "abc"}},
            {"event_type": "order.created"},
            {"payload": {"order_id": 1}},
        ],
    )
    def test_event_rejects_invalid_bodies(
        self, client: httpx.Client, idempotency_key: str, body: dict
    ):
        response = client.post(
            "/events", json=body, headers={"Idempotency-Key": idempotency_key}
        )
        assert response.status_code == 422

    @pytest.mark.xfail(
        reason="I4: main.py catches pymongo's DuplicateKeyError, but the postgres "
        "backend raises psycopg's UniqueViolation, so the replay 500s instead of "
        "returning the original event",
        strict=False,
    )
    def test_replayed_idempotency_key_returns_the_original_event(
        self, idempotency_key: str
    ):
        headers = {"Idempotency-Key": idempotency_key}
        with httpx.Client(base_url=base_url, timeout=10) as own_client:
            first = own_client.post("/events", json=event_body(), headers=headers)
            second = own_client.post("/events", json=event_body(), headers=headers)
        assert second.status_code == 202
        assert second.json() == first.json()

    def test_created_event_can_be_read_back(
        self, client: httpx.Client, idempotency_key: str
    ):
        created = client.post(
            "/events", json=event_body(42), headers={"Idempotency-Key": idempotency_key}
        )
        response = client.get(f"/events/{created.json()}")
        assert response.status_code == 200

        event = response.json()
        assert event["_id"] == created.json()
        assert event["event_type"] == "order.created"
        assert event["payload"] == {"order_id": 42}
        assert event["idempotency_key"] == idempotency_key

    def test_unknown_event_id_returns_404(self, client: httpx.Client):
        assert client.get(f"/events/evt_{uuid.uuid4().hex}").status_code == 404

    def test_event_url_does_not_accept_post(self, client: httpx.Client):
        assert client.post("/events/evt_whatever").status_code == 405

    def test_accepted_counter_advances_with_each_event(
        self, client: httpx.Client, idempotency_key: str
    ):
        before = counter_value(client, "events_accepted_total")
        client.post(
            "/events", json=event_body(), headers={"Idempotency-Key": idempotency_key}
        )
        assert counter_value(client, "events_accepted_total") >= before + 1
