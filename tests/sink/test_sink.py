import uuid
from collections.abc import Generator
from time import perf_counter

import httpx
import pytest

base_url = "http://localhost:9001"


def hook_body(event_id: str = "evt_test", order_id: int = 1) -> dict:
    return {
        "event_id": event_id,
        "event_type": "order.created",
        "payload": {"order_id": order_id},
    }


class TestSink:
    @pytest.fixture(scope="class")
    @classmethod
    def client(cls) -> Generator[httpx.Client]:
        with httpx.Client(base_url=base_url, timeout=10) as client:
            try:
                client.get("/health")
            except httpx.ConnectError:
                pytest.skip(f"sink is not running at {base_url}")
            yield client

    @pytest.fixture
    @classmethod
    def client_id(cls) -> str:
        return f"test-{uuid.uuid4().hex[:8]}"

    def test_health_reports_alive(self, client: httpx.Client):
        response = client.get("/health")
        assert response.status_code == 200
        assert response.json() == "SINK IS ALIIIIIIIIIIIIVE"

    def test_root_serves_the_same_healthcheck(self, client: httpx.Client):
        assert client.get("/").json() == client.get("/health").json()

    def test_config_returns_what_it_stored(self, client: httpx.Client, client_id: str):
        response = client.put(
            f"/config/{client_id}", json={"accept_rate": 50, "delay_ms": 0}
        )
        assert response.status_code == 200
        assert response.json() == {"accept_rate": 50, "delay_ms": 0}

    def test_config_fills_in_defaults(self, client: httpx.Client, client_id: str):
        response = client.put(f"/config/{client_id}", json={})
        assert response.status_code == 200
        assert response.json() == {"accept_rate": 100, "delay_ms": 20}

    @pytest.mark.parametrize(
        "body", [{"accept_rate": 101}, {"accept_rate": -1}, {"delay_ms": -1}]
    )
    def test_config_rejects_out_of_range_values(
        self, client: httpx.Client, client_id: str, body: dict
    ):
        assert client.put(f"/config/{client_id}", json=body).status_code == 422

    def test_hook_accepts_and_records_the_payload(
        self, client: httpx.Client, client_id: str
    ):
        client.put(f"/config/{client_id}", json={"accept_rate": 100, "delay_ms": 0})
        response = client.post(f"/hook/{client_id}", json=hook_body(order_id=7))
        assert response.status_code == 200

        received = client.get("/received_hook").json()
        assert received[client_id] == [hook_body(order_id=7)]

    def test_hook_always_succeeds_at_full_accept_rate(
        self, client: httpx.Client, client_id: str
    ):
        client.put(f"/config/{client_id}", json={"accept_rate": 100, "delay_ms": 0})
        for _ in range(5):
            assert (
                client.post(f"/hook/{client_id}", json=hook_body()).status_code == 200
            )

    def test_hook_always_fails_at_zero_accept_rate(
        self, client: httpx.Client, client_id: str
    ):
        client.put(f"/config/{client_id}", json={"accept_rate": 0, "delay_ms": 0})
        for _ in range(5):
            assert (
                client.post(f"/hook/{client_id}", json=hook_body()).status_code == 500
            )

    def test_rejected_hooks_are_not_recorded(
        self, client: httpx.Client, client_id: str
    ):
        client.put(f"/config/{client_id}", json={"accept_rate": 0, "delay_ms": 0})
        client.post(f"/hook/{client_id}", json=hook_body())
        assert client_id not in client.get("/received_hook").json()

    def test_hook_honours_the_configured_delay(
        self, client: httpx.Client, client_id: str
    ):
        client.put(f"/config/{client_id}", json={"accept_rate": 100, "delay_ms": 300})
        started = perf_counter()
        client.post(f"/hook/{client_id}", json=hook_body())
        assert perf_counter() - started >= 0.3

    def test_unconfigured_client_is_fast_and_healthy(self, client: httpx.Client):
        response = client.post(f"/hook/test-{uuid.uuid4().hex[:8]}", json=hook_body())
        assert response.status_code == 200

    @pytest.mark.parametrize(
        "body",
        [
            {"event_type": "order.created", "payload": {"order_id": 1}},
            {"event_id": "e", "event_type": "not.an.event", "payload": {"order_id": 1}},
            {"event_id": "e", "event_type": "order.created", "payload": {"o": "abc"}},
            {},
        ],
    )
    def test_hook_rejects_malformed_bodies(
        self, client: httpx.Client, client_id: str, body: dict
    ):
        assert client.post(f"/hook/{client_id}", json=body).status_code == 422

    def test_hook_404_always_reports_not_found(
        self, client: httpx.Client, client_id: str
    ):
        assert (
            client.post(f"/hook_404/{client_id}", json=hook_body()).status_code == 404
        )
